//! `Crossword` -- follower gossiping and reconstruction reads of missing
//! shards.

use std::collections::HashMap;
use std::mem;

use rand::prelude::*;
use tokio::time::{Duration, Instant};

use super::*;
use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError};

/// Maximum number of slots to request gossiping for ahead of `commit_bar`.
const GOSSIP_WINDOW_SLOTS: usize = 2048;

/// Maximum total size of the values to request gossiping for ahead of
/// `commit_bar`.
const GOSSIP_WINDOW_BYTES: usize = 64 << 20;

// CrosswordReplica follower gossiping
impl CrosswordReplica {
    /// Chooses a random `gossip_timeout` from the min-max range and kicks off
    /// the `gossip_timer`.
    pub(super) fn kickoff_gossip_timer(&mut self) -> Result<(), SummersetError> {
        self.gossip_timer.cancel()?;

        let timeout_ms = rand::rng()
            .random_range(self.config.gossip_timeout_min..=self.config.gossip_timeout_max);
        // pf_trace!("kickoff gossip_timer @ {} ms", timeout_ms);
        self.gossip_timer
            .kickoff(Duration::from_millis(timeout_ms))?;

        Ok(())
    }

    /// Decide to which peers should I request gossiping and what shards
    /// should I exclude.
    #[allow(clippy::too_many_arguments, clippy::ptr_arg, clippy::ref_option)]
    fn gossip_targets_excl(
        me: ReplicaId,
        population: u8,
        num_data_shards: u8,
        replica_bk: &Option<ReplicaBookkeeping>,
        mut avail_shards_map: Bitmap,
        assignment: &Vec<Bitmap>,
        peer_alive: &Bitmap,
    ) -> HashMap<ReplicaId, Bitmap> {
        let mut src_peer = me;
        if let Some(ReplicaBookkeeping { source, .. }) = replica_bk {
            src_peer = *source;
        }

        // greedily considers my peers, starting from the one with my ID + 1,
        // until enough number of shards covered; the leader who initially
        // replicated this instance to me is only asked as the last resort
        let mut targets_excl = HashMap::new();
        let peers = ((me + 1)..(me + population))
            .map(|p| p % population)
            .filter(|&p| p != src_peer)
            .chain((src_peer != me).then_some(src_peer));
        for peer in peers {
            if !peer_alive.get(peer).unwrap() {
                // skip peers that I don't think are alive right now
                continue;
            }

            // only ask for shards which I don't have right now and I have not
            // asked others for
            let mut useful_shards = Vec::new();
            for (idx, flag) in assignment[peer as usize].iter() {
                if flag && !avail_shards_map.get(idx).unwrap() {
                    useful_shards.push(idx);
                }
            }
            if !useful_shards.is_empty() {
                targets_excl.insert(peer, avail_shards_map.clone());
                for idx in useful_shards {
                    avail_shards_map.set(idx, true).unwrap();
                }
            }

            if avail_shards_map.count() >= num_data_shards {
                break;
            }
        }

        targets_excl
    }

    /// Triggers gossiping for my missing shards in committed but not-yet-
    /// executed instances: fetch missing shards from peers, preferring
    /// follower peers that hold data shards. Requests cover a bounded window
    /// ahead of `commit_bar`, so that slow links do not pile them up; if no
    /// progress is made for a hearing timeout (e.g., replies got lost, or
    /// peers had nothing to send yet), they are sent again.
    pub(super) fn trigger_gossiping(&mut self) -> Result<(), SummersetError> {
        // maintain a map from peer ID to send to -> slots_excl to send
        let mut recon_slots: HashMap<ReplicaId, Vec<(usize, Bitmap)>> =
            HashMap::with_capacity(self.population as usize - 1);
        for peer in 0..self.population {
            if peer != self.id {
                recon_slots.insert(peer, vec![]);
            }
        }

        // skip the tail while new instances keep coming, as slower followers
        // may still be receiving their own shards; repair it once idle
        let end_slot = self.start_slot + self.insts.len();
        let mut until_slot = if end_slot == self.gossip_last_end {
            end_slot
        } else {
            end_slot.saturating_sub(self.config.gossip_tail_ignores)
        };
        self.gossip_last_end = end_slot;

        // bound the window of requests ahead of commit_bar
        let mut window_bytes = 0;
        for slot in self.commit_bar..until_slot {
            if slot - self.commit_bar >= GOSSIP_WINDOW_SLOTS || window_bytes >= GOSSIP_WINDOW_BYTES
            {
                until_slot = slot;
                break;
            }
            window_bytes += self.insts[slot - self.start_slot].reqs_cw.data_len();
        }

        // request again what has made no progress, otherwise only what is new
        let (since, bar) = self.gossip_retry;
        let mut from_slot = cmp::max(self.gossip_bar, self.commit_bar);
        if since.elapsed() >= Duration::from_millis(self.config.hb_hear_timeout_min) {
            if bar == self.commit_bar {
                from_slot = self.commit_bar;
            }
            self.gossip_retry = (Instant::now(), self.commit_bar);
        }
        let mut slot_up_to = from_slot;
        for slot in from_slot..until_slot {
            {
                let inst = &self.insts[slot - self.start_slot];
                if inst.status < Status::Committed {
                    break;
                }
                slot_up_to = slot + 1;
                if inst.status >= Status::Executed {
                    continue;
                }
            }

            let avail_shards_map = self.insts[slot - self.start_slot]
                .reqs_cw
                .avail_shards_map();
            let assignment = &self.insts[slot - self.start_slot].assignment;
            let num_data_shards = self.insts[slot - self.start_slot].reqs_cw.num_data_shards();
            if avail_shards_map.count() < num_data_shards {
                // decide which peers to ask for which shards from
                let targets_excl = Self::gossip_targets_excl(
                    self.id,
                    self.population,
                    num_data_shards,
                    &self.insts[slot - self.start_slot].replica_bk,
                    avail_shards_map,
                    assignment,
                    self.heartbeater.peer_alive(),
                );

                for (peer, exclude) in targets_excl {
                    recon_slots.get_mut(&peer).unwrap().push((slot, exclude));

                    // send reconstruction read messages in chunks
                    let batch_size = if self.config.gossip_batch_size == 0 {
                        self.config.msg_chunk_size
                    } else {
                        self.config.gossip_batch_size
                    };
                    if recon_slots[&peer].len() == batch_size {
                        self.transport_hub.send_msg(
                            PeerMsg::Reconstruct {
                                slots_excl: mem::take(recon_slots.get_mut(&peer).unwrap()),
                            },
                            peer,
                        )?;
                        pf_trace!("sent Reconstruct -> {} for {} slots", peer, batch_size);
                    }
                }
            }
        }

        // send reconstruction read message for remaining slots
        for (peer, slots_excl) in recon_slots.drain() {
            if !slots_excl.is_empty() {
                let num_slots = slots_excl.len();
                self.transport_hub
                    .send_msg(PeerMsg::Reconstruct { slots_excl }, peer)?;
                pf_trace!("sent Reconstruct -> {} for {} slots", peer, num_slots);
            }
        }

        // reset gossip trigger timer
        if !self.config.disable_gossip_timer {
            self.kickoff_gossip_timer()?;
        }

        // update gossip_bar
        if slot_up_to > from_slot {
            pf_debug!(
                "triggered gossiping: slots {} - {}",
                from_slot,
                slot_up_to - 1
            );
        }
        self.gossip_bar = cmp::max(self.gossip_bar, slot_up_to);

        Ok(())
    }

    /// Re-sends reconstruction reads, once per hearing timeout, for committed
    /// instances that still lack enough shards to execute, as earlier reads
    /// (e.g., the ones at step-up) may have got lost.
    pub(super) fn retrigger_reconstructs(&mut self) -> Result<(), SummersetError> {
        if !self.is_leader()
            || self.recon_retry.elapsed() < Duration::from_millis(self.config.hb_hear_timeout_min)
        {
            return Ok(());
        }
        self.recon_retry = Instant::now();
        let recon_slots: Vec<(usize, Bitmap)> = (self.exec_bar..self.start_slot + self.insts.len())
            .filter_map(|slot| {
                let inst = &self.insts[slot - self.start_slot];
                (inst.status == Status::Committed
                    && inst.reqs_cw.avail_shards() < inst.reqs_cw.num_data_shards())
                .then(|| (slot, inst.reqs_cw.avail_shards_map()))
            })
            .collect();
        for chunk in recon_slots.chunks(self.config.msg_chunk_size) {
            self.transport_hub.bcast_msg(
                PeerMsg::Reconstruct {
                    slots_excl: chunk.to_vec(),
                },
                None,
            )?;
            pf_debug!("re-sent Reconstruct messages for {} slots", chunk.len());
        }
        Ok(())
    }

    /// Handler of Reconstruct message from leader or gossiping peer.
    pub(super) fn handle_msg_reconstruct(
        &mut self,
        peer: ReplicaId,
        slots_excl: Vec<(usize, Bitmap)>,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "received Reconstruct <- {} for {} slots",
            peer,
            slots_excl.len()
        );
        let mut slots_data = HashMap::new();

        for (slot, mut subset) in slots_excl {
            if slot < self.start_slot {
                // TODO: this has one caveat: a new leader trying to do
                //       reconstruction reads might find that all other peers
                //       have snapshotted that slot. Proper InstallSnapshot-
                //       style messages will be needed to deal with this; but
                //       since this scenario is just too rare and does not
                //       affect evaluation at all, it will be implemented after
                //       a rework of the storage backend module
                continue;
            }

            // I have nothing to send back for slots beyond my log; padding it
            // with null slots instead would hide them from a Prepare phase of
            // mine that learns of them
            if slot >= self.start_slot + self.insts.len() {
                continue;
            }
            let inst = &self.insts[slot - self.start_slot];

            // ignore spurious duplications; also ignore if I have nothing to
            // send back, or only shards of a value under another code scheme
            if inst.status < Status::Accepting || subset.size() != inst.reqs_cw.num_shards() {
                continue;
            }
            subset.flip(); // exclude unwanted shards the sender already has
            let reply_cw = inst.reqs_cw.subset_copy(&subset, false)?;
            if reply_cw.avail_shards() == 0 {
                continue;
            }

            let Some(value_id) = inst.value_id else {
                continue;
            };
            slots_data.insert(
                slot,
                AcceptedValue {
                    ballot: inst.bal,
                    value_id,
                    reqs_cw: reply_cw,
                },
            );
        }

        if !slots_data.is_empty() {
            let num_slots = slots_data.len();
            self.transport_hub
                .send_msg(PeerMsg::ReconstructReply { slots_data }, peer)?;
            pf_trace!("sent ReconstructReply -> {} for {} slots", peer, num_slots);
        }
        Ok(())
    }

    /// Handler of Reconstruct reply from replica.
    pub(super) fn handle_msg_reconstruct_reply(
        &mut self,
        peer: ReplicaId,
        slots_data: HashMap<usize, AcceptedValue>,
    ) -> Result<(), SummersetError> {
        for (
            slot,
            AcceptedValue {
                ballot,
                value_id,
                reqs_cw,
            },
        ) in slots_data
        {
            if slot < self.start_slot {
                continue; // ignore if slot index outdated
            }
            pf_trace!(
                "in ReconstructReply <- {} for slot {} bal {} shards {:?}",
                peer,
                slot,
                ballot,
                reqs_cw.avail_shards_map()
            );
            debug_assert!(slot < self.start_slot + self.insts.len());
            debug_assert!(self.insts[slot - self.start_slot].status >= Status::Committed);
            let inst = &mut self.insts[slot - self.start_slot];

            // [for perf breakdown only]
            if self.config.record_breakdown && self.config.record_size_recv {
                (*self
                    .bw_accumulators
                    .get_mut(&peer)
                    .expect("peer should exist in experiments")) += reqs_cw.get_size();
            }

            // same-value shards remain useful regardless of accepted ballot
            if inst.status < Status::Executed && inst.value_id == Some(value_id) {
                inst.reqs_cw.absorb_other(reqs_cw)?;
            }
        }

        // if enough shards have been gathered, can push execution forward
        self.advance_commit_bar()
    }
}
