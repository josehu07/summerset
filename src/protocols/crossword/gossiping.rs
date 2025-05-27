//! Crossword -- follower gossiping.

use std::collections::HashMap;
use std::mem;

use super::*;

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError};

use rand::prelude::*;

use tokio::time::Duration;

// CrosswordReplica follower gossiping
impl CrosswordReplica {
    /// Chooses a random gossip_timeout from the min-max range and kicks off
    /// the gossip_timer.
    pub(super) fn kickoff_gossip_timer(
        &mut self,
    ) -> Result<(), SummersetError> {
        self.gossip_timer.cancel()?;

        let timeout_ms = thread_rng().gen_range(
            self.config.gossip_timeout_min..=self.config.gossip_timeout_max,
        );
        // pf_trace!("kickoff gossip_timer @ {} ms", timeout_ms);
        self.gossip_timer
            .kickoff(Duration::from_millis(timeout_ms))?;

        Ok(())
    }

    /// Decide to which peers should I request gossiping and what shards
    /// should I exclude.
    #[allow(clippy::too_many_arguments, clippy::ptr_arg)]
    fn gossip_targets_excl(
        me: ReplicaId,
        population: u8,
        rs_data_shards: u8,
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
        // until enough number of shards covered
        let mut targets_excl = HashMap::new();
        for p in (me + 1)..(me + population) {
            let peer = p % population;
            if peer == src_peer {
                // skip leader who initially replicated this instance to me
                continue;
            }
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

            if avail_shards_map.count() >= rs_data_shards {
                break;
            }
        }

        targets_excl
    }

    /// Triggers gossiping for my missing shards in committed but not-yet-
    /// executed instances: fetch missing shards from peers, preferring
    /// follower peers that hold data shards.
    pub(super) fn trigger_gossiping(&mut self) -> Result<(), SummersetError> {
        // maintain a map from peer ID to send to -> slots_excl to send
        let mut recon_slots: HashMap<ReplicaId, Vec<(usize, Bitmap)>> =
            HashMap::with_capacity(self.population as usize - 1);
        for peer in 0..self.population {
            if peer != self.id {
                recon_slots.insert(peer, vec![]);
            }
        }

        let mut slot_up_to = self.gossip_bar;
        let until_slot = if self.start_slot + self.insts.len()
            > self.config.gossip_tail_ignores
        {
            self.start_slot + self.insts.len() - self.config.gossip_tail_ignores
        } else {
            0
        };
        for slot in self.gossip_bar..until_slot {
            slot_up_to = slot;
            {
                let inst = &self.insts[slot - self.start_slot];
                if inst.status >= Status::Executed {
                    continue;
                } else if inst.status < Status::Committed {
                    break;
                }
            }

            let avail_shards_map = self.insts[slot - self.start_slot]
                .reqs_cw
                .avail_shards_map();
            let assignment = &self.insts[slot - self.start_slot].assignment;
            if avail_shards_map.count() < self.rs_data_shards {
                // decide which peers to ask for which shards from
                let targets_excl = Self::gossip_targets_excl(
                    self.id,
                    self.population,
                    self.rs_data_shards,
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
                                slots_excl: mem::take(
                                    recon_slots.get_mut(&peer).unwrap(),
                                ),
                            },
                            peer,
                        )?;
                        pf_trace!(
                            "sent Reconstruct -> {} for {} slots",
                            peer,
                            batch_size
                        );
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
                pf_trace!(
                    "sent Reconstruct -> {} for {} slots",
                    peer,
                    num_slots
                );
            }
        }

        // reset gossip trigger timer
        if !self.config.disable_gossip_timer {
            self.kickoff_gossip_timer()?;
        }

        // update gossip_bar
        if slot_up_to > self.gossip_bar {
            pf_debug!(
                "triggered gossiping: slots {} - {}",
                self.gossip_bar,
                slot_up_to - 1
            );
            self.gossip_bar = slot_up_to;
        }

        Ok(())
    }
}
