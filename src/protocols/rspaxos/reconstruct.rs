//! `RS-Paxos` -- reconstruction reads of missing shards.

use std::cmp;
use std::collections::HashMap;

use super::*;
use crate::server::ReplicaId;
use crate::utils::{Bitmap, RSCodeword, SummersetError};

/// Maximum number of slots to read for reconstruction ahead of `commit_bar`.
const RECON_WINDOW_SLOTS: usize = 2048;

/// Maximum total size of the values to read for reconstruction ahead of
/// `commit_bar`.
const RECON_WINDOW_BYTES: usize = 64 << 20;

// RSPaxosReplica reconstruction reads logic
impl RSPaxosReplica {
    /// Sends reconstruction reads for committed instances that still lack
    /// enough shards to execute, within a bounded window ahead of
    /// `commit_bar` so that slow links do not pile them up. A follower reads
    /// new ones right away, each shard from the other follower holding it
    /// (sparing the leader); all reads are re-sent to everyone if no progress
    /// is made for a hearing timeout, as earlier ones (e.g., the ones at
    /// step-up) may have got lost.
    pub(super) fn retrigger_reconstructs(&mut self) -> Result<(), SummersetError> {
        let (since, bar) = self.recon_retry;
        let mut overdue = false;
        if since.elapsed() >= Duration::from_millis(self.config.hb_hear_timeout_min) {
            overdue = bar == self.commit_bar;
            self.recon_retry = (Instant::now(), self.commit_bar);
        }
        if !overdue && self.is_leader() {
            return Ok(());
        }
        let from_slot = if overdue {
            self.commit_bar
        } else {
            cmp::max(self.recon_bar, self.commit_bar)
        };

        let mut recon_slots: HashMap<ReplicaId, Vec<(usize, Bitmap)>> = HashMap::new();
        let mut slot_up_to = from_slot;
        let mut window_bytes = 0;
        for slot in self.commit_bar..(self.start_slot + self.insts.len()) {
            let inst = &self.insts[slot - self.start_slot];
            if inst.status < Status::Committed
                || slot - self.commit_bar >= RECON_WINDOW_SLOTS
                || window_bytes >= RECON_WINDOW_BYTES
            {
                break;
            }

            window_bytes += inst.reqs_cw.data_len();
            if slot < from_slot {
                continue;
            }
            slot_up_to = slot + 1;
            let avail = inst.reqs_cw.avail_shards_map();
            if inst.status == Status::Executed || avail.count() >= self.majority {
                continue;
            }

            let mut needed = self.majority - avail.count();
            for peer in ((self.id + 1)..(self.id + self.population)).map(|p| p % self.population) {
                if overdue {
                    recon_slots
                        .entry(peer)
                        .or_default()
                        .push((slot, avail.clone()));
                } else if needed > 0 && self.leader != Some(peer) && !avail.get(peer)? {
                    let mut excl = Bitmap::new(self.population, true);
                    excl.set(peer, false)?;
                    recon_slots.entry(peer).or_default().push((slot, excl));
                    needed -= 1;
                }
            }
        }
        if !self.is_leader() {
            self.recon_bar = cmp::max(self.recon_bar, slot_up_to);
        }

        for (peer, slots_excl) in recon_slots {
            for chunk in slots_excl.chunks(self.config.msg_chunk_size) {
                self.transport_hub.send_msg(
                    PeerMsg::Reconstruct {
                        slots_excl: chunk.to_vec(),
                    },
                    peer,
                )?;
            }
            pf_debug!(
                "sent Reconstruct messages -> {} for {} slots",
                peer,
                slots_excl.len()
            );
        }
        Ok(())
    }

    /// Handler of Reconstruct message from leader or a follower.
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
            // send back
            if inst.status < Status::Accepting {
                continue;
            }
            subset.flip(); // exclude unwanted shards the sender already has
            let reply_cw = inst.reqs_cw.subset_copy(&subset, false)?;
            if reply_cw.avail_shards() == 0 {
                continue;
            }

            // send back my ballot for this slot and the available shards
            slots_data.insert(slot, (inst.bal, reply_cw));
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
        slots_data: HashMap<usize, (Ballot, RSCodeword<ReqBatch>)>,
    ) -> Result<(), SummersetError> {
        for (slot, (ballot, reqs_cw)) in slots_data {
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

            // if reply not outdated and ballot is up-to-date, absorb the shards
            // from this replica
            if inst.status < Status::Executed && ballot >= inst.bal {
                inst.reqs_cw.absorb_other(reqs_cw)?;
            }
        }

        // if enough shards have been gathered, can push execution forward
        self.advance_commit_bar()
    }
}
