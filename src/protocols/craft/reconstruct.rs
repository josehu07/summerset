//! `CRaft` -- reconstruction reads of missing shards.

use std::cmp;
use std::collections::HashMap;

use super::*;
use crate::server::ReplicaId;
use crate::utils::{Bitmap, RSCodeword, SummersetError};

/// Maximum number of entries to read for reconstruction ahead of
/// `last_commit`.
const RECON_WINDOW_SLOTS: usize = 2048;

/// Maximum total size of the entries to read for reconstruction ahead of
/// `last_commit`.
const RECON_WINDOW_BYTES: usize = 64 << 20;

// CRaftReplica reconstruction reads logic
impl CRaftReplica {
    /// Sends reconstruction reads for committed entries that still lack
    /// enough shards to execute, within a bounded window ahead of
    /// `last_commit` so that slow links do not pile them up: right away for
    /// new ones (where a follower reads each shard from the other follower
    /// holding it, sparing the leader), and again to everyone if no progress
    /// is made for a hearing timeout, as earlier reads may have got lost.
    pub(super) fn retrigger_reconstructs(&mut self) -> Result<(), SummersetError> {
        let (since, bar) = self.recon_retry;
        let mut overdue = false;
        if since.elapsed() >= Duration::from_millis(self.config.hb_hear_timeout_min) {
            overdue = bar == self.last_commit;
            self.recon_retry = (Instant::now(), self.last_commit);
        }
        let from_slot = if overdue {
            self.last_commit + 1
        } else {
            cmp::max(self.last_recon, self.last_commit) + 1
        };

        let mut recon_slots: HashMap<ReplicaId, Vec<(usize, Term, Bitmap)>> = HashMap::new();
        let mut to_slot = self.commit_known;
        let mut window_bytes = 0;
        for slot in (self.last_commit + 1)..=self.commit_known {
            if slot - self.last_commit > RECON_WINDOW_SLOTS || window_bytes >= RECON_WINDOW_BYTES {
                to_slot = slot - 1;
                break;
            }
            window_bytes += self.log[slot - self.start_slot].reqs_cw.data_len();
        }
        for slot in from_slot..=to_slot {
            let entry = &self.log[slot - self.start_slot];
            let avail = entry.reqs_cw.avail_shards_map();
            if avail.count() >= self.majority {
                continue;
            }
            let mut needed = self.majority - avail.count();
            for peer in ((self.id + 1)..(self.id + self.population)).map(|p| p % self.population) {
                if overdue || self.role == Role::Leader {
                    recon_slots
                        .entry(peer)
                        .or_default()
                        .push((slot, entry.term, avail.clone()));
                } else if needed > 0 && self.leader != Some(peer) && !avail.get(peer)? {
                    let mut excl = Bitmap::new(self.population, true);
                    excl.set(peer, false)?;
                    recon_slots
                        .entry(peer)
                        .or_default()
                        .push((slot, entry.term, excl));
                    needed -= 1;
                }
            }
        }
        self.last_recon = cmp::max(self.last_recon, to_slot);

        for (peer, slots) in recon_slots {
            for chunk in slots.chunks(self.config.msg_chunk_size) {
                self.transport_hub.send_msg(
                    PeerMsg::Reconstruct {
                        slots: chunk.to_vec(),
                    },
                    peer,
                )?;
            }
            pf_debug!(
                "sent Reconstruct messages -> {} for {} slots",
                peer,
                slots.len()
            );
        }
        Ok(())
    }

    /// Handler of Reconstruct message from leader.
    pub(super) async fn handle_msg_reconstruct(
        &mut self,
        peer: ReplicaId,
        slots: Vec<(usize, Term, Bitmap)>,
    ) -> Result<(), SummersetError> {
        pf_trace!("received Reconstruct <- {} for slots {:?}", peer, slots);
        let mut slots_data = HashMap::new();

        // reconstruction messages also count as heartbeats
        self.heard_heartbeat(peer, self.curr_term).await?;

        for (slot, term, mut subset) in slots {
            if slot < self.start_slot
                || slot >= self.start_slot + self.log.len()
                || term != self.log[slot - self.start_slot].term
            {
                // NOTE: this has one caveat: a new leader trying to do
                //       reconstruction reads might find that all other peers
                //       have snapshotted that slot. Proper InstallSnapshot-
                //       style messages will be needed to deal with this; but
                //       since this scenario is just too rare, it is not
                //       implemented yet
                continue;
            }

            // term match, send back my available shards the requester lacks
            subset.flip();
            let reply_cw = self.log[slot - self.start_slot]
                .reqs_cw
                .subset_copy(&subset, false)?;
            if reply_cw.avail_shards() > 0 {
                slots_data.insert(slot, (term, reply_cw));
            }
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
    pub(super) async fn handle_msg_reconstruct_reply(
        &mut self,
        peer: ReplicaId,
        slots_data: HashMap<usize, (Term, RSCodeword<ReqBatch>)>,
    ) -> Result<(), SummersetError> {
        // reconstruction messages also count as heartbeats
        self.heard_heartbeat(peer, self.curr_term).await?;

        for (slot, (term, reqs_cw)) in slots_data {
            if slot < self.start_slot
                || slot >= self.start_slot + self.log.len()
                || self.log[slot - self.start_slot].term != term
            {
                continue; // ignore if slot index or entry outdated
            }
            pf_trace!(
                "in ReconstructReply <- {} for slot {} shards {:?}",
                peer,
                slot,
                reqs_cw.avail_shards_map()
            );

            // absorb the shards from this replica
            self.log[slot - self.start_slot]
                .reqs_cw
                .absorb_other(reqs_cw)?;
        }

        // if enough shards have been gathered, can push execution forward
        self.commit_up_to(self.commit_known)
    }
}
