//! Raft -- leader election.

use std::cmp;
use std::collections::HashSet;

use super::*;

use crate::utils::SummersetError;
use crate::manager::CtrlMsg;
use crate::server::{ReplicaId, LogAction, LogResult};

use rand::prelude::*;

use tokio::time::Duration;

// RaftReplica leader election timeout logic
impl RaftReplica {
    /// Check if the given term is larger than mine. If so, convert my role
    /// back to follower. Returns true if my role was not follower but now
    /// converted to follower, and false otherwise.
    pub(super) async fn check_term(
        &mut self,
        peer: ReplicaId,
        term: Term,
    ) -> Result<bool, SummersetError> {
        if term > self.curr_term {
            self.curr_term = term;
            self.voted_for = None;
            self.votes_granted.clear();

            // refresh heartbeat hearing timer
            self.leader = Some(peer);
            self.heard_heartbeat(peer, term)?;

            // also make the two critical fields durable, synchronously
            let (old_results, result) = self
                .storage_hub
                .do_sync_action(
                    0, // using 0 as dummy log action ID
                    LogAction::Write {
                        entry: DurEntry::Metadata {
                            curr_term: self.curr_term,
                            voted_for: self.voted_for,
                        },
                        offset: 0,
                        sync: self.config.logger_sync,
                    },
                )
                .await?;
            for (old_id, old_result) in old_results {
                self.handle_log_result(old_id, old_result)?;
                self.heard_heartbeat(peer, term)?;
            }
            if let LogResult::Write {
                offset_ok: true, ..
            } = result
            {
            } else {
                return logged_err!(self.id; "unexpected log result type or failed write");
            }

            if self.role != Role::Follower {
                self.role = Role::Follower;
                self.control_hub
                    .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
                pf_info!(self.id; "converted back to follower");
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Becomes a candidate and starts the election procedure.
    pub(super) async fn become_a_candidate(&mut self) -> Result<(), SummersetError> {
        if self.role != Role::Follower {
            return Ok(());
        }

        self.role = Role::Candidate;

        // increment current term and vote for myself
        self.curr_term += 1;
        self.voted_for = Some(self.id);
        self.votes_granted = HashSet::from([self.id]);
        pf_info!(self.id; "starting election with term {}...", self.curr_term);

        // reset election timeout timer
        self.heard_heartbeat(self.id, self.curr_term)?;

        // send RequestVote messages to all other peers
        let last_slot = self.start_slot + self.log.len() - 1;
        debug_assert!(last_slot >= self.start_slot);
        let last_term = self.log[last_slot - self.start_slot].term;
        self.transport_hub.bcast_msg(
            PeerMsg::RequestVote {
                term: self.curr_term,
                last_slot,
                last_term,
            },
            None,
        )?;
        pf_trace!(self.id; "broadcast RequestVote with term {} last {} term {}",
                           self.curr_term, last_slot, last_term);

        // also make the two critical fields durable, synchronously
        let (old_results, result) = self
            .storage_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Write {
                    entry: DurEntry::Metadata {
                        curr_term: self.curr_term,
                        voted_for: self.voted_for,
                    },
                    offset: 0,
                    sync: self.config.logger_sync,
                },
            )
            .await?;
        for (old_id, old_result) in old_results {
            self.handle_log_result(old_id, old_result)?;
            self.heard_heartbeat(self.id, self.curr_term)?;
        }
        if let LogResult::Write {
            offset_ok: true, ..
        } = result
        {
        } else {
            return logged_err!(self.id; "unexpected log result type or failed write");
        }

        Ok(())
    }

    /// Becomes the leader after enough votes granted for me.
    pub(super) fn become_the_leader(&mut self) -> Result<(), SummersetError> {
        pf_info!(self.id; "elected to be leader with term {}", self.curr_term);
        self.role = Role::Leader;
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;

        // clear peers' heartbeat reply counters, and broadcast a heartbeat now
        for cnts in self.hb_reply_cnts.values_mut() {
            *cnts = (1, 0, 0);
        }
        self.bcast_heartbeats()?;

        // re-initialize next_slot and match_slot information
        for slot in self.next_slot.values_mut() {
            *slot = self.start_slot + self.log.len();
        }
        for slot in self.try_next_slot.values_mut() {
            *slot = self.start_slot + self.log.len();
        }
        for slot in self.match_slot.values_mut() {
            *slot = 0;
        }

        // mark some possibly unreplied entries as external
        for slot in self
            .log
            .iter_mut()
            .skip(self.last_commit + 1 - self.start_slot)
        {
            slot.external = true;
        }

        Ok(())
    }

    /// Broadcasts empty AppendEntries messages as heartbeats to all peers.
    pub(super) fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        for peer in 0..self.population {
            if peer == self.id {
                continue;
            }
            let prev_slot = cmp::min(
                self.try_next_slot[&peer] - 1,
                self.start_slot + self.log.len() - 1,
            );
            debug_assert!(prev_slot >= self.start_slot);
            let prev_term = self.log[prev_slot - self.start_slot].term;
            self.transport_hub.bcast_msg(
                PeerMsg::AppendEntries {
                    term: self.curr_term,
                    prev_slot,
                    prev_term,
                    entries: vec![],
                    leader_commit: self.last_commit,
                    last_snap: self.last_snap,
                },
                None,
            )?;
        }

        // update max heartbeat reply counters and their repetitions seen
        for (&peer, cnts) in self.hb_reply_cnts.iter_mut() {
            if cnts.0 > cnts.1 {
                // more hb replies have been received from this peer; it is
                // probably alive
                cnts.1 = cnts.0;
                cnts.2 = 0;
            } else {
                // did not receive hb reply from this peer at least for the
                // last sent hb from me; increment repetition count
                cnts.2 += 1;
                let repeat_threshold = (self.config.hb_hear_timeout_min
                    / self.config.hb_send_interval_ms)
                    as u8;
                if cnts.2 > repeat_threshold {
                    // did not receive hb reply from this peer for too many
                    // past hbs sent from me; this peer is probably dead
                    if self.peer_alive.get(peer)? {
                        self.peer_alive.set(peer, false)?;
                        pf_info!(self.id; "peer_alive updated: {:?}", self.peer_alive);
                    }
                    cnts.2 = 0;
                }
            }
        }

        // I also heard this heartbeat from myself
        self.heard_heartbeat(self.id, self.curr_term)?;

        // pf_trace!(self.id; "broadcast heartbeats term {}", self.curr_term);
        Ok(())
    }

    /// Chooses a random hb_hear_timeout from the min-max range and kicks off
    /// the hb_hear_timer.
    pub(super) fn kickoff_hb_hear_timer(&mut self) -> Result<(), SummersetError> {
        self.hb_hear_timer.cancel()?;

        if !self.config.disable_hb_timer {
            let timeout_ms = thread_rng().gen_range(
                self.config.hb_hear_timeout_min
                    ..=self.config.hb_hear_timeout_max,
            );
            // pf_trace!(self.id; "kickoff hb_hear_timer @ {} ms", timeout_ms);
            self.hb_hear_timer
                .kickoff(Duration::from_millis(timeout_ms))?;
        }

        Ok(())
    }

    /// Heard a heartbeat from some other replica. Resets election timer.
    pub(super) fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        _term: Term,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
            self.hb_reply_cnts.get_mut(&peer).unwrap().0 += 1;
            if !self.peer_alive.get(peer)? {
                self.peer_alive.set(peer, true)?;
                pf_info!(self.id; "peer_alive updated: {:?}", self.peer_alive);
            }
        }

        // reset hearing timer
        self.kickoff_hb_hear_timer()?;

        // pf_trace!(self.id; "heard heartbeat <- {} term {}", peer, term);
        Ok(())
    }
}
