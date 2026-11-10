//! `CRaft` -- leader election.

use std::cmp;
use std::collections::HashSet;

use super::*;
use crate::manager::CtrlMsg;
use crate::server::{LogAction, LogResult, ReplicaId};
use crate::utils::SummersetError;

// CRaftReplica leader election timeout logic
impl CRaftReplica {
    /// Check if the given term is larger than mine. If so, convert my role
    /// back to follower. Returns true if my role was not follower but now
    /// converted to follower, and false otherwise.
    pub(super) async fn check_term(
        &mut self,
        peer: ReplicaId,
        term: Term,
    ) -> Result<bool, SummersetError> {
        if term > self.curr_term
        // || (term == self.curr_term && self.role == Role::Candidate)
        {
            self.curr_term = term;
            self.voted_for = None;
            self.votes_granted.clear();

            // refresh heartbeat hearing timer
            self.leader = Some(peer);
            self.heard_heartbeat(peer, term).await?;

            // also make the two critical fields durable, synchronously
            let (old_results, result) = self
                .storage_hub
                .do_sync_action(LogAction::Write {
                    entry: DurEntry::pack_meta(self.curr_term, self.voted_for),
                    offset: 0,
                    sync: self.config.logger_sync,
                })
                .await?;
            for (old_id, old_result) in old_results {
                self.handle_log_result(old_id, old_result).await?;
                self.heard_heartbeat(peer, term).await?;
            }
            if let LogResult::Write {
                offset_ok: true, ..
            } = result
            {
            } else {
                return logged_err!("unexpected log result type or failed write");
            }

            if self.role == Role::Follower {
                Ok(false)
            } else {
                // only a leader (not a candidate) has a step-down to notify
                if self.role == Role::Leader {
                    self.control_hub
                        .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
                }
                self.role = Role::Follower;
                self.heartbeater.set_sending(false);
                pf_info!("converted back to follower");
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    /// Switch between normal "1 shard per replica" mode and full-copy mode.
    /// If falling back to full-copy as the leader, also upgrades uncommitted
    /// entries to full copies, re-persisting and re-sending them.
    pub(super) async fn switch_assignment_mode(
        &mut self,
        to_full_copy: bool,
    ) -> Result<(), SummersetError> {
        if self.full_copy_mode == to_full_copy {
            return Ok(()); // invalid this mode, ignore
        }
        pf_info!(
            "switching assignment config to: {}",
            if to_full_copy { "full-copy" } else { "1-shard" }
        );
        self.full_copy_mode = to_full_copy;
        if !to_full_copy || self.role != Role::Leader {
            return Ok(());
        }

        // with too many peers down, uncommitted single-shard entries may never
        // gather the acks they need, so upgrade those I have the data of
        let mut first_upgraded = None;
        for slot in (self.commit_known + 1)..(self.start_slot + self.log.len()) {
            let entry = &mut self.log[slot - self.start_slot];
            if !entry.full_copy && entry.reqs_cw.avail_shards() >= self.majority {
                if entry.reqs_cw.avail_data_shards() < self.majority {
                    entry.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }
                entry.full_copy = true;
                first_upgraded.get_or_insert(slot);
            }
        }
        let Some(from) = first_upgraded else {
            return Ok(());
        };

        // re-persist my copies of them, then re-send them to followers, whose
        // earlier acks do not cover the upgrade
        self.rewrite_log_from(from).await?;
        let to = self.start_slot + self.log.len() - 1;
        for peer in 0..self.population {
            let peer_from = cmp::min(self.next_slot[&peer], from);
            if peer == self.id || peer_from <= self.start_slot {
                continue;
            }
            *self.next_slot.get_mut(&peer).unwrap() = peer_from;
            *self.match_slot.get_mut(&peer).unwrap() = cmp::min(self.match_slot[&peer], from - 1);
            self.send_append_entries(peer, peer_from, to)?;
            *self.try_next_slot.get_mut(&peer).unwrap() = to + 1;
        }
        Ok(())
    }

    /// If current leader is not me but times out, becomes a candidate and
    /// starts the election procedure. A candidate whose election times out
    /// (e.g., on a split vote) starts another one.
    pub(super) async fn become_a_candidate(
        &mut self,
        timeout_source: ReplicaId,
    ) -> Result<(), SummersetError> {
        let leader_timeout =
            self.role == Role::Follower && self.leader.is_none_or(|l| l == timeout_source);
        if !(leader_timeout || self.role == Role::Candidate) || self.config.disallow_step_up {
            return Ok(());
        }

        self.role = Role::Candidate;

        // increment current term and vote for myself
        self.curr_term += 1;
        self.voted_for = Some(self.id);
        self.votes_granted = HashSet::from([self.id]);
        pf_info!("starting election with term {}...", self.curr_term);

        // reset election timeout timers, any of which firing before I win means
        // the election timed out
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer(None)?;
        }

        // make the two critical fields durable, synchronously, before asking
        // for votes
        let (old_results, result) = self
            .storage_hub
            .do_sync_action(LogAction::Write {
                entry: DurEntry::pack_meta(self.curr_term, self.voted_for),
                offset: 0,
                sync: self.config.logger_sync,
            })
            .await?;
        for (old_id, old_result) in old_results {
            self.handle_log_result(old_id, old_result).await?;
            self.heard_heartbeat(self.id, self.curr_term).await?;
        }
        if let LogResult::Write {
            offset_ok: true, ..
        } = result
        {
        } else {
            return logged_err!("unexpected log result type or failed write");
        }

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
        pf_trace!(
            "broadcast RequestVote with term {} last {} term {}",
            self.curr_term,
            last_slot,
            last_term
        );

        Ok(())
    }

    /// Becomes the leader after enough votes granted for me.
    pub(super) async fn become_the_leader(&mut self) -> Result<(), SummersetError> {
        pf_info!("elected to be leader with term {}", self.curr_term);
        self.role = Role::Leader;
        self.heartbeater.set_sending(true);
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;

        // re-initialize next_slot and match_slot information (before sending
        // heartbeats, so that they do not carry stale prev_slots)
        for slot in self.next_slot.values_mut() {
            *slot = self.start_slot + self.log.len();
        }
        for slot in self.try_next_slot.values_mut() {
            *slot = self.start_slot + self.log.len();
        }
        for slot in self.match_slot.values_mut() {
            *slot = 0;
        }

        // clear peers' heartbeat reply counters, and broadcast a heartbeat now;
        // as a new leader, I decide on the assignment mode afresh
        self.full_copy_mode = false;
        self.heartbeater.clear_reply_cnts(None)?;
        self.bcast_heartbeats().await?;

        // mark some possibly unreplied entries as external
        for slot in self
            .log
            .iter_mut()
            .skip(self.last_commit + 1 - self.start_slot)
        {
            slot.external = true;
        }

        // append a no-op entry of my term, so that entries of earlier terms
        // get committed along with it even without new client requests
        self.append_new_entry(vec![], false)?;

        Ok(())
    }

    /// Sends empty `AppendEntries` messages as heartbeats to all peers, each
    /// checking the peer's log at its last known matching point.
    pub(super) async fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        self.retrigger_reconstructs()?;
        for peer in 0..self.population {
            if peer == self.id {
                continue;
            }
            let prev_slot = cmp::min(
                self.next_slot[&peer] - 1,
                self.start_slot + self.log.len() - 1,
            );
            debug_assert!(prev_slot >= self.start_slot);
            let prev_term = self.log[prev_slot - self.start_slot].term;
            self.transport_hub.send_msg(
                PeerMsg::AppendEntries {
                    term: self.curr_term,
                    prev_slot,
                    prev_term,
                    entries: vec![],
                    leader_commit: self.commit_known,
                    last_snap: self.last_snap,
                },
                peer,
            )?;
        }

        // update max heartbeat reply counters and their repetitions seen,
        // and peers' liveness status accordingly
        self.heartbeater.update_bcast_cnts()?;

        // I also heard this heartbeat from myself
        self.heard_heartbeat(self.id, self.curr_term).await?;

        // check if we need to fall back to full-copy replication
        if !self.full_copy_mode
            && self.population - self.heartbeater.peer_alive().count()
                >= self.config.fault_tolerance
        {
            self.switch_assignment_mode(true).await?;
        }

        // pf_trace!("broadcast heartbeats term {}", self.curr_term);
        Ok(())
    }

    /// Heard a heartbeat from some other replica. Resets election timer.
    pub(super) async fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        _term: Term,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
            // update the peer's reply cnt and its liveness status accordingly
            self.heartbeater.update_heard_cnt(peer)?;
            // check if we can move back to 1-shard replication (NOT done by
            // vanilla CRaft)
            // if self.population - self.heartbeater.peer_alive().count()
            //     < self.config.fault_tolerance
            // {
            //     self.switch_assignment_mode(false).await?;
            // }
        }

        // reset hearing timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer(Some(peer))?;
        }

        // pf_trace!("heard heartbeat <- {} term {}", peer, term);
        Ok(())
    }
}
