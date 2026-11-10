//! `Raft` -- peer-peer messaging.

use std::cmp;

use super::*;
use crate::server::{LogAction, LogResult, ReplicaId};
use crate::utils::SummersetError;

// RaftReplica peer-peer messages handling
impl RaftReplica {
    /// Handler of `AppendEntries` message from leader.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn handle_msg_append_entries(
        &mut self,
        leader: ReplicaId,
        term: Term,
        prev_slot: usize,
        prev_term: Term,
        mut entries: Vec<LogEntry>,
        leader_commit: usize,
        last_snap: usize,
    ) -> Result<(), SummersetError> {
        if !entries.is_empty() {
            pf_trace!(
                "received AcceptEntries <- {} for slots {} - {} term {}",
                leader,
                prev_slot + 1,
                prev_slot + entries.len(),
                term
            );
        }
        self.check_term(leader, term).await?;
        if term == self.curr_term && self.role == Role::Candidate {
            // a leader got elected in my term, so obey it (keeping my vote)
            self.role = Role::Follower;
            pf_info!("converted back to follower");
        } else if term == self.curr_term && self.role == Role::Leader {
            return logged_err!("another leader {} in my term {}", leader, term);
        }

        // reply false if term smaller than mine, or if my log does not
        // contain an entry at prev_slot matching prev_term; heartbeats are
        // checked as well, as their success commits up to prev_slot
        if term < self.curr_term
            || prev_slot < self.start_slot
            || prev_slot >= self.start_slot + self.log.len()
            || self.log[prev_slot - self.start_slot].term != prev_term
        {
            // figure out the conflict info to send back
            let conflict_term =
                if prev_slot >= self.start_slot && prev_slot < self.start_slot + self.log.len() {
                    self.log[prev_slot - self.start_slot].term
                } else {
                    0
                };
            let mut conflict_slot = prev_slot;
            while conflict_term > 0 && conflict_slot > self.start_slot {
                if self.log[conflict_slot - 1 - self.start_slot].term == conflict_term {
                    conflict_slot -= 1;
                } else {
                    break;
                }
            }

            self.transport_hub.send_msg(
                PeerMsg::AppendEntriesReply {
                    term: self.curr_term,
                    end_slot: prev_slot + entries.len(),
                    conflict: Some((conflict_term, conflict_slot)),
                },
                leader,
            )?;
            pf_trace!(
                "sent AcceptEntriesReply -> {} term {} end_slot {} fail",
                leader,
                self.curr_term,
                prev_slot
            );

            if term == self.curr_term {
                // also refresh heartbeat timer here since the "decrementing"
                // procedure for a lagging follower might take long
                self.leader = Some(leader);
                self.heard_heartbeat(leader, term).await?;
            }
            return Ok(());
        }

        // update my knowledge of who's the current leader, and reset election
        // timeout timer
        self.leader = Some(leader);
        self.heard_heartbeat(leader, term).await?;

        // check if any existing entry conflicts with a new one in `entries`.
        // If so, truncate everything at and after that entry; entries I
        // already have (e.g., when retransmitted) are not appended again
        let num_entries = entries.len();
        let mut first_new = prev_slot + 1 + num_entries;
        for (slot, new_entry) in entries
            .iter()
            .enumerate()
            .map(|(s, e)| (s + prev_slot + 1, e))
        {
            if slot >= self.start_slot + self.log.len() {
                first_new = slot;
                break;
            } else if self.log[slot - self.start_slot].term != new_entry.term {
                // the conflicting entry's append may still be in flight, so wait
                // for it to learn where the entry starts
                for (old_ctx, old_result) in self.storage_hub.barrier().await? {
                    self.handle_log_result(old_ctx, old_result).await?;
                    self.heard_heartbeat(leader, term).await?;
                }
                let Some(cut_offset) = self.log[slot - self.start_slot].log_offset else {
                    return logged_err!("log offset of slot {} unknown", slot);
                };
                // do this truncation in-place for simplicity
                let (_, result) = self
                    .storage_hub
                    .do_sync_action(LogAction::Truncate { offset: cut_offset })
                    .await?;
                if let LogResult::Truncate {
                    offset_ok: true,
                    now_size,
                } = result
                {
                    debug_assert_eq!(now_size, cut_offset);
                    self.log_offset = cut_offset;
                } else {
                    return logged_err!("unexpected log result type or failed truncate");
                }
                // truncate in-mem log as well
                self.log.truncate(slot - self.start_slot);
                first_new = slot;
                break;
            }
        }

        // append new entries into my log, and submit logger actions to make
        // new entries durable
        let mut num_appended = 0;
        for (slot, mut entry) in entries
            .drain((first_new - prev_slot - 1)..entries.len())
            .enumerate()
            .map(|(s, e)| (s + first_new, e))
        {
            entry.log_offset = None;

            self.log.push(entry.clone());
            self.storage_hub.submit_action(
                LogCtx::FollowerAppend {
                    slot,
                    end_slot: prev_slot + num_entries,
                    term: self.curr_term,
                },
                LogAction::Append {
                    entry: DurEntry::LogEntry { entry },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!("submitted follower append log action for slot {}", slot);

            num_appended += 1;
        }

        // if no entries appended, reply right away (also as a reverse
        // heartbeat for peer health tracking), acknowledging only entries
        // already durable; otherwise, the last append's completion replies
        if num_appended == 0 {
            self.transport_hub.send_msg(
                PeerMsg::AppendEntriesReply {
                    term: self.curr_term,
                    end_slot: cmp::min(prev_slot + num_entries, self.last_durable_slot()),
                    conflict: None,
                },
                leader,
            )?;
        }

        // if leader_commit is larger than my last_commit, commit up to it, but
        // not beyond the entries known to match the leader's
        if leader_commit > self.last_commit {
            self.commit_up_to(cmp::min(leader_commit, prev_slot + num_entries))?;
        }

        // if last_snap is larger than mine, update last_snap
        if last_snap > self.last_snap {
            self.last_snap = last_snap;
        }

        Ok(())
    }

    /// Handler of `AppendEntries` reply from follower.
    #[allow(clippy::too_many_lines)]
    async fn handle_msg_append_entries_reply(
        &mut self,
        peer: ReplicaId,
        term: Term,
        end_slot: usize,
        conflict: Option<(Term, usize)>,
    ) -> Result<(), SummersetError> {
        if conflict.is_some() || self.match_slot[&peer] != end_slot {
            pf_trace!(
                "received AcceptEntriesReply <- {} term {} end_slot {} {}",
                peer,
                term,
                end_slot,
                if conflict.is_none() { "ok" } else { "fail" }
            );
        }
        if self.check_term(peer, term).await? || self.role != Role::Leader || term < self.curr_term
        {
            return Ok(()); // ignore if outdated
        }
        self.heard_heartbeat(peer, term).await?;

        // ignore stale replies (e.g., reordered or duplicated ones), which a
        // later success has moved past
        if self.next_slot[&peer] > end_slot + 1 {
            return Ok(());
        }

        if conflict.is_none() {
            // success: update next_slot and match_slot for follower
            *self.next_slot.get_mut(&peer).unwrap() = end_slot + 1;
            if self.try_next_slot[&peer] < end_slot + 1 {
                *self.try_next_slot.get_mut(&peer).unwrap() = end_slot + 1;
            }
            *self.match_slot.get_mut(&peer).unwrap() = end_slot;

            // since we updated some match_slot here, check if any additional
            // entries are now considered committed
            let mut new_commit = self.last_commit;
            for slot in (self.last_commit + 1)..(self.start_slot + self.log.len()) {
                let entry = &self.log[slot - self.start_slot];
                if entry.term != self.curr_term {
                    continue; // cannot decide commit using non-latest term
                }

                #[allow(clippy::cast_possible_truncation)]
                let match_cnt = 1 + self.match_slot.values().filter(|&&s| s >= slot).count() as u8;
                if match_cnt >= self.quorum_cnt {
                    // quorum size reached, set new_commit to here
                    new_commit = slot;
                }
            }

            // submit newly committed commands, if any, for execution
            self.commit_up_to(new_commit)?;

            // also check if any additional entries are safe to snapshot
            for slot in (self.last_snap + 1)..=end_slot {
                #[allow(clippy::cast_possible_truncation)]
                let match_cnt = 1 + self.match_slot.values().filter(|&&s| s >= slot).count() as u8;
                if match_cnt == self.population {
                    // all servers have durably stored this entry
                    self.last_snap = slot;
                }
            }
        } else {
            // failed: decrement next_slot for follower and retry
            debug_assert!(self.next_slot[&peer] >= 1);
            if self.next_slot[&peer] == 1 {
                *self.try_next_slot.get_mut(&peer).unwrap() = 1;
                return Ok(()); // cannot move backward any more
            }

            *self.next_slot.get_mut(&peer).unwrap() -= 1;
            if let Some((conflict_term, conflict_slot)) = conflict {
                while self.next_slot[&peer] > self.start_slot
                    && self.log[self.next_slot[&peer] - self.start_slot].term == conflict_term
                    && self.next_slot[&peer] >= conflict_slot
                    && self.next_slot[&peer] > 1
                {
                    // bypass all conflicting entries in the conflicting term
                    *self.next_slot.get_mut(&peer).unwrap() -= 1;
                }
            }
            *self.try_next_slot.get_mut(&peer).unwrap() = self.next_slot[&peer];
            debug_assert!(end_slot >= self.next_slot[&peer]);

            let prev_slot = self.next_slot[&peer] - 1;
            if prev_slot < self.start_slot {
                return logged_err!("snapshotted slot {} queried", prev_slot);
            }
            if prev_slot >= self.start_slot + self.log.len() {
                return Ok(());
            }
            let to = cmp::min(end_slot, self.start_slot + self.log.len() - 1);
            self.send_append_entries(peer, self.next_slot[&peer], to)?;

            // update try_next_slot to avoid blindly sending the same
            // entries again on future triggers
            *self.try_next_slot.get_mut(&peer).unwrap() = end_slot + 1;
        }

        Ok(())
    }

    /// Handler of `RequestVote` message from candidate.
    async fn handle_msg_request_vote(
        &mut self,
        candidate: ReplicaId,
        term: Term,
        last_slot: usize,
        last_term: Term,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "received RequestVote <- {} with term {} last {} term {}",
            candidate,
            term,
            last_slot,
            last_term
        );
        self.check_term(candidate, term).await?;

        // if the given term is smaller than mine, reply false
        if term < self.curr_term {
            self.transport_hub.send_msg(
                PeerMsg::RequestVoteReply {
                    term: self.curr_term,
                    granted: false,
                },
                candidate,
            )?;
            pf_trace!(
                "sent RequestVoteReply -> {} term {} false",
                candidate,
                self.curr_term
            );
            return Ok(());
        }

        // if I did not vote for anyone else in my current term and that the
        // candidate's log is as up-to-date as mine, grant vote
        #[allow(clippy::collapsible_if)]
        if self.voted_for.is_none() || (self.voted_for.unwrap() == candidate) {
            // the candidate's log is as up-to-date as mine if it ends with a
            // higher term, or with the same term and no shorter
            let my_last_term = self.log.last().unwrap().term;
            if last_term > my_last_term
                || (last_term == my_last_term && last_slot + 1 >= self.start_slot + self.log.len())
            {
                // hear a heartbeat here to prevent me from starting an
                // election soon
                self.heard_heartbeat(candidate, term).await?;

                // update voted_for and make the field durable, synchronously,
                // before granting the vote
                self.voted_for = Some(candidate);
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
                    self.heard_heartbeat(candidate, term).await?;
                }
                if let LogResult::Write {
                    offset_ok: true, ..
                } = result
                {
                } else {
                    return logged_err!("unexpected log result type or failed write");
                }

                self.transport_hub.send_msg(
                    PeerMsg::RequestVoteReply {
                        term: self.curr_term,
                        granted: true,
                    },
                    candidate,
                )?;
                pf_trace!(
                    "sent RequestVoteReply -> {} term {} granted",
                    candidate,
                    self.curr_term
                );
            }
        }

        Ok(())
    }

    /// Handler of `RequestVote` reply from peer.
    async fn handle_msg_request_vote_reply(
        &mut self,
        peer: ReplicaId,
        term: Term,
        granted: bool,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "received RequestVoteReply <- {} with term {} {}",
            peer,
            term,
            if granted { "granted" } else { "false" }
        );
        if self.check_term(peer, term).await?
            || self.role != Role::Candidate
            || term < self.curr_term
            || !granted
        {
            return Ok(()); // only count granted votes of my current election
        }

        // bookkeep this vote
        self.votes_granted.insert(peer);

        // if a majority of servers have voted for me, become the leader
        if u8::try_from(self.votes_granted.len())? >= self.quorum_cnt {
            self.become_the_leader().await?;
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    pub(super) async fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::AppendEntries {
                term,
                prev_slot,
                prev_term,
                entries,
                leader_commit,
                last_snap,
            } => {
                self.handle_msg_append_entries(
                    peer,
                    term,
                    prev_slot,
                    prev_term,
                    entries,
                    leader_commit,
                    last_snap,
                )
                .await
            }
            PeerMsg::AppendEntriesReply {
                term,
                end_slot,
                conflict,
            } => {
                self.handle_msg_append_entries_reply(peer, term, end_slot, conflict)
                    .await
            }
            PeerMsg::RequestVote {
                term,
                last_slot,
                last_term,
            } => {
                self.handle_msg_request_vote(peer, term, last_slot, last_term)
                    .await
            }
            PeerMsg::RequestVoteReply { term, granted } => {
                self.handle_msg_request_vote_reply(peer, term, granted)
                    .await
            }
        }
    }
}
