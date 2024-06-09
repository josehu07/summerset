//! Raft -- peer-peer messaging.

use std::cmp;

use super::*;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, ApiRequest, LogAction, LogResult};

// RaftReplica peer-peer messages handling
impl RaftReplica {
    /// Handler of AppendEntries message from leader.
    #[allow(clippy::too_many_arguments)]
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
            pf_trace!(self.id; "received AcceptEntries <- {} for slots {} - {} term {}",
                               leader, prev_slot + 1, prev_slot + entries.len(), term);
        }
        if self.check_term(leader, term).await? || self.role != Role::Follower {
            if term == self.curr_term && self.role == Role::Candidate {
                // a little hack to promptly obey the elected leader if I
                // started an election with the same term number at roughly
                // the same time
                self.curr_term -= 1;
                self.check_term(leader, term).await?;
            } else {
                return Ok(());
            }
        }

        // reply false if term smaller than mine, or if my log does not
        // contain an entry at prev_slot matching prev_term
        if !entries.is_empty()
            && (term < self.curr_term
                || prev_slot < self.start_slot
                || prev_slot >= self.start_slot + self.log.len()
                || self.log[prev_slot - self.start_slot].term != prev_term)
        {
            // figure out the conflict info to send back
            let conflict_term = if prev_slot >= self.start_slot
                && prev_slot < self.start_slot + self.log.len()
            {
                self.log[prev_slot - self.start_slot].term
            } else {
                0
            };
            let mut conflict_slot = prev_slot;
            while conflict_term > 0 && conflict_slot > self.start_slot {
                if self.log[conflict_slot - 1 - self.start_slot].term
                    == conflict_term
                {
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
            pf_trace!(self.id; "sent AcceptEntriesReply -> {} term {} end_slot {} fail",
                               leader, self.curr_term, prev_slot);

            if term >= self.curr_term {
                // also refresh heartbeat timer here since the "decrementing"
                // procedure for a lagging follower might take long
                self.heard_heartbeat(leader, term)?;
            }
            return Ok(());
        }

        // update my knowledge of who's the current leader, and reset election
        // timeout timer
        self.leader = Some(leader);
        self.heard_heartbeat(leader, term)?;

        // check if any existing entry conflicts with a new one in `entries`.
        // If so, truncate everything at and after that entry
        let mut first_new = prev_slot + 1;
        for (slot, new_entry) in entries
            .iter()
            .enumerate()
            .map(|(s, e)| (s + prev_slot + 1, e))
        {
            if slot >= self.start_slot + self.log.len() {
                first_new = slot;
                break;
            } else if self.log[slot - self.start_slot].term != new_entry.term {
                let cut_offset = self.log[slot - self.start_slot].log_offset;
                // do this truncation in-place for simplicity
                let (old_results, result) = self
                    .storage_hub
                    .do_sync_action(
                        0, // using 0 as dummy log action ID
                        LogAction::Truncate { offset: cut_offset },
                    )
                    .await?;
                for (old_id, old_result) in old_results {
                    self.handle_log_result(old_id, old_result)?;
                    self.heard_heartbeat(leader, term)?;
                }
                if let LogResult::Truncate {
                    offset_ok: true,
                    now_size,
                } = result
                {
                    debug_assert_eq!(now_size, cut_offset);
                    self.log_offset = cut_offset;
                } else {
                    return logged_err!(
                        self.id;
                        "unexpected log result type or failed truncate"
                    );
                }
                // truncate in-mem log as well
                self.log.truncate(slot - self.start_slot);
                first_new = slot;
                break;
            }
        }

        // append new entries into my log, and submit logger actions to make
        // new entries durable
        let (num_entries, mut num_appended) = (entries.len(), 0);
        for (slot, mut entry) in entries
            .drain((first_new - prev_slot - 1)..entries.len())
            .enumerate()
            .map(|(s, e)| (s + first_new, e))
        {
            entry.log_offset = 0;

            self.log.push(entry.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(
                    slot,
                    prev_slot + num_entries,
                    Role::Follower,
                ),
                LogAction::Append {
                    entry: DurEntry::LogEntry { entry },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted follower append log action for slot {}", slot);

            num_appended += 1;
        }

        // even if no entries appended, also send back AppendEntriesReply
        // as a follower-to-leader reverse heardbeat for peer health
        // tracking purposes
        if num_appended == 0 {
            self.transport_hub.send_msg(
                PeerMsg::AppendEntriesReply {
                    term: self.curr_term,
                    end_slot: first_new - 1,
                    conflict: None,
                },
                leader,
            )?;
        }

        // if leader_commit is larger than my last_commit, update last_commit
        if leader_commit > self.last_commit {
            let mut new_commit =
                cmp::min(leader_commit, prev_slot + entries.len());
            new_commit =
                cmp::min(new_commit, self.start_slot + self.log.len() - 1);

            // submit newly committed entries for state machine execution
            for slot in (self.last_commit + 1)..=new_commit {
                let entry = &self.log[slot - self.start_slot];
                for (cmd_idx, (_, req)) in entry.reqs.iter().enumerate() {
                    if let ApiRequest::Req { cmd, .. } = req {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(slot, cmd_idx),
                            cmd.clone(),
                        )?;
                    } else {
                        continue; // ignore other types of requests
                    }
                }
                pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                   entry.reqs.len(), slot);
            }

            self.last_commit = new_commit;
        }

        // if last_snap is larger than mine, update last_snap
        if last_snap > self.last_snap {
            self.last_snap = last_snap;
        }

        Ok(())
    }

    /// Handler of AppendEntries reply from follower.
    async fn handle_msg_append_entries_reply(
        &mut self,
        peer: ReplicaId,
        term: Term,
        end_slot: usize,
        conflict: Option<(Term, usize)>,
    ) -> Result<(), SummersetError> {
        if conflict.is_some() || self.match_slot[&peer] != end_slot {
            pf_trace!(self.id; "received AcceptEntriesReply <- {} term {} end_slot {} {}",
                               peer, term, end_slot,
                               if conflict.is_none() { "ok" } else { "fail" });
        }
        if self.check_term(peer, term).await? || self.role != Role::Leader {
            return Ok(());
        }
        self.heard_heartbeat(peer, term)?;

        if conflict.is_none() {
            // success: update next_slot and match_slot for follower
            debug_assert!(self.next_slot[&peer] <= end_slot + 1);
            *self.next_slot.get_mut(&peer).unwrap() = end_slot + 1;
            if self.try_next_slot[&peer] < end_slot + 1 {
                *self.try_next_slot.get_mut(&peer).unwrap() = end_slot + 1;
            }
            *self.match_slot.get_mut(&peer).unwrap() = end_slot;

            // since we updated some match_slot here, check if any additional
            // entries are now considered committed
            let mut new_commit = self.last_commit;
            for slot in
                (self.last_commit + 1)..(self.start_slot + self.log.len())
            {
                let entry = &self.log[slot - self.start_slot];
                if entry.term != self.curr_term {
                    continue; // cannot decide commit using non-latest term
                }

                let match_cnt = 1 + self
                    .match_slot
                    .values()
                    .filter(|&&s| s >= slot)
                    .count() as u8;
                if match_cnt >= self.quorum_cnt {
                    // quorum size reached, set new_commit to here
                    new_commit = slot;
                }
            }

            // submit newly committed commands, if any, for execution
            for slot in (self.last_commit + 1)..=new_commit {
                let entry = &self.log[slot - self.start_slot];
                for (cmd_idx, (_, req)) in entry.reqs.iter().enumerate() {
                    if let ApiRequest::Req { cmd, .. } = req {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(slot, cmd_idx),
                            cmd.clone(),
                        )?;
                    } else {
                        continue; // ignore other types of requests
                    }
                }
                pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                   entry.reqs.len(), slot);
            }

            self.last_commit = new_commit;

            // also check if any additional entries are safe to snapshot
            for slot in (self.last_snap + 1)..=end_slot {
                let match_cnt = 1 + self
                    .match_slot
                    .values()
                    .filter(|&&s| s >= slot)
                    .count() as u8;
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
                    && self.log[self.next_slot[&peer] - self.start_slot].term
                        == conflict_term
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
                return logged_err!(self.id; "snapshotted slot {} queried", prev_slot);
            }
            if prev_slot >= self.start_slot + self.log.len() {
                return Ok(());
            }
            let mut entries: Vec<LogEntry> = self
                .log
                .iter()
                .take(end_slot + 1 - self.start_slot)
                .skip(self.next_slot[&peer] - self.start_slot)
                .cloned()
                .map(|e| LogEntry {
                    external: false,
                    ..e
                })
                .collect();

            // NOTE: here breaking long AppendEntries into chunks to keep
            // peers heartbeated
            let mut now_prev_slot = prev_slot;
            while !entries.is_empty() {
                let end = cmp::min(entries.len(), self.config.msg_chunk_size);
                let chunk = entries.drain(0..end).collect();

                let now_prev_term =
                    self.log[now_prev_slot - self.start_slot].term;
                self.transport_hub.send_msg(
                    PeerMsg::AppendEntries {
                        term: self.curr_term,
                        prev_slot: now_prev_slot,
                        prev_term: now_prev_term,
                        entries: chunk,
                        leader_commit: self.last_commit,
                        last_snap: self.last_snap,
                    },
                    peer,
                )?;
                pf_trace!(self.id; "sent AppendEntries -> {} with slots {} - {}",
                                   peer, now_prev_slot + 1, now_prev_slot + end);

                now_prev_slot += end;
            }

            // update try_next_slot to avoid blindly sending the same
            // entries again on future triggers
            *self.try_next_slot.get_mut(&peer).unwrap() = end_slot + 1;
        }

        Ok(())
    }

    /// Handler of RequestVote message from candidate.
    async fn handle_msg_request_vote(
        &mut self,
        candidate: ReplicaId,
        term: Term,
        last_slot: usize,
        last_term: Term,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received RequestVote <- {} with term {} last {} term {}",
                           candidate, term, last_slot, last_term);
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
            pf_trace!(self.id; "sent RequestVoteReply -> {} term {} false",
                               candidate, self.curr_term);
            return Ok(());
        }

        // if I did not vote for anyone else in my current term and that the
        // candidate's log is as up-to-date as mine, grant vote
        #[allow(clippy::collapsible_if)]
        if self.voted_for.is_none() || (self.voted_for.unwrap() == candidate) {
            if last_term >= self.log.last().unwrap().term
                || (last_term == self.curr_term
                    && last_slot + 1 >= self.start_slot + self.log.len())
            {
                self.transport_hub.send_msg(
                    PeerMsg::RequestVoteReply {
                        term: self.curr_term,
                        granted: true,
                    },
                    candidate,
                )?;
                pf_trace!(self.id; "sent RequestVoteReply -> {} term {} granted",
                                   candidate, self.curr_term);

                // hear a heartbeat here to prevent me from starting an
                // election soon
                self.heard_heartbeat(candidate, term)?;

                // update voted_for and make the field durable, synchronously
                self.voted_for = Some(candidate);
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
                    self.heard_heartbeat(candidate, term)?;
                }
                if let LogResult::Write {
                    offset_ok: true, ..
                } = result
                {
                } else {
                    return logged_err!(self.id; "unexpected log result type or failed write");
                }
            }
        }

        Ok(())
    }

    /// Handler of RequestVote reply from peer.
    async fn handle_msg_request_vote_reply(
        &mut self,
        peer: ReplicaId,
        term: Term,
        granted: bool,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received RequestVoteReply <- {} with term {} {}",
                           peer, term, if granted { "granted" } else { "false" });
        if self.check_term(peer, term).await? || self.role != Role::Candidate {
            return Ok(());
        }

        // bookkeep this vote
        self.votes_granted.insert(peer);

        // if a majority of servers have voted for me, become the leader
        if self.votes_granted.len() as u8 >= self.quorum_cnt {
            self.become_the_leader()?;
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
                self.handle_msg_append_entries_reply(
                    peer, term, end_slot, conflict,
                )
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
