//! CRaft -- durable logging.

use std::cmp;

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::server::{LogResult, LogActionId};

// CRaftReplica durable logging
impl CRaftReplica {
    /// Handler of leader append logging result chan recv.
    fn handle_logged_leader_append(
        &mut self,
        slot: usize,
        slot_e: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot || self.role != Role::Leader {
            return Ok(()); // ignore if outdated
        }
        pf_trace!(self.id; "finished leader append logging for slot {} <= {}",
                           slot, slot_e);
        debug_assert_eq!(slot, slot_e);

        // broadcast AppendEntries messages to followers, each containing just
        // the one shard of each entry for that follower
        for peer in 0..self.population {
            if peer == self.id || self.try_next_slot[&peer] < 1 {
                continue;
            }

            let prev_slot = self.try_next_slot[&peer] - 1;
            if prev_slot < self.start_slot {
                return logged_err!(self.id; "snapshotted slot {} queried", prev_slot);
            }
            if prev_slot >= self.start_slot + self.log.len() {
                continue;
            }
            let mut entries: Vec<LogEntry> = self
                .log
                .iter()
                .take(slot + 1 - self.start_slot)
                .skip(self.try_next_slot[&peer] - self.start_slot)
                .map(|e| {
                    if self.full_copy_mode {
                        debug_assert!(
                            e.reqs_cw.avail_data_shards() >= self.majority
                        );
                        LogEntry {
                            term: e.term,
                            reqs_cw: e
                                .reqs_cw
                                .subset_copy(
                                    &Bitmap::from(
                                        self.population,
                                        (0..self.majority).collect(),
                                    ),
                                    false,
                                )
                                .unwrap(),
                            external: false,
                            log_offset: e.log_offset,
                        }
                    } else {
                        LogEntry {
                            term: e.term,
                            reqs_cw: e
                                .reqs_cw
                                .subset_copy(
                                    &Bitmap::from(self.population, vec![peer]),
                                    false,
                                )
                                .unwrap(),
                            external: false,
                            log_offset: e.log_offset,
                        }
                    }
                })
                .collect();

            if slot >= self.try_next_slot[&peer] {
                let mut now_prev_slot = prev_slot;
                while !entries.is_empty() {
                    let end =
                        cmp::min(entries.len(), self.config.msg_chunk_size);
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
                *self.try_next_slot.get_mut(&peer).unwrap() = slot + 1;
            }
        }

        // I also heard my own heartbeat
        self.heard_heartbeat(self.id, self.curr_term)?;

        Ok(())
    }

    /// Handler of follower append logging result chan recv.
    fn handle_logged_follower_append(
        &mut self,
        slot: usize,
        slot_e: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot || self.role != Role::Follower {
            return Ok(()); // ignore if outdated
        }
        pf_trace!(self.id; "finished follower append logging for slot {} <= {}",
                           slot, slot_e);
        debug_assert!(slot <= slot_e);

        // if all consecutive entries are made durable, reply AppendEntries
        // success back to leader
        if slot == slot_e {
            if let Some(leader) = self.leader {
                self.transport_hub.send_msg(
                    PeerMsg::AppendEntriesReply {
                        term: self.curr_term,
                        end_slot: slot_e,
                        conflict: None,
                    },
                    leader,
                )?;
                pf_trace!(self.id; "sent AppendEntriesReply -> {} up to slot {}",
                                   leader, slot_e);
            }
        }

        Ok(())
    }

    /// Synthesized handler of durable logging result chan recv.
    pub(super) fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<DurEntry>,
    ) -> Result<(), SummersetError> {
        let (slot, slot_e, entry_type) = Self::split_log_action_id(action_id);
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot_e < self.start_slot + self.log.len());

        if let LogResult::Append { now_size } = log_result {
            let entry = &mut self.log[slot - self.start_slot];
            if entry.log_offset != self.log_offset {
                // entry has incorrect log_offset bookkept; update it
                entry.log_offset = self.log_offset;
            }
            debug_assert!(now_size > self.log_offset);
            self.log_offset = now_size;
        } else {
            return logged_err!(self.id; "unexpected log result type: {:?}", log_result);
        }

        match entry_type {
            Role::Follower => self.handle_logged_follower_append(slot, slot_e),
            Role::Leader => self.handle_logged_leader_append(slot, slot_e),
            _ => {
                logged_err!(self.id; "unexpected log entry type: {:?}", entry_type)
            }
        }
    }
}
