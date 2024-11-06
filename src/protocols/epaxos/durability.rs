//! EPaxos -- durable logging.

use super::*;

use crate::server::{ApiRequest, LogActionId, LogResult};
use crate::utils::SummersetError;

// EPaxosReplica durable WAL logging
impl EPaxosReplica {
    /// Handler of PrepareBal logging result chan recv.
    fn handle_logged_prepare_bal(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished PrepareBal logging for slot {} bal {}",
            slot,
            self.insts[slot - self.start_slot].bal
        );
        let inst = &self.insts[slot - self.start_slot];
        let voted = if inst.voted.0 > 0 {
            Some(inst.voted.clone())
        } else {
            None
        };

        if self.is_leader() {
            // on leader, finishing the logging of a PrepareBal entry
            // is equivalent to receiving a Prepare reply from myself
            // (as an acceptor role)
            if let Some(LeaderBookkeeping {
                trigger_slot,
                endprep_slot,
                ..
            }) = inst.leader_bk
            {
                if slot <= endprep_slot {
                    self.handle_msg_prepare_reply(
                        self.id,
                        slot,
                        trigger_slot,
                        endprep_slot,
                        inst.bal,
                        voted,
                    )?;
                }
            }
        } else {
            // on follower replica, finishing the logging of a
            // PrepareBal entry leads to sending back a Prepare reply
            if let Some(ReplicaBookkeeping {
                source,
                trigger_slot,
                endprep_slot,
            }) = inst.replica_bk
            {
                self.transport_hub.send_msg(
                    PeerMsg::PrepareReply {
                        slot,
                        trigger_slot,
                        endprep_slot,
                        ballot: inst.bal,
                        voted,
                    },
                    source,
                )?;
                pf_trace!(
                    "sent PrepareReply -> {} for slot {} / {} bal {}",
                    source,
                    slot,
                    endprep_slot,
                    inst.bal
                );
            }
        }

        Ok(())
    }

    /// Handler of AcceptData logging result chan recv.
    fn handle_logged_accept_data(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished AcceptData logging for slot {} bal {}",
            slot,
            self.insts[slot - self.start_slot].bal
        );
        let inst = &self.insts[slot - self.start_slot];

        if self.is_leader() {
            // on leader, finishing the logging of an AcceptData entry
            // is equivalent to receiving an Accept reply from myself
            // (as an acceptor role)
            self.handle_msg_accept_reply(self.id, slot, inst.bal, None)?;
        } else {
            // on follower replica, finishing the logging of an
            // AcceptData entry leads to sending back an Accept reply
            if let Some(ReplicaBookkeeping { source, .. }) = inst.replica_bk {
                self.transport_hub.send_msg(
                    PeerMsg::AcceptReply {
                        slot,
                        ballot: inst.bal,
                        reply_ts: None,
                    },
                    source,
                )?;
                pf_trace!(
                    "sent AcceptReply -> {} for slot {} bal {}",
                    source,
                    slot,
                    inst.bal
                );
            }
        }

        Ok(())
    }

    /// Handler of CommitSlot logging result chan recv.
    fn handle_logged_commit_slot(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished CommitSlot logging for slot {} bal {}",
            slot,
            self.insts[slot - self.start_slot].bal
        );

        // update index of the first non-committed instance
        if slot == self.commit_bar {
            while self.commit_bar < self.start_slot + self.insts.len() {
                let inst = &mut self.insts[self.commit_bar - self.start_slot];
                if inst.status < Status::Committed {
                    break;
                }

                // submit commands in committed instance to the state machine
                // for execution
                if inst.reqs.is_empty() {
                    inst.status = Status::Executed;
                } else if inst.status == Status::Committed {
                    for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            self.state_machine.submit_cmd(
                                Self::make_command_id(self.commit_bar, cmd_idx),
                                cmd.clone(),
                            )?;
                        } else {
                            continue; // ignore other types of requests
                        }
                    }
                    pf_trace!(
                        "submitted {} exec commands for slot {}",
                        inst.reqs.len(),
                        self.commit_bar
                    );
                }

                self.commit_bar += 1;
            }
        }

        Ok(())
    }

    /// Synthesized handler of durable logging result chan recv.
    pub(super) async fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<WalEntry>,
    ) -> Result<(), SummersetError> {
        let (slot, entry_type) = Self::split_log_action_id(action_id);
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot < self.start_slot + self.insts.len());

        if let LogResult::Append { now_size } = log_result {
            debug_assert!(now_size >= self.wal_offset);
            // update first wal_offset of slot
            let inst = &mut self.insts[slot - self.start_slot];
            if inst.wal_offset == 0 || inst.wal_offset > self.wal_offset {
                inst.wal_offset = self.wal_offset;
            }
            debug_assert!(inst.wal_offset <= self.wal_offset);
            // then update self.wal_offset
            self.wal_offset = now_size;
        } else {
            return logged_err!("unexpected log result type: {:?}", log_result);
        }

        match entry_type {
            Status::Preparing => self.handle_logged_prepare_bal(slot),
            Status::Accepting => self.handle_logged_accept_data(slot),
            Status::Committed => self.handle_logged_commit_slot(slot),
            _ => {
                logged_err!("unexpected log entry type: {:?}", entry_type)
            }
        }
    }
}
