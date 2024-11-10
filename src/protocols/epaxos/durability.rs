//! EPaxos -- durable logging.

use super::*;

use crate::server::{LogActionId, LogResult};
use crate::utils::SummersetError;

// EPaxosReplica durable WAL logging
impl EPaxosReplica {
    /// Handler of PreAcceptSlot logging result chan recv.
    fn handle_logged_pre_accept_slot(
        &mut self,
        slot: SlotIdx,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished PreAcceptSlot logging for slot {} bal {}",
            slot,
            self.insts[row][col - self.start_col].bal
        );
        let inst = &self.insts[row][col - self.start_col];

        if let Some(LeaderBookkeeping { .. }) = inst.leader_bk {
            // on command leader, finishing the logging of a PreAcceptSlot
            // entry is equivalent to receiving a PreAccept reply from myself
            // (as an acceptor role)
            self.handle_msg_pre_accept_reply(
                self.id,
                slot,
                inst.bal,
                inst.seq,
                inst.deps.clone(),
            )?;
        } else if let Some(ReplicaBookkeeping { source }) = inst.replica_bk {
            // on follower replica, finishing the logging of a PreAcceptSlot
            // entry leads to sending back a PreAccept reply
            self.transport_hub.send_msg(
                PeerMsg::PreAcceptReply {
                    slot,
                    ballot: inst.bal,
                    seq: inst.seq,
                    deps: inst.deps.clone(),
                },
                source,
            )?;
            pf_trace!(
                "sent PreAcceptReply -> {} for slot {} bal {} seq {} deps {}",
                source,
                slot,
                inst.bal,
                inst.seq,
                inst.deps,
            );
        }

        Ok(())
    }

    /// Handler of AcceptSlot logging result chan recv.
    fn handle_logged_accept_slot(
        &mut self,
        slot: SlotIdx,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished AcceptSlot logging for slot {} bal {}",
            slot,
            self.insts[row][col - self.start_col].bal
        );
        let inst = &self.insts[row][col - self.start_col];

        if let Some(LeaderBookkeeping { .. }) = inst.leader_bk {
            // on command leader, finishing the logging of an AcceptSlot entry
            // is equivalent to receiving an Accept reply from myself (as an
            // acceptor role)
            self.handle_msg_accept_reply(self.id, slot, inst.bal)?;
        } else if let Some(ReplicaBookkeeping { source }) = inst.replica_bk {
            // on follower replica, finishing the logging of an AcceptSlot
            // entry leads to sending back an Accept reply
            self.transport_hub.send_msg(
                PeerMsg::AcceptReply {
                    slot,
                    ballot: inst.bal,
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

        Ok(())
    }

    /// Handler of CommitSlot logging result chan recv.
    async fn handle_logged_commit_slot(
        &mut self,
        slot: SlotIdx,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished CommitSlot logging for slot {} bal {}",
            slot,
            self.insts[row][col - self.start_col].bal
        );

        // update index of the first non-committed instance in this row
        if col == self.commit_bars[row] {
            let mut advanced = false;
            while self.commit_bars[row] < self.start_col + self.insts[row].len()
            {
                let inst = &mut self.insts[row]
                    [self.commit_bars[row] - self.start_col];
                if inst.status < Status::Committed {
                    break;
                } else if inst.reqs.is_empty() {
                    inst.status = Status::Executed;
                }
                self.commit_bars[row] += 1;
                advanced = true;
            }

            // attempt the execution algorithm on the new tail at commit_bar
            if advanced {
                let tail_slot =
                    SlotIdx(row as ReplicaId, self.commit_bars[row] - 1);
                if self.attempt_execution(tail_slot, false).await? {
                    // re-check each row and attempt execution of committed
                    // instances in case they were previously blocked by me
                    let mut reattempts = vec![];
                    for (row, &col) in self.commit_bars.iter().enumerate() {
                        if col > self.exec_bars[row]
                            && self.insts[row][col - 1 - self.start_col].status
                                == Status::Committed
                        {
                            reattempts.push(SlotIdx(row as ReplicaId, col - 1));
                        }
                    }
                    for slot in reattempts {
                        self.attempt_execution(slot, false).await?;
                    }
                }
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
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(col < self.start_col + self.insts[row].len());

        if let LogResult::Append { now_size } = log_result {
            debug_assert!(now_size >= self.wal_offset);
            // update first wal_offset of slot
            let inst = &mut self.insts[row][col - self.start_col];
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
            Status::PreAccepting => self.handle_logged_pre_accept_slot(slot),
            Status::Accepting => self.handle_logged_accept_slot(slot),
            Status::Committed => self.handle_logged_commit_slot(slot).await,
            _ => {
                logged_err!("unexpected log entry type: {:?}", entry_type)
            }
        }
    }
}
