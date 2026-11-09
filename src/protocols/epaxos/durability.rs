//! `EPaxos` -- durable logging.

use super::*;
use crate::server::LogResult;
use crate::utils::SummersetError;

// EPaxosReplica durable WAL logging
impl EPaxosReplica {
    /// Handler of `PreAcceptSlot` logging result chan recv, replying with the
    /// `seq` and `deps` logged (which a later re-sent `PreAccept` may update).
    fn handle_logged_pre_accept_slot(
        &mut self,
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished PreAcceptSlot logging for slot {} bal {}",
            slot,
            ballot
        );
        let inst = &self.insts[row][col - self.start_col];
        if inst.bal != ballot {
            return Ok(()); // ignore if instance has moved on to a higher ballot
        }

        if let Some(LeaderBookkeeping { .. }) = inst.leader_bk {
            // on command leader, finishing the logging of a PreAcceptSlot
            // entry is equivalent to receiving a PreAccept reply from myself
            // (as an acceptor role)
            self.handle_msg_pre_accept_reply(self.id, slot, ballot, seq, deps)?;
        } else if let Some(ReplicaBookkeeping { source }) = inst.replica_bk {
            // on follower replica, finishing the logging of a PreAcceptSlot
            // entry leads to sending back a PreAccept reply
            pf_trace!(
                "sent PreAcceptReply -> {} for slot {} bal {} seq {} deps {}",
                source,
                slot,
                ballot,
                seq,
                deps,
            );
            self.transport_hub.send_msg(
                PeerMsg::PreAcceptReply {
                    slot,
                    ballot,
                    seq,
                    deps,
                },
                source,
            )?;
        }

        Ok(())
    }

    /// Handler of `AcceptSlot` logging result chan recv.
    fn handle_logged_accept_slot(
        &mut self,
        slot: SlotIdx,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished AcceptSlot logging for slot {} bal {}",
            slot,
            ballot
        );
        let inst = &self.insts[row][col - self.start_col];
        if inst.bal != ballot {
            return Ok(()); // ignore if instance has moved on to a higher ballot
        }

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

    /// Handler of `CommitSlot` logging result chan recv.
    async fn handle_logged_commit_slot(&mut self, slot: SlotIdx) -> Result<(), SummersetError> {
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
            while self.commit_bars[row] < self.start_col + self.insts[row].len() {
                let col = self.commit_bars[row];
                let inst = &mut self.insts[row][col - self.start_col];
                if inst.status < Status::Committed {
                    break;
                } else if inst.reqs.is_empty() {
                    inst.status = Status::Executed;
                    // nothing to execute, so advance `exec_bars` here if at it
                    self.advance_exec_bar(row, col);
                }
                self.commit_bars[row] += 1;
                advanced = true;
            }

            // attempt the execution algorithm on the new tail at commit_bar
            if advanced {
                let tail_slot = SlotIdx(ReplicaId::try_from(row)?, self.commit_bars[row] - 1);
                if self.attempt_execution(tail_slot, false).await? {
                    // re-check each row and attempt execution of committed
                    // instances in case they were previously blocked by me
                    let mut reattempts = vec![];
                    for (row, &col) in self.commit_bars.iter().enumerate() {
                        if col > self.exec_bars[row]
                            && self.insts[row][col - 1 - self.start_col].status == Status::Committed
                        {
                            reattempts.push(SlotIdx(ReplicaId::try_from(row)?, col - 1));
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
        log_ctx: LogCtx,
        log_result: LogResult<WalEntry>,
    ) -> Result<(), SummersetError> {
        let LogResult::Append { offset, now_size } = log_result else {
            return logged_err!("unexpected log result type: {:?}", log_result);
        };
        debug_assert!(now_size >= self.wal_offset);
        self.wal_offset = now_size;

        let (row, col) = log_ctx.slot().unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(col < self.start_col + self.insts[row].len());
        // entries complete in log order, so the first one of the instance
        // tells where its entries start
        self.insts[row][col - self.start_col]
            .wal_offset
            .get_or_insert(offset);

        match log_ctx {
            LogCtx::PreAccept {
                slot,
                ballot,
                seq,
                deps,
            } => self.handle_logged_pre_accept_slot(slot, ballot, seq, deps),
            LogCtx::Accept { slot, ballot } => self.handle_logged_accept_slot(slot, ballot),
            LogCtx::Commit { slot } => self.handle_logged_commit_slot(slot).await,
        }
    }
}
