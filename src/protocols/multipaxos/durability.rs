//! `MultiPaxos` -- durable logging.

use super::*;
use crate::server::{ApiRequest, LogResult};
use crate::utils::SummersetError;

// MultiPaxosReplica durable WAL logging
impl MultiPaxosReplica {
    /// Handler of `PrepareBal` logging result chan recv.
    fn handle_logged_prepare_bal(
        &mut self,
        slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished PrepareBal logging for slot {} bal {}",
            slot,
            ballot
        );

        let inst = &mut self.insts[slot - self.start_slot];
        debug_assert!(inst.prep_inflight > 0);
        inst.prep_inflight = inst.prep_inflight.saturating_sub(1);
        if (inst.status != Status::Preparing && inst.status < Status::Committed)
            || inst.bal != ballot
            || inst.prep_inflight > 0
        {
            // ignore if instance has moved on, or if a newer PrepareBal (whose
            // ballot is what a reply would claim) is not yet durable
            return Ok(());
        }

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
                && slot <= endprep_slot
            {
                self.handle_msg_prepare_reply(
                    self.id,
                    slot,
                    trigger_slot,
                    endprep_slot,
                    inst.bal,
                    voted,
                    self.accept_bar,
                )?;
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
                        accept_bar: self.accept_bar,
                    },
                    source,
                )?;
                pf_trace!(
                    "sent PrepareReply -> {} for slot {} / {} bal {} accept_bar {}",
                    source,
                    slot,
                    endprep_slot,
                    inst.bal,
                    self.accept_bar,
                );
            }
        }

        Ok(())
    }

    /// Handler of `AcceptData` logging result chan recv.
    fn handle_logged_accept_data(
        &mut self,
        slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished AcceptData logging for slot {} bal {}",
            slot,
            ballot
        );

        // update index of the first non-accepting instance
        if slot == self.accept_bar {
            while self.accept_bar < self.start_slot + self.insts.len() {
                let inst = &mut self.insts[self.accept_bar - self.start_slot];
                if inst.status < Status::Accepting {
                    break;
                }
                self.accept_bar += 1;
            }
        }

        // an older completion must not acknowledge what the instance has
        // accepted since, which is not durable yet
        let inst = &self.insts[slot - self.start_slot];
        if inst.bal != ballot {
            return Ok(());
        }

        if self.is_leader() {
            // on leader, finishing the logging of an AcceptData entry
            // is equivalent to receiving an Accept reply from myself
            // (as an acceptor role)
            self.handle_msg_accept_reply(self.id, slot, inst.bal, None)?;
            // [for perf breakdown only]
            if let Some(sw) = self.bd_stopwatch.as_mut() {
                let _ = sw.record_now(slot, 1, None);
            }
        } else {
            // on follower replica, finishing the logging of an
            // AcceptData entry leads to sending back an Accept reply
            if let Some(ReplicaBookkeeping { source, .. }) = inst.replica_bk {
                self.transport_hub.send_msg(
                    PeerMsg::AcceptReply {
                        slot,
                        ballot: inst.bal,
                        reply_ts: if self.config.record_breakdown {
                            Some(SystemTime::now())
                        } else {
                            None
                        },
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

    /// Handler of `CommitSlot` logging result chan recv.
    fn handle_logged_commit_slot(&mut self, slot: usize) -> Result<(), SummersetError> {
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
            while self.commit_bar < self.accept_bar {
                let inst = &mut self.insts[self.commit_bar - self.start_slot];
                if inst.status < Status::Committed {
                    break;
                }

                // submit commands in committed instance to the state machine
                // for execution
                if inst.reqs.is_empty() {
                    inst.status = Status::Executed;
                    // nothing to execute, so advance `exec_bar` here if at it
                    if self.exec_bar == self.commit_bar {
                        self.exec_bar += 1;
                    }
                } else if inst.status == Status::Committed {
                    for (cmd_idx, (client, req)) in inst.reqs.iter().enumerate() {
                        if let ApiRequest::Req { id: req_id, cmd } = req {
                            self.state_machine.submit_cmd(
                                Self::make_command_id(self.commit_bar, cmd_idx),
                                cmd.clone(),
                                Some((*client, *req_id)),
                            )?;
                        }
                    }
                    pf_trace!(
                        "submitted {} exec commands for slot {}",
                        inst.reqs.len(),
                        self.commit_bar
                    );
                }

                self.commit_bar += 1;

                // if the end of my log has been committed
                if self.commit_bar == self.start_slot + self.insts.len() {
                    // if I'm the leader and urgent CommitNotice is on,
                    // broadcast CommitNotice messages
                    if self.is_leader()
                        && self.bal_prepared > 0
                        && self.bal_prepared == self.bal_max_seen
                        && self.config.urgent_commit_notice
                    {
                        self.transport_hub.bcast_msg(
                            PeerMsg::CommitNotice {
                                ballot: self.bal_max_seen,
                                commit_bar: self.commit_bar,
                            },
                            None,
                        )?;
                        pf_trace!(
                            "broadcast CommitNotice messages at bal {} commit_bar {}",
                            self.bal_max_seen,
                            self.commit_bar
                        );
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

        let slot = log_ctx.slot();
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot < self.start_slot + self.insts.len());
        // entries complete in log order, so the first one of the instance
        // tells where its entries start
        self.insts[slot - self.start_slot]
            .wal_offset
            .get_or_insert(offset);

        match log_ctx {
            LogCtx::PrepareBal { slot, ballot } => self.handle_logged_prepare_bal(slot, ballot),
            LogCtx::AcceptData { slot, ballot } => self.handle_logged_accept_data(slot, ballot),
            LogCtx::CommitSlot { slot } => self.handle_logged_commit_slot(slot),
        }
    }
}
