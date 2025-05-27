//! QuorumLeases -- durable logging.

use super::*;

use crate::server::{ApiRequest, LeaseNotice, LogActionId, LogResult};
use crate::utils::SummersetError;

// QuorumLeasesReplica durable WAL logging
impl QuorumLeasesReplica {
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
                        self.accept_bar,
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

    /// Handler of AcceptData logging result chan recv.
    async fn handle_logged_accept_data(
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

        // revoke read leases I've been granted and am granting to
        debug_assert!(slot > self.qlease_ver as usize);
        let grant_set = self.qlease_manager.grant_set();
        if !self.config.no_lease_retraction {
            if slot == self.qlease_num as usize + 1 // first Accept after qleased
                && self.qlease_grantees().get(self.id)?
            {
                self.qlease_manager
                    .add_notice(self.qlease_num, LeaseNotice::ClearHeld)?;
                self.ensure_qlease_cleared().await?;
            }
            if slot == self.qlease_num as usize + 1
            // first Accept after qleased
            {
                self.qlease_manager.add_notice(
                    self.qlease_num,
                    LeaseNotice::DoRevoke {
                        peers: Some(self.qlease_grantees().clone()),
                    },
                )?;
                // synchronous ensurance not needed here, because in normal case,
                // Accepts should arrive at all grantees normally and they will
                // honor the revocations and reply to the leader by themselves;
                // this revocation is just a trigger for the safety path in cases
                // of leaseholder unresponsiveness
            }
        }

        if self.is_leader() {
            // on leader, finishing the logging of an AcceptData entry
            // is equivalent to receiving an Accept reply from myself
            // (as an acceptor role)
            let inst = &self.insts[slot - self.start_slot];
            self.handle_msg_accept_reply(self.id, slot, inst.bal, grant_set)?;
        } else {
            // on follower replica, finishing the logging of an
            // AcceptData entry leads to sending back an Accept reply
            let inst = &self.insts[slot - self.start_slot];
            if let Some(ReplicaBookkeeping { source, .. }) = inst.replica_bk {
                self.transport_hub.send_msg(
                    PeerMsg::AcceptReply {
                        slot,
                        ballot: inst.bal,
                        grant_set: grant_set.clone(),
                    },
                    source,
                )?;
                pf_trace!(
                    "sent AcceptReply -> {} for slot {} bal {} grants {:?}",
                    source,
                    slot,
                    inst.bal,
                    grant_set,
                );
            }
        }

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

        Ok(())
    }

    /// Handler of CommitSlot logging result chan recv.
    async fn handle_logged_commit_slot(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "finished CommitSlot logging for slot {} bal {} cb {}",
            slot,
            self.insts[slot - self.start_slot].bal,
            self.commit_bar
        );

        // update index of the first non-committed instance
        if slot == self.commit_bar {
            while self.commit_bar < self.accept_bar {
                let inst = &mut self.insts[self.commit_bar - self.start_slot];
                if inst.status < Status::Committed {
                    break;
                }
                let mut conf_changed = false;

                // submit commands in committed instance to the state machine
                // for execution
                if inst.reqs.is_empty() {
                    inst.status = Status::Executed;
                } else if inst.status == Status::Committed {
                    let mut conf_changes = vec![];
                    for (cmd_idx, (client, req)) in inst.reqs.iter().enumerate()
                    {
                        match req {
                            ApiRequest::Req { cmd, .. } => {
                                self.state_machine.submit_cmd(
                                    Self::make_command_id(
                                        self.commit_bar,
                                        cmd_idx,
                                    ),
                                    cmd.clone(),
                                )?;
                            }
                            ApiRequest::Conf { id: req_id, delta } => {
                                conf_changes.push((
                                    *client,
                                    *req_id,
                                    delta.clone(),
                                ));
                            }
                            _ => {} // ignore other types of requests
                        }
                    }
                    pf_trace!(
                        "submitted {} exec commands for slot {}",
                        inst.reqs.len(),
                        self.commit_bar
                    );

                    // if there're read leaseholder roles config changes in the
                    // request batch, maybe apply
                    if !conf_changes.is_empty() {
                        let external = inst.external;
                        self.commit_conf_changes(
                            self.commit_bar,
                            external,
                            conf_changes,
                        )
                        .await?;
                        conf_changed = true;
                    }
                }

                self.commit_bar += 1;

                // if the end of my log has been committed
                if self.commit_bar == self.start_slot + self.insts.len() {
                    // it's time to re-initiate granting read leases
                    debug_assert!(self.commit_bar > 1);
                    if conf_changed || !self.config.no_lease_retraction {
                        self.qlease_num = self.commit_bar as LeaseNum - 1;
                        self.qlease_manager.add_notice(
                            self.qlease_num,
                            LeaseNotice::NewGrants {
                                peers: Some(self.qlease_grantees().clone()),
                                accept_bar: None,
                            },
                        )?;
                    }

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
            Status::Accepting => self.handle_logged_accept_data(slot).await,
            Status::Committed => self.handle_logged_commit_slot(slot).await,
            _ => {
                logged_err!("unexpected log entry type: {:?}", entry_type)
            }
        }
    }
}
