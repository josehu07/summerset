//! QuorumLeases -- recovery from WAL.

use super::*;

use crate::server::{ApiRequest, LogAction, LogResult};
use crate::utils::SummersetError;

// QuorumLeasesReplica recovery from WAL log
impl QuorumLeasesReplica {
    /// Apply a durable storage log entry for recovery.
    async fn recover_apply_entry(
        &mut self,
        entry: WalEntry,
    ) -> Result<(), SummersetError> {
        match entry {
            WalEntry::PrepareBal { slot, ballot } => {
                if slot < self.start_slot {
                    return Ok(()); // ignore if slot index outdated
                }
                // locate instance in memory, filling in null instances if needed
                while self.start_slot + self.insts.len() <= slot {
                    self.insts.push(self.null_instance());
                }
                // update instance state
                let inst = &mut self.insts[slot - self.start_slot];
                inst.bal = ballot;
                inst.status = Status::Preparing;
                // update bal_prep_sent and bal_max_seen, reset bal_prepared
                if self.bal_prep_sent < ballot {
                    self.bal_prep_sent = ballot;
                }
                if self.bal_max_seen < ballot {
                    self.bal_max_seen = ballot;
                }
                self.bal_prepared = 0;
            }

            WalEntry::AcceptData { slot, ballot, reqs } => {
                if slot < self.start_slot {
                    return Ok(()); // ignore if slot index outdated
                }
                // locate instance in memory, filling in null instances if needed
                while self.start_slot + self.insts.len() <= slot {
                    self.insts.push(self.null_instance());
                }
                // update instance state
                let inst = &mut self.insts[slot - self.start_slot];
                inst.bal = ballot;
                inst.status = Status::Accepting;
                inst.reqs.clone_from(&reqs);
                Self::refresh_highest_slot(slot, &reqs, &mut self.highest_slot);
                inst.voted = (ballot, reqs);
                // it could be the case that the PrepareBal action for this
                // ballot has been snapshotted
                if self.bal_prep_sent < ballot {
                    self.bal_prep_sent = ballot;
                }
                // update bal_prepared and bal_max_seen
                if self.bal_prepared < ballot {
                    self.bal_prepared = ballot;
                }
                if self.bal_max_seen < ballot {
                    self.bal_max_seen = ballot;
                }
                debug_assert!(self.bal_prepared <= self.bal_prep_sent);
            }

            WalEntry::CommitSlot { slot } => {
                if slot < self.start_slot {
                    return Ok(()); // ignore if slot index outdated
                }
                debug_assert!(slot < self.start_slot + self.insts.len());
                // update instance status
                self.insts[slot - self.start_slot].status = Status::Committed;
                // submit commands in contiguously committed instance to the
                // state machine
                if slot == self.commit_bar {
                    while self.commit_bar < self.start_slot + self.insts.len() {
                        let inst =
                            &mut self.insts[self.commit_bar - self.start_slot];
                        if inst.status < Status::Committed {
                            break;
                        }
                        // execute all commands in this instance on state machine
                        // synchronously
                        for (_, req) in inst.reqs.clone() {
                            match req {
                                ApiRequest::Req { cmd, .. } => {
                                    self.state_machine
                                        .do_sync_cmd(
                                            0, // using 0 as dummy command ID
                                            cmd,
                                        )
                                        .await?;
                                }
                                ApiRequest::Conf { conf, .. } => {
                                    self.leasers_cfg = conf;
                                    self.leasers_ver = self.commit_bar;
                                }
                                _ => {} // ignore other request types
                            }
                        }
                        // update instance status, commit_bar and exec_bar
                        self.commit_bar += 1;
                        self.exec_bar += 1;
                        inst.status = Status::Executed;
                    }
                }
            }
        }

        Ok(())
    }

    /// Recover state from durable storage WAL log.
    pub(super) async fn recover_from_wal(
        &mut self,
    ) -> Result<(), SummersetError> {
        debug_assert_eq!(self.wal_offset, 0);
        loop {
            match self
                .storage_hub
                .do_sync_action(
                    0, // using 0 as dummy log action ID
                    LogAction::Read {
                        offset: self.wal_offset,
                    },
                )
                .await?
                .1
            {
                LogResult::Read {
                    entry: Some(entry),
                    end_offset,
                } => {
                    self.recover_apply_entry(entry).await?;
                    // update log offset
                    self.wal_offset = end_offset;
                }
                LogResult::Read { entry: None, .. } => {
                    // end of log reached
                    break;
                }
                _ => {
                    return logged_err!("unexpected log result type");
                }
            }
        }

        // tell manager about read lease roles if changed
        if self.leasers_ver > 0 {
            self.control_hub.send_ctrl(CtrlMsg::LeaserStatus {
                is_grantor: self.leasers_cfg.is_grantor(self.id)?,
                is_grantee: self.leasers_cfg.is_grantee(self.id)?,
            })?;
        }

        // do an extra Truncate to remove partial entry at the end if any
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = self
            .storage_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Truncate {
                    offset: self.wal_offset,
                },
            )
            .await?
            .1
        {
            if self.wal_offset > 0 {
                pf_info!(
                    "recovered from wal log: commit {} exec {}",
                    self.commit_bar,
                    self.exec_bar
                );
            }
            Ok(())
        } else {
            logged_err!("unexpected log result type or failed truncate")
        }
    }
}
