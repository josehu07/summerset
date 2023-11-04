//! RS-Paxos -- recovery from WAL.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, LogAction, LogResult};

// RSPaxosReplica recovery from WAL log
impl RSPaxosReplica {
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
                    self.insts.push(self.null_instance()?);
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

            WalEntry::AcceptData {
                slot,
                ballot,
                reqs_cw,
            } => {
                if slot < self.start_slot {
                    return Ok(()); // ignore if slot index outdated
                }
                // locate instance in memory, filling in null instances if needed
                while self.start_slot + self.insts.len() <= slot {
                    self.insts.push(self.null_instance()?);
                }
                // update instance state
                let inst = &mut self.insts[slot - self.start_slot];
                inst.bal = ballot;
                inst.status = Status::Accepting;
                inst.reqs_cw = reqs_cw.clone();
                inst.voted = (ballot, reqs_cw);
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
                        // check number of available shards
                        if inst.reqs_cw.avail_shards() < self.majority {
                            // can't execute if I don't have the complete request batch
                            break;
                        } else if inst.reqs_cw.avail_data_shards()
                            < self.majority
                        {
                            // have enough shards but need reconstruction
                            inst.reqs_cw
                                .reconstruct_data(Some(&self.rs_coder))?;
                        }
                        // execute all commands in this instance on state machine
                        // synchronously
                        for (_, req) in inst.reqs_cw.get_data()?.clone() {
                            if let ApiRequest::Req { cmd, .. } = req {
                                // using 0 as a special command ID
                                self.state_machine.submit_cmd(0, cmd)?;
                                let _ = self.state_machine.get_result().await?;
                            }
                        }
                        // update instance status, commit_bar, and exec_bar
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
    pub async fn recover_from_wal(&mut self) -> Result<(), SummersetError> {
        debug_assert_eq!(self.wal_offset, 0);
        loop {
            // using 0 as a special log action ID
            self.storage_hub.submit_action(
                0,
                LogAction::Read {
                    offset: self.wal_offset,
                },
            )?;
            let (_, log_result) = self.storage_hub.get_result().await?;

            match log_result {
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
                    return logged_err!(self.id; "unexpected log result type");
                }
            }
        }

        // do an extra Truncate to remove paritial entry at the end if any
        self.storage_hub.submit_action(
            0,
            LogAction::Truncate {
                offset: self.wal_offset,
            },
        )?;
        let (_, log_result) = self.storage_hub.get_result().await?;
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = log_result
        {
            if self.wal_offset > 0 {
                pf_info!(self.id; "recovered from wal log: commit {} exec {}",
                                  self.commit_bar, self.exec_bar);
            }
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type or failed truncate")
        }
    }
}
