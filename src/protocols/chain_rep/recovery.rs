//! ChainRep -- recovery from WAL.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, LogAction, LogResult};

// ChainRepReplica recovery from WAL log
impl ChainRepReplica {
    /// Apply a durable storage log entry for recovery.
    async fn recover_apply_entry(
        &mut self,
        entry: WalEntry,
    ) -> Result<(), SummersetError> {
        // locate entry in memory, filling in null entries if needed
        while self.log.len() <= entry.slot {
            self.log.push(Self::null_log_entry());
        }

        // update log entry state
        self.log[entry.slot].status = Status::Propagated;
        self.log[entry.slot].reqs = entry.reqs;

        // submit commands in contiguously filled entries to the state machine
        if entry.slot == self.prop_bar {
            while self.prop_bar < self.log.len() {
                if self.log[self.prop_bar].status < Status::Propagated {
                    break;
                }
                // execute all commands in this entry synchronously
                for (_, req) in self.log[self.prop_bar].reqs.clone() {
                    if let ApiRequest::Req { cmd, .. } = req {
                        self.state_machine
                            .do_sync_cmd(
                                0, // using 0 as dummy command ID
                                cmd,
                            )
                            .await?;
                    }
                }
                // update entry status, prop_bar and exec_bar
                self.log[self.prop_bar].status = Status::Executed;
                self.prop_bar += 1;
                self.exec_bar += 1;
            }
        }

        Ok(())
    }

    /// Recover state from durable storage WAL log.
    pub async fn recover_from_wal(&mut self) -> Result<(), SummersetError> {
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
                    return logged_err!(self.id; "unexpected log result type");
                }
            }
        }

        // do an extra Truncate to remove paritial entry at the end if any
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
                pf_info!(self.id; "recovered from wal log: prop {} exec {}",
                                  self.prop_bar, self.exec_bar);
            }
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type or failed truncate")
        }
    }
}
