//! CRaft -- recovery from WAL.

use super::*;

use crate::utils::{SummersetError, RSCodeword};
use crate::server::{LogAction, LogResult};

// CRaftReplica recovery from WAL log
impl CRaftReplica {
    /// Recover state from durable storage WAL log.
    pub async fn recover_from_wal(&mut self) -> Result<(), SummersetError> {
        debug_assert_eq!(self.log_offset, 0);

        // first, try to read the first several bytes, which should record
        // necessary durable metadata
        self.storage_hub
            .submit_action(0, LogAction::Read { offset: 0 })?;
        let (_, log_result) = self.storage_hub.get_result().await?;

        match log_result {
            LogResult::Read {
                entry:
                    Some(DurEntry::Metadata {
                        curr_term,
                        voted_for,
                    }),
                end_offset,
            } => {
                self.log_offset = end_offset;
                self.log_meta_end = end_offset;

                // recover necessary metadata info
                self.curr_term = curr_term;
                self.voted_for = voted_for;

                // read out and push all log entries into memory log
                loop {
                    // using 0 as a special log action ID
                    self.storage_hub.submit_action(
                        0,
                        LogAction::Read {
                            offset: self.log_offset,
                        },
                    )?;
                    let (_, log_result) = self.storage_hub.get_result().await?;

                    match log_result {
                        LogResult::Read {
                            entry: Some(DurEntry::LogEntry { mut entry }),
                            end_offset,
                        } => {
                            entry.log_offset = self.log_offset;
                            entry.external = false; // no re-replying to clients
                            self.log.push(entry);
                            self.log_offset = end_offset; // update log offset
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
            }

            LogResult::Read { entry: None, .. } => {
                // log file is empty, write initial metadata
                self.storage_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: DurEntry::Metadata {
                            curr_term: 0,
                            voted_for: None,
                        },
                        offset: 0,
                        sync: self.config.logger_sync,
                    },
                )?;
                let (_, log_result) = self.storage_hub.get_result().await?;
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = log_result
                {
                    self.log_offset = now_size;
                    self.log_meta_end = now_size;
                } else {
                    return logged_err!(self.id; "unexpected log result type or failed write");
                }
                // ... and push a 0-th dummy entry into in-mem log
                let null_entry = LogEntry {
                    term: 0,
                    reqs_cw: RSCodeword::from_null(
                        self.majority,
                        self.population - self.majority,
                    )?,
                    external: false,
                    log_offset: 0,
                };
                self.log.push(null_entry.clone());
                // ... and write the 0-th dummy entry durably
                self.storage_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: DurEntry::LogEntry { entry: null_entry },
                        offset: self.log_offset,
                        sync: self.config.logger_sync,
                    },
                )?;
                let (_, log_result) = self.storage_hub.get_result().await?;
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = log_result
                {
                    self.log[0].log_offset = self.log_offset;
                    self.log_offset = now_size;
                } else {
                    return logged_err!(self.id; "unexpected log result type or failed write");
                }
            }

            _ => return logged_err!(self.id; "unexpected log result type"),
        }

        // do an extra Truncate to remove paritial entry at the end if any
        debug_assert!(self.log_offset >= self.log_meta_end);
        self.storage_hub.submit_action(
            0,
            LogAction::Truncate {
                offset: self.log_offset,
            },
        )?;
        let (_, log_result) = self.storage_hub.get_result().await?;
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = log_result
        {
            if self.log_offset > self.log_meta_end {
                pf_info!(self.id; "recovered from wal log: term {} voted {:?} |log| {}",
                                  self.curr_term, self.voted_for, self.log.len());
            }
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type or failed truncate")
        }
    }
}
