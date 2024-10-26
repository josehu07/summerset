//! Raft -- recovery from WAL.

use super::*;

use crate::server::{LogAction, LogResult};
use crate::utils::SummersetError;

// RaftReplica recovery from WAL log
impl RaftReplica {
    /// Recover state from durable storage WAL log.
    pub(super) async fn recover_from_wal(
        &mut self,
    ) -> Result<(), SummersetError> {
        debug_assert_eq!(self.log_offset, 0);

        // first, try to read the first several bytes, which should record
        // necessary durable metadata
        match self
            .storage_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Read { offset: 0 },
            )
            .await?
            .1
        {
            LogResult::Read {
                entry: Some(meta_entry),
                end_offset,
            } => {
                self.log_offset = end_offset;
                self.log_meta_end = end_offset;

                // recover necessary metadata info
                (self.curr_term, self.voted_for) = meta_entry.unpack_meta()?;

                // read out and push all log entries into memory log
                loop {
                    match self
                        .storage_hub
                        .do_sync_action(
                            0, // using 0 as dummy log action ID
                            LogAction::Read {
                                offset: self.log_offset,
                            },
                        )
                        .await?
                        .1
                    {
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
                            return logged_err!("unexpected log result type");
                        }
                    }
                }
            }

            LogResult::Read { entry: None, .. } => {
                // log file is empty, write initial metadata
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = self
                    .storage_hub
                    .do_sync_action(
                        0, // using 0 as dummy log action ID
                        LogAction::Write {
                            entry: DurEntry::pack_meta(0, None),
                            offset: 0,
                            sync: self.config.logger_sync,
                        },
                    )
                    .await?
                    .1
                {
                    self.log_offset = now_size;
                    self.log_meta_end = now_size;
                } else {
                    return logged_err!(
                        "unexpected log result type or failed write"
                    );
                }
                // ... and push a 0-th dummy entry into in-mem log
                let null_entry = LogEntry {
                    term: 0,
                    reqs: vec![],
                    external: false,
                    log_offset: 0,
                };
                self.log.push(null_entry.clone());
                // ... and write the 0-th dummy entry durably
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = self
                    .storage_hub
                    .do_sync_action(
                        0, // using 0 as dummy log action ID
                        LogAction::Write {
                            entry: DurEntry::LogEntry { entry: null_entry },
                            offset: self.log_offset,
                            sync: self.config.logger_sync,
                        },
                    )
                    .await?
                    .1
                {
                    self.log[0].log_offset = self.log_offset;
                    self.log_offset = now_size;
                } else {
                    return logged_err!(
                        "unexpected log result type or failed write"
                    );
                }
            }

            _ => return logged_err!("unexpected log result type"),
        }

        // do an extra Truncate to remove partial entry at the end if any
        debug_assert!(self.log_offset >= self.log_meta_end);
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = self
            .storage_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Truncate {
                    offset: self.log_offset,
                },
            )
            .await?
            .1
        {
            if self.log_offset > self.log_meta_end {
                pf_info!(
                    "recovered from wal log: term {} voted {:?} |log| {}",
                    self.curr_term,
                    self.voted_for,
                    self.log.len()
                );
            }
            Ok(())
        } else {
            logged_err!("unexpected log result type or failed truncate")
        }
    }
}
