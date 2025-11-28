//! `RepNothing` -- recovery from WAL.

use super::*;
use crate::server::{ApiRequest, LogAction, LogResult};
use crate::utils::SummersetError;

// RepNothingReplica recovery from WAL log
impl RepNothingReplica {
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
                    // execute all commands on state machine synchronously
                    for (_, req) in entry.reqs.clone() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            self.state_machine
                                .do_sync_cmd(
                                    0, // using 0 as dummy command ID
                                    cmd,
                                )
                                .await?;
                        }
                    }
                    // rebuild in-memory log entry
                    let num_reqs = entry.reqs.len();
                    self.insts.push(Instance {
                        reqs: entry.reqs,
                        durable: true,
                        execed: vec![true; num_reqs],
                    });
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
            Ok(())
        } else {
            logged_err!("unexpected log result type")
        }
    }
}
