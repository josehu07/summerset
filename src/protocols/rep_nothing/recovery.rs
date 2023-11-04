//! RepNothing -- recovery from WAL.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, LogAction, LogResult};

// RepNothingReplica recovery from WAL log
impl RepNothingReplica {
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
                    // execute all commands on state machine synchronously
                    for (_, req) in entry.reqs.clone() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            // using 0 as a special command ID
                            self.state_machine.submit_cmd(0, cmd)?;
                            let _ = self.state_machine.get_result().await?;
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
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type")
        }
    }
}
