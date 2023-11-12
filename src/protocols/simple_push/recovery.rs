//! SimplePush -- recovery from WAL.

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::server::{ApiRequest, LogAction, LogResult};

// SimplePushReplica recovery from WAL log
impl SimplePushReplica {
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
                    let (from_peer, reqs) = match entry {
                        WalEntry::FromClient { reqs } => (None, reqs),
                        WalEntry::PeerPushed {
                            peer,
                            src_inst_idx,
                            reqs,
                        } => (Some((peer, src_inst_idx)), reqs),
                    };
                    // execute all commands on state machine synchronously
                    for (_, req) in reqs.clone() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            // using 0 as a special command ID
                            self.state_machine.submit_cmd(0, cmd)?;
                            let _ = self.state_machine.get_result().await?;
                        }
                    }
                    // rebuild in-memory log entry
                    let num_reqs = reqs.len();
                    self.insts.push(Instance {
                        reqs,
                        durable: true,
                        pending_peers: Bitmap::new(self.population, false),
                        execed: vec![true; num_reqs],
                        from_peer,
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
