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
            let (_, log_result) = self
                .storage_hub
                .do_sync_action(
                    0, // using 0 as dummy log action ID
                    LogAction::Read {
                        offset: self.wal_offset,
                    },
                )
                .await?;

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
                            self.state_machine
                                .do_sync_cmd(
                                    0, // using 0 as dummy command ID
                                    cmd,
                                )
                                .await?;
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
        let (_, log_result) = self
            .storage_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Truncate {
                    offset: self.wal_offset,
                },
            )
            .await?;
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
