//! `EPaxos` -- snapshotting & GC.

use super::*;
use crate::manager::CtrlMsg;
use crate::server::{LogAction, LogResult};
use crate::utils::SummersetError;

// EPaxosReplica snapshotting & GC logic
impl EPaxosReplica {
    /// Would take a snapshot and discard the log up to it, but is disabled:
    /// columns do not make cuts closed under dependencies (an instance left
    /// of a cut may depend on ones right of it, executed before it), and
    /// dumping key-value pairs by column loses their execution order. Proper
    /// snapshots need cuts along the execution order, e.g., of the state
    /// machine itself.
    pub(super) fn take_new_snapshot(&self) {
        pf_debug!("snapshots disabled, staying at start {}", self.start_col);
    }

    /// Recover initial state from durable storage snapshot file.
    pub(super) async fn recover_from_snapshot(&mut self) -> Result<(), SummersetError> {
        debug_assert_eq!(self.snap_offset, 0);

        // first, try to read the first several bytes, which should record the
        // start_slot index
        match self
            .snapshot_hub
            .do_sync_action(LogAction::Read { offset: 0 })
            .await?
            .1
        {
            LogResult::Read {
                entry: Some(SnapEntry::SlotInfo { start_col }),
                end_offset,
            } => {
                self.snap_offset = end_offset;

                // recover necessary slot indices info
                self.start_col = start_col;
                self.commit_bars = vec![start_col; self.population as usize];
                self.exec_bars = vec![start_col; self.population as usize];
                self.snap_bar = start_col;

                // repeatedly apply key-value pairs
                loop {
                    match self
                        .snapshot_hub
                        .do_sync_action(LogAction::Read {
                            offset: self.snap_offset,
                        })
                        .await?
                        .1
                    {
                        LogResult::Read {
                            entry: Some(SnapEntry::KVPairSet { pairs }),
                            end_offset,
                        } => {
                            // execute Put commands on state machine
                            for (key, value) in pairs {
                                self.state_machine
                                    .do_sync_cmd(
                                        0, // using 0 as dummy command ID
                                        Command::Put { key, value },
                                        None,
                                    )
                                    .await?;
                            }
                            // update snapshot file offset
                            self.snap_offset = end_offset;
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

                // tell manager about my start_col index
                self.control_hub.send_ctrl(CtrlMsg::SnapshotUpTo {
                    new_start: self.start_col,
                })?;

                if self.start_col > 0 {
                    pf_info!(
                        "recovered from snapshot: start {} commit {} exec {}",
                        self.start_col,
                        self.commit_bars[self.id as usize],
                        self.exec_bars[self.id as usize]
                    );
                }
                Ok(())
            }

            LogResult::Read { entry: None, .. } => {
                // snapshot file is empty. Write a 0 as start_col and return
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = self
                    .snapshot_hub
                    .do_sync_action(LogAction::Write {
                        entry: SnapEntry::SlotInfo { start_col: 0 },
                        offset: 0,
                        sync: self.config.logger_sync,
                    })
                    .await?
                    .1
                {
                    self.snap_offset = now_size;
                    Ok(())
                } else {
                    logged_err!("unexpected log result type or failed write")
                }
            }

            _ => {
                logged_err!("unexpected log result type")
            }
        }
    }
}
