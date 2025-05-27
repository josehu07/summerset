//! EPaxos -- snapshotting & GC.

use std::cmp;
use std::collections::HashMap;

use super::*;

use crate::manager::CtrlMsg;
use crate::server::{LogAction, LogResult};
use crate::utils::SummersetError;

// EPaxosReplica snapshotting & GC logic
impl EPaxosReplica {
    /// Dump new key-value pairs to snapshot file.
    // TODO: the current snapshot dumping mechanism is not fully functional as
    //       we need to take care of the case when there's a dependency from
    //       the left side of new_start_col to the right side of it.
    async fn snapshot_dump_kv_pairs(
        &mut self,
        _new_start_col: usize,
    ) -> Result<(), SummersetError> {
        // collect all key-value pairs put up to exec_bar
        let pairs = HashMap::new(); // dummy for now

        // write the collection to snapshot file
        if let LogResult::Append { now_size } = self
            .snapshot_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Append {
                    entry: SnapEntry::KVPairSet { pairs },
                    sync: self.config.logger_sync,
                },
            )
            .await?
            .1
        {
            self.snap_offset = now_size;
            Ok(())
        } else {
            logged_err!("unexpected log result type")
        }
    }

    /// Discard everything older than start_col in durable WAL log.
    async fn snapshot_discard_log(&mut self) -> Result<(), SummersetError> {
        // do a dummy sync read to force all previously submitted log actions
        // to be processed
        let (old_results, _) = self
            .storage_hub
            .do_sync_action(0, LogAction::Read { offset: 0 })
            .await?;
        for (old_id, old_result) in old_results {
            self.handle_log_result(old_id, old_result).await?;
        }

        // get offset to cut the WAL at
        let cut_offset = self
            .insts
            .iter()
            .map(|inst| {
                if inst.is_empty() {
                    self.wal_offset
                } else {
                    inst[0].wal_offset
                }
            })
            .min()
            .unwrap();

        // discard the log before cut_offset
        if cut_offset > 0 {
            if let LogResult::Discard {
                offset_ok: true,
                now_size,
            } = self
                .storage_hub
                .do_sync_action(
                    0, // using 0 as dummy log action ID
                    LogAction::Discard {
                        offset: cut_offset,
                        keep: 0,
                    },
                )
                .await?
                .1
            {
                debug_assert_eq!(self.wal_offset - cut_offset, now_size);
                self.wal_offset = now_size;
            } else {
                return logged_err!(
                    "unexpected log result type or failed discard"
                );
            }
        }

        // update inst.wal_offset for all remaining in-mem instances
        for insts in &mut self.insts {
            for inst in insts {
                if inst.wal_offset > 0 {
                    debug_assert!(inst.wal_offset >= cut_offset);
                    inst.wal_offset -= cut_offset;
                }
            }
        }

        Ok(())
    }

    /// Take a snapshot up to current min(exec_bars), then discard the in-mem
    /// log up to that index as well as outdate entries in the durable WAL file.
    //
    // NOTE: the current implementation does not guard against crashes in the
    //       middle of taking a snapshot. Production quality implementations
    //       should make the snapshotting action "atomic".
    //
    // NOTE: the current implementation does not take care of InstallSnapshot
    //       messages (which is needed when some lagging follower has some slot
    //       which all other peers have snapshotted); we assume here that failed
    //       Accept messages will be retried indefinitely until success before
    //       its associated data gets discarded from peer's memory.
    pub(super) async fn take_new_snapshot(
        &mut self,
    ) -> Result<(), SummersetError> {
        let exec_min = *self.exec_bars.iter().min().unwrap();
        pf_debug!(
            "taking new snapshot: start {} exec {} snap {}",
            self.start_col,
            exec_min,
            self.snap_bar
        );
        debug_assert!(exec_min >= self.start_col);

        let new_start_col = cmp::min(self.snap_bar, exec_min);
        if new_start_col == self.start_col {
            return Ok(());
        }

        // NOTE: broadcast heartbeats here to appease peers
        self.bcast_heartbeats().await?;

        // collect and dump all Puts in executed instances
        self.snapshot_dump_kv_pairs(new_start_col).await?;

        // write new slot info entry to the head of snapshot
        match self
            .snapshot_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Write {
                    entry: SnapEntry::SlotInfo {
                        start_col: new_start_col,
                    },
                    offset: 0,
                    sync: self.config.logger_sync,
                },
            )
            .await?
            .1
        {
            LogResult::Write {
                offset_ok: true, ..
            } => {}
            _ => {
                return logged_err!(
                    "unexpected log result type or failed write"
                );
            }
        }

        // update start_slot and discard all in-memory log instances up to exec_bar
        for insts in &mut self.insts {
            insts.drain(0..(new_start_col - self.start_col));
        }
        self.start_col = new_start_col;

        // NOTE: broadcast heartbeats here to appease peers
        self.bcast_heartbeats().await?;

        // discarding everything older than start_slot in WAL log
        self.snapshot_discard_log().await?;

        // reset the heartbeat hearing timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer(None)?;
        }

        pf_info!("took snapshot up to: start {}", self.start_col);
        Ok(())
    }

    /// Recover initial state from durable storage snapshot file.
    pub(super) async fn recover_from_snapshot(
        &mut self,
    ) -> Result<(), SummersetError> {
        debug_assert_eq!(self.snap_offset, 0);

        // first, try to read the first several bytes, which should record the
        // start_slot index
        match self
            .snapshot_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Read { offset: 0 },
            )
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
                        .do_sync_action(
                            0, // using 0 as dummy log action ID
                            LogAction::Read {
                                offset: self.snap_offset,
                            },
                        )
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
                    .do_sync_action(
                        0, // using 0 as dummy log action ID
                        LogAction::Write {
                            entry: SnapEntry::SlotInfo { start_col: 0 },
                            offset: 0,
                            sync: self.config.logger_sync,
                        },
                    )
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
