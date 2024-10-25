//! Raft -- snapshotting & GC.

use std::cmp;
use std::collections::HashMap;

use super::*;

use crate::manager::CtrlMsg;
use crate::server::{ApiRequest, Command, LogAction, LogResult};
use crate::utils::SummersetError;

// RaftReplica snapshotting & GC logic
impl RaftReplica {
    /// Dump new key-value pairs to snapshot file.
    async fn snapshot_dump_kv_pairs(
        &mut self,
        new_start_slot: usize,
    ) -> Result<(), SummersetError> {
        // collect all key-value pairs put up to exec_bar
        let mut pairs = HashMap::new();
        for slot in self.start_slot..new_start_slot {
            let entry = &self.log[slot - self.start_slot];
            for (_, req) in entry.reqs.clone() {
                if let ApiRequest::Req {
                    cmd: Command::Put { key, value },
                    ..
                } = req
                {
                    pairs.insert(key, value);
                }
            }
        }

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

    /// Discard everything lower than start_slot in durable log.
    async fn snapshot_discard_log(&mut self) -> Result<(), SummersetError> {
        // do a dummy sync read to force all previously submitted log actions
        // to be processed
        debug_assert!(!self.log.is_empty());
        let (old_results, _) = self
            .storage_hub
            .do_sync_action(0, LogAction::Read { offset: 0 })
            .await?;
        for (old_id, old_result) in old_results {
            self.handle_log_result(old_id, old_result).await?;
        }

        // cut at the first entry's log_offset
        debug_assert_ne!(self.log[0].log_offset, 0);
        let cut_offset = self.log[0].log_offset;

        // discard the log after meta_end and before cut_offset
        if cut_offset > 0 {
            debug_assert!(self.log_meta_end > 0);
            debug_assert!(self.log_meta_end <= cut_offset);
            if let LogResult::Discard {
                offset_ok: true,
                now_size,
            } = self
                .storage_hub
                .do_sync_action(
                    0,
                    LogAction::Discard {
                        offset: cut_offset,
                        keep: self.log_meta_end,
                    },
                )
                .await?
                .1
            {
                debug_assert_eq!(
                    self.log_offset - cut_offset + self.log_meta_end,
                    now_size
                );
                self.log_offset = now_size;
            } else {
                return logged_err!(
                    "unexpected log result type or failed discard"
                );
            }
        }

        // update entry.log_offset for all remaining in-mem entries
        for entry in &mut self.log {
            if entry.log_offset > 0 {
                debug_assert!(entry.log_offset >= cut_offset);
                entry.log_offset -= cut_offset - self.log_meta_end;
            }
        }

        Ok(())
    }

    /// Take a snapshot up to current last_exec, then discard the in-mem log up
    /// to that index as well as their data in the durable log file.
    ///
    /// NOTE: the current implementation does not guard against crashes in the
    /// middle of taking a snapshot. Production quality implementations should
    /// make the snapshotting action "atomic".
    ///
    /// NOTE: the current implementation does not take care of InstallSnapshot
    /// messages (which is needed when some lagging follower has some slot
    /// which all other peers have snapshotted); we take the conservative
    /// approach that a snapshot is only taken when data has been durably
    /// committed on all servers.
    pub(super) async fn take_new_snapshot(
        &mut self,
    ) -> Result<(), SummersetError> {
        pf_debug!(
            "taking new snapshot: start {} exec {} snap {}",
            self.start_slot,
            self.last_exec,
            self.last_snap
        );
        debug_assert!(self.last_exec + 1 >= self.start_slot);

        // always keep at least one entry in log to make indexing happy
        let new_start_slot = cmp::min(self.last_snap, self.last_exec);
        debug_assert!(new_start_slot < self.start_slot + self.log.len());
        if new_start_slot < self.start_slot + 1 {
            return Ok(());
        }

        // collect and dump all Puts in executed entries
        if self.role == Role::Leader {
            // NOTE: broadcast heartbeats here to appease followers
            self.bcast_heartbeats().await?;
        }
        self.snapshot_dump_kv_pairs(new_start_slot).await?;

        // write new slot info entry to the head of snapshot
        match self
            .snapshot_hub
            .do_sync_action(
                0, // using 0 as dummy log action ID
                LogAction::Write {
                    entry: SnapEntry::SlotInfo {
                        start_slot: new_start_slot,
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

        // update start_slot and discard all in-mem log entries up to
        // new_start_slot
        self.log.drain(0..(new_start_slot - self.start_slot));
        self.start_slot = new_start_slot;

        // discarding everything lower than start_slot in durable log
        if self.role == Role::Leader {
            // NOTE: broadcast heartbeats here to appease followers
            self.bcast_heartbeats().await?;
        }
        self.snapshot_discard_log().await?;

        // reset the leader heartbeat hear timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer()?;
        }

        pf_info!("took snapshot up to: start {}", self.start_slot);
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
                entry: Some(SnapEntry::SlotInfo { start_slot }),
                end_offset,
            } => {
                self.snap_offset = end_offset;

                // recover start_slot info
                self.start_slot = start_slot;
                if start_slot > 0 {
                    self.last_commit = start_slot - 1;
                    self.last_exec = start_slot - 1;
                    self.last_snap = start_slot - 1;
                }

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

                // tell manager about my start_slot index
                self.control_hub.send_ctrl(CtrlMsg::SnapshotUpTo {
                    new_start: self.start_slot,
                })?;

                if self.start_slot > 0 {
                    pf_info!(
                        "recovered from snapshot: start {}",
                        self.start_slot
                    );
                }
                Ok(())
            }

            LogResult::Read { entry: None, .. } => {
                // snapshot file is empty. Write a 0 as start_slot and return
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = self
                    .snapshot_hub
                    .do_sync_action(
                        0, // using 0 as dummy log action ID
                        LogAction::Write {
                            entry: SnapEntry::SlotInfo { start_slot: 0 },
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
