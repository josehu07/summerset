//! RS-Paxos -- snapshotting & GC.

use std::cmp;
use std::collections::HashMap;

use super::*;

use crate::utils::SummersetError;
use crate::manager::CtrlMsg;
use crate::server::{Command, ApiRequest, LogAction, LogResult};

// RSPaxosReplica snapshotting & GC logic
impl RSPaxosReplica {
    /// Dump new key-value pairs to snapshot file.
    async fn snapshot_dump_kv_pairs(
        &mut self,
        new_start_slot: usize,
    ) -> Result<(), SummersetError> {
        // collect all key-value pairs put up to exec_bar
        let mut pairs = HashMap::new();
        for slot in self.start_slot..new_start_slot {
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert!(inst.reqs_cw.avail_data_shards() >= self.majority);
            for (_, req) in inst.reqs_cw.get_data()?.clone() {
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
        self.snapshot_hub.submit_action(
            0, // using 0 as dummy log action ID
            LogAction::Append {
                entry: SnapEntry::KVPairSet { pairs },
                sync: self.config.logger_sync,
            },
        )?;
        let (_, log_result) = self.snapshot_hub.get_result().await?;
        if let LogResult::Append { now_size } = log_result {
            self.snap_offset = now_size;
            Ok(())
        } else {
            logged_err!(
                self.id;
                "unexpected log result type"
            )
        }
    }

    /// Discard everything older than start_slot in durable WAL log.
    async fn snapshot_discard_log(&mut self) -> Result<(), SummersetError> {
        let cut_offset = if !self.insts.is_empty() {
            self.insts[0].wal_offset
        } else {
            self.wal_offset
        };

        // discard the log before cut_offset
        if cut_offset > 0 {
            self.storage_hub.submit_action(
                0,
                LogAction::Discard {
                    offset: cut_offset,
                    keep: 0,
                },
            )?;
            loop {
                let (action_id, log_result) =
                    self.storage_hub.get_result().await?;
                if action_id != 0 {
                    // normal log action previously in queue; process it
                    self.handle_log_result(action_id, log_result)?;
                } else {
                    if let LogResult::Discard {
                        offset_ok: true,
                        now_size,
                    } = log_result
                    {
                        debug_assert_eq!(
                            self.wal_offset - cut_offset,
                            now_size
                        );
                        self.wal_offset = now_size;
                    } else {
                        return logged_err!(
                            self.id;
                            "unexpected log result type or failed discard"
                        );
                    }
                    break;
                }
            }
        }

        // update inst.wal_offset for all remaining in-mem instances
        for inst in &mut self.insts {
            if inst.wal_offset > 0 {
                debug_assert!(inst.wal_offset >= cut_offset);
                inst.wal_offset -= cut_offset;
            }
        }

        Ok(())
    }

    /// Take a snapshot up to current exec_bar, then discard the in-mem log up
    /// to that index as well as outdate entries in the durable WAL log file.
    ///
    /// NOTE: the current implementation does not guard against crashes in the
    /// middle of taking a snapshot. Production quality implementations should
    /// make the snapshotting action "atomic".
    ///
    /// NOTE: the current implementation does not take care of InstallSnapshot
    /// messages (which is needed when some lagging follower has some slot
    /// which all other peers have snapshotted); we assume here that failed
    /// Accept messages will be retried indefinitely until success before its
    /// associated data gets discarded from leader's memory.
    pub async fn take_new_snapshot(&mut self) -> Result<(), SummersetError> {
        pf_debug!(self.id; "taking new snapshot: start {} exec {} snap {}",
                           self.start_slot, self.exec_bar, self.snap_bar);
        debug_assert!(self.exec_bar >= self.start_slot);

        let new_start_slot = cmp::min(self.snap_bar, self.exec_bar);
        if new_start_slot == self.start_slot {
            return Ok(());
        }

        // collect and dump all Puts in executed instances
        if self.is_leader() {
            // NOTE: broadcast heartbeats here to appease followers
            self.bcast_heartbeats()?;
        }
        self.snapshot_dump_kv_pairs(new_start_slot).await?;

        // write new slot info entry to the head of snapshot
        self.snapshot_hub.submit_action(
            0,
            LogAction::Write {
                entry: SnapEntry::SlotInfo {
                    start_slot: new_start_slot,
                },
                offset: 0,
                sync: self.config.logger_sync,
            },
        )?;
        let (_, log_result) = self.snapshot_hub.get_result().await?;
        match log_result {
            LogResult::Write {
                offset_ok: true, ..
            } => {}
            _ => {
                return logged_err!(self.id; "unexpected log result type or failed write");
            }
        }

        // update start_slot and discard all in-memory log instances up to exec_bar
        self.insts.drain(0..(new_start_slot - self.start_slot));
        self.start_slot = new_start_slot;

        // discarding everything older than start_slot in WAL log
        if self.is_leader() {
            // NOTE: broadcast heartbeats here to appease followers
            self.bcast_heartbeats()?;
        }
        self.snapshot_discard_log().await?;

        // reset the leader heartbeat hear timer
        self.kickoff_hb_hear_timer()?;

        pf_info!(self.id; "took snapshot up to: start {}", self.start_slot);
        Ok(())
    }

    /// Recover initial state from durable storage snapshot file.
    pub async fn recover_from_snapshot(
        &mut self,
    ) -> Result<(), SummersetError> {
        debug_assert_eq!(self.snap_offset, 0);

        // first, try to read the first several bytes, which should record the
        // start_slot index
        self.snapshot_hub
            .submit_action(0, LogAction::Read { offset: 0 })?;
        let (_, log_result) = self.snapshot_hub.get_result().await?;

        match log_result {
            LogResult::Read {
                entry: Some(SnapEntry::SlotInfo { start_slot }),
                end_offset,
            } => {
                self.snap_offset = end_offset;

                // recover necessary slot indices info
                self.start_slot = start_slot;
                self.commit_bar = start_slot;
                self.exec_bar = start_slot;
                self.snap_bar = start_slot;

                // repeatedly apply key-value pairs
                loop {
                    self.snapshot_hub.submit_action(
                        0,
                        LogAction::Read {
                            offset: self.snap_offset,
                        },
                    )?;
                    let (_, log_result) =
                        self.snapshot_hub.get_result().await?;

                    match log_result {
                        LogResult::Read {
                            entry: Some(SnapEntry::KVPairSet { pairs }),
                            end_offset,
                        } => {
                            // execute Put commands on state machine
                            for (key, value) in pairs {
                                self.state_machine.submit_cmd(
                                    0,
                                    Command::Put { key, value },
                                )?;
                                let _ = self.state_machine.get_result().await?;
                            }
                            // update snapshot file offset
                            self.snap_offset = end_offset;
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

                // tell manager about my start_slot index
                self.control_hub.send_ctrl(CtrlMsg::SnapshotUpTo {
                    new_start: self.start_slot,
                })?;

                if self.start_slot > 0 {
                    pf_info!(self.id; "recovered from snapshot: start {} commit {} exec {}",
                                      self.start_slot, self.commit_bar, self.exec_bar);
                }
                Ok(())
            }

            LogResult::Read { entry: None, .. } => {
                // snapshot file is empty. Write a 0 as start_slot and return
                self.snapshot_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: SnapEntry::SlotInfo { start_slot: 0 },
                        offset: 0,
                        sync: self.config.logger_sync,
                    },
                )?;
                let (_, log_result) = self.snapshot_hub.get_result().await?;
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = log_result
                {
                    self.snap_offset = now_size;
                    Ok(())
                } else {
                    logged_err!(self.id; "unexpected log result type or failed write")
                }
            }

            _ => {
                logged_err!(self.id; "unexpected log result type")
            }
        }
    }
}
