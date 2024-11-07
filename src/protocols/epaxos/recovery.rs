//! EPaxos -- recovery from WAL.

use super::*;

use crate::server::{ApiRequest, LogAction, LogResult};
use crate::utils::SummersetError;

// EPaxosReplica recovery from WAL log
impl EPaxosReplica {
    /// Apply a durable storage log entry for recovery.
    async fn recover_apply_entry(
        &mut self,
        entry: WalEntry,
    ) -> Result<(), SummersetError> {
        match entry {
            WalEntry::PreAcceptSlot {
                slot,
                ballot,
                seq,
                deps,
                reqs,
            } => {
                let (row, col) = slot.unpack();
                if col < self.start_col {
                    return Ok(()); // ignore if slot index outdated
                }
                // locate instance in memory, filling in null instances if needed
                while self.start_col + self.insts.len() <= col {
                    let inst = self.null_instance();
                    self.insts[row].push(inst);
                }
                // update instance state
                let inst = &mut self.insts[row][col - self.start_col];
                inst.bal = ballot;
                inst.status = Status::PreAccepting;
                inst.seq = seq;
                inst.deps = deps;
                inst.reqs = reqs;
                Self::refresh_highest_cols(
                    slot,
                    &inst.reqs,
                    self.population,
                    &mut self.highest_cols,
                );
            }

            WalEntry::AcceptSlot {
                slot,
                ballot,
                seq,
                deps,
                reqs,
            } => {
                let (row, col) = slot.unpack();
                if col < self.start_col {
                    return Ok(()); // ignore if slot index outdated
                }
                // locate instance in memory, filling in null instances if needed
                while self.start_col + self.insts.len() <= col {
                    let inst = self.null_instance();
                    self.insts[row].push(inst);
                }
                // update instance state
                let inst = &mut self.insts[row][col - self.start_col];
                inst.bal = ballot;
                inst.status = Status::Accepting;
                inst.seq = seq;
                inst.deps = deps;
                inst.reqs = reqs;
                Self::refresh_highest_cols(
                    slot,
                    &inst.reqs,
                    self.population,
                    &mut self.highest_cols,
                );
            }

            WalEntry::CommitSlot {
                slot,
                ballot,
                seq,
                deps,
                reqs,
            } => {
                let (row, col) = slot.unpack();
                if col < self.start_col {
                    return Ok(()); // ignore if slot index outdated
                }
                // locate instance in memory, filling in null instances if needed
                while self.start_col + self.insts.len() <= col {
                    let inst = self.null_instance();
                    self.insts[row].push(inst);
                }
                // update instance state
                let inst = &mut self.insts[row][col - self.start_col];
                inst.bal = ballot;
                inst.status = Status::Committed;
                inst.seq = seq;
                inst.deps = deps;
                inst.reqs = reqs;
                Self::refresh_highest_cols(
                    slot,
                    &inst.reqs,
                    self.population,
                    &mut self.highest_cols,
                );
                // submit commands in contiguously committed instance to the
                // state machine
                if col == self.commit_bars[row] {
                    while self.commit_bars[row]
                        < self.start_col + self.insts[row].len()
                    {
                        let inst = &mut self.insts[row]
                            [self.commit_bars[row] - self.start_col];
                        if inst.status < Status::Committed {
                            break;
                        }
                        // FIXME: correct execution algo.
                        // // execute all commands in this instance on state machine
                        // // synchronously
                        // for (_, req) in inst.reqs.clone() {
                        //     if let ApiRequest::Req { cmd, .. } = req {
                        //         self.state_machine
                        //             .do_sync_cmd(
                        //                 0, // using 0 as dummy command ID
                        //                 cmd,
                        //             )
                        //             .await?;
                        //     }
                        // }
                        // update instance status, commit_bar and exec_bar
                        self.commit_bars[row] += 1;
                        self.exec_bars[row] += 1;
                        inst.status = Status::Executed;
                    }
                }
            }
        }

        Ok(())
    }

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
                    self.recover_apply_entry(entry).await?;
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
            if self.wal_offset > 0 {
                pf_info!(
                    "recovered from wal log: commit {} exec {}",
                    self.commit_bars[self.id as usize],
                    self.exec_bars[self.id as usize]
                );
            }
            Ok(())
        } else {
            logged_err!("unexpected log result type or failed truncate")
        }
    }
}
