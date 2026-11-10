//! `Crossword` -- command execution.

use super::*;
use crate::server::{ApiReply, ApiRequest, CommandId, CommandResult};
use crate::utils::{RSCodeword, SummersetError};

// CrosswordReplica state machine execution
impl CrosswordReplica {
    /// Makes the request batch of the committed instance at slot readable if
    /// enough shards are available, reconstructing and verifying it if needed.
    /// Returns false if not (yet) readable; a batch failing verification gets
    /// its shards I have not accepted durably dropped, to be fetched again.
    pub(super) fn payload_ready(&mut self, slot: usize) -> Result<bool, SummersetError> {
        let inst = &mut self.insts[slot - self.start_slot];
        let num_data_shards = inst.reqs_cw.num_data_shards();
        if inst.reqs_cw.avail_shards() < num_data_shards {
            return Ok(false);
        } else if inst.reqs_cw.avail_data_shards() >= num_data_shards {
            return Ok(true);
        }

        // have enough shards but need reconstruction
        let value_id = inst
            .value_id
            .expect("committed instance should have an identity");
        inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
        if let Err(e) = Self::verify_value_id(
            value_id,
            &mut inst.reqs_cw,
            self.rs_data_shards,
            self.rs_total_shards,
        ) {
            pf_warn!("dropping unpersisted shards of slot {}: {}", slot, e);
            inst.reqs_cw = match &inst.voted {
                Some(voted) if voted.value_id == value_id => voted.reqs_cw.clone(),
                _ => RSCodeword::from_null(
                    self.rs_data_shards,
                    self.rs_total_shards - self.rs_data_shards,
                )?,
            };
            return Ok(false);
        }
        Ok(true)
    }

    /// Submits the commands of contiguously committed instances from
    /// `commit_bar` on for execution, as long as their batches are readable.
    pub(super) fn advance_commit_bar(&mut self) -> Result<(), SummersetError> {
        while self.commit_bar < self.start_slot + self.insts.len() {
            let slot = self.commit_bar;
            if self.insts[slot - self.start_slot].status < Status::Committed {
                break;
            }
            if !self.payload_ready(slot)? {
                // can't execute if I don't have the complete request batch
                pf_debug!("postponing execution for slot {}", slot);
                break;
            }

            // submit commands in committed instance to the state machine for
            // execution
            let inst = &mut self.insts[slot - self.start_slot];
            let reqs = inst.reqs_cw.get_data()?;
            if reqs.is_empty() {
                inst.status = Status::Executed;
                // nothing to execute, so advance `exec_bar` here if at it
                if self.exec_bar == slot {
                    self.exec_bar += 1;
                }
            } else if inst.status == Status::Committed {
                for (cmd_idx, (client, req)) in reqs.iter().enumerate() {
                    if let ApiRequest::Req { id: req_id, cmd } = req {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(slot, cmd_idx),
                            cmd.clone(),
                            Some((*client, *req_id)),
                        )?;
                    }
                }
                pf_trace!("submitted {} exec commands for slot {}", reqs.len(), slot);
            }

            self.commit_bar += 1;
        }
        Ok(())
    }

    /// Handler of state machine exec result chan recv.
    pub(super) async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot < self.start_slot + self.insts.len());
        pf_trace!("executed cmd in instance at slot {} idx {}", slot, cmd_idx);

        let inst = &mut self.insts[slot - self.start_slot];
        let reqs = inst.reqs_cw.get_data()?;
        debug_assert!(cmd_idx < reqs.len());
        let (client, ref req) = reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if inst.external && self.external_api.has_client(client) {
                self.external_api
                    .send_reply(ApiReply::normal(*req_id, Some(cmd_result)), client)?;
                pf_trace!(
                    "replied -> client {} for slot {} idx {}",
                    client,
                    slot,
                    cmd_idx
                );
            }
        } else {
            return logged_err!("unexpected API request type");
        }

        // if all commands in this instance have been executed, set status to
        // Executed and update `exec_bar`
        if cmd_idx == reqs.len() - 1 {
            inst.status = Status::Executed;
            pf_debug!("executed all cmds in instance at slot {}", slot);

            // [for perf breakdown only]
            if self.is_leader()
                && let Some(sw) = self.bd_stopwatch.as_mut()
            {
                let _ = sw.record_now(slot, 5, None);
            }

            // update index of the first non-executed instance
            if slot == self.exec_bar {
                while self.exec_bar < self.start_slot + self.insts.len() {
                    let inst = &mut self.insts[self.exec_bar - self.start_slot];
                    if inst.status < Status::Executed {
                        break;
                    }
                    self.exec_bar += 1;
                }
            }
        }

        Ok(())
    }
}
