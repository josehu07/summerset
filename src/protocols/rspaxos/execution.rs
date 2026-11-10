//! `RS-Paxos` -- command execution.

use super::*;
use crate::server::{ApiReply, ApiRequest, CommandId, CommandResult};
use crate::utils::SummersetError;

// RSPaxosReplica state machine execution
impl RSPaxosReplica {
    /// Submits the commands of contiguously committed instances from
    /// `commit_bar` on for execution, as long as their batches can be
    /// reconstructed from the shards available.
    pub(super) fn advance_commit_bar(&mut self) -> Result<(), SummersetError> {
        while self.commit_bar < self.start_slot + self.insts.len() {
            let slot = self.commit_bar;
            let inst = &mut self.insts[slot - self.start_slot];
            if inst.status < Status::Committed {
                break;
            }
            if inst.reqs_cw.avail_shards() < self.majority {
                // can't execute if I don't have the complete request batch
                pf_debug!("postponing execution for slot {}", slot);
                break;
            } else if inst.reqs_cw.avail_data_shards() < self.majority {
                // have enough shards but need reconstruction
                inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
            }

            // submit commands in committed instance to the state machine for
            // execution
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
