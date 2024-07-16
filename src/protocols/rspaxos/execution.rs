//! RS-Paxos -- command execution.

use super::*;

use crate::server::{ApiReply, ApiRequest, CommandId, CommandResult};
use crate::utils::SummersetError;

// RSPaxosReplica state machine execution
impl RSPaxosReplica {
    /// Handler of state machine exec result chan recv.
    pub(super) fn handle_cmd_result(
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
                self.external_api.send_reply(
                    ApiReply::Reply {
                        id: *req_id,
                        result: Some(cmd_result),
                        redirect: None,
                    },
                    client,
                )?;
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
