//! Raft -- command execution.

use super::*;

use crate::utils::SummersetError;
use crate::server::{CommandResult, CommandId, ApiRequest, ApiReply};

// RaftReplica state machine execution
impl RaftReplica {
    /// Handler of state machine exec result chan recv.
    pub fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot < self.start_slot + self.log.len());
        pf_trace!(self.id; "executed cmd in entry at slot {} idx {}",
                           slot, cmd_idx);

        let entry = &mut self.log[slot - self.start_slot];
        debug_assert!(cmd_idx < entry.reqs.len());
        let (client, ref req) = entry.reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if entry.external && self.external_api.has_client(client) {
                self.external_api.send_reply(
                    ApiReply::Reply {
                        id: *req_id,
                        result: Some(cmd_result),
                        redirect: None,
                    },
                    client,
                )?;
                pf_trace!(self.id; "replied -> client {} for slot {} idx {}",
                                   client, slot, cmd_idx);
            }
        } else {
            return logged_err!(self.id; "unexpected API request type");
        }

        // if all commands in this entry have been executed, update last_exec
        if cmd_idx == entry.reqs.len() - 1 {
            pf_debug!(self.id; "executed all cmds in entry at slot {}", slot);
            self.last_exec = slot;
        }

        Ok(())
    }
}
