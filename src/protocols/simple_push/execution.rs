//! SimplePush -- command execution.

use super::*;

use crate::utils::SummersetError;
use crate::server::{CommandResult, CommandId, ApiRequest, ApiReply};

// SimplePushReplica state machine execution
impl SimplePushReplica {
    /// Handler of state machine exec result chan recv.
    pub fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (inst_idx, cmd_idx) = Self::split_command_id(cmd_id);
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, inst_idx, cmd_idx);
        }

        let inst = &mut self.insts[inst_idx];
        if cmd_idx >= inst.reqs.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, inst_idx, cmd_idx);
        }
        if inst.execed[cmd_idx] {
            return logged_err!(self.id; "duplicate command index {}|{}", inst_idx, cmd_idx);
        }
        if !inst.durable {
            return logged_err!(self.id; "instance {} is not durable yet", inst_idx);
        }
        if inst.pending_peers.count() > 0 {
            return logged_err!(self.id; "instance {} has pending peers", inst_idx);
        }
        inst.execed[cmd_idx] = true;

        // if this instance was directly from client, reply to the
        // corresponding client of this request
        if inst.from_peer.is_none() {
            let (client, req) = &inst.reqs[cmd_idx];
            match req {
                ApiRequest::Req { id: req_id, .. } => {
                    if self.external_api.has_client(*client) {
                        self.external_api.send_reply(
                            ApiReply::Reply {
                                id: *req_id,
                                result: Some(cmd_result),
                                redirect: None,
                            },
                            *client,
                        )?;
                    }
                }
                _ => {
                    return logged_err!(self.id; "unknown request type at {}|{}", inst_idx, cmd_idx)
                }
            }
        }

        Ok(())
    }
}
