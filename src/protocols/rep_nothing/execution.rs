//! RepNothing -- command execution.

use super::*;

use crate::server::{ApiReply, ApiRequest, CommandId, CommandResult};
use crate::utils::SummersetError;

// RepNothingReplica state machine execution
impl RepNothingReplica {
    /// Handler of state machine exec result chan recv.
    pub(super) async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (inst_idx, cmd_idx) = Self::split_command_id(cmd_id);
        if inst_idx >= self.insts.len() {
            return logged_err!(
                "invalid command ID {} ({}|{}) seen",
                cmd_id,
                inst_idx,
                cmd_idx
            );
        }

        let inst = &mut self.insts[inst_idx];
        if cmd_idx >= inst.reqs.len() {
            return logged_err!(
                "invalid command ID {} ({}|{}) seen",
                cmd_id,
                inst_idx,
                cmd_idx
            );
        }
        if inst.execed[cmd_idx] {
            return logged_err!(
                "duplicate command index {}|{}",
                inst_idx,
                cmd_idx
            );
        }
        if !inst.durable {
            return logged_err!("instance {} is not durable yet", inst_idx);
        }
        inst.execed[cmd_idx] = true;

        // reply to the corresponding client of this request
        let (client, req) = &inst.reqs[cmd_idx];
        match req {
            ApiRequest::Req { id: req_id, .. } => {
                if self.external_api.has_client(*client) {
                    self.external_api.send_reply(
                        ApiReply::normal(*req_id, Some(cmd_result)),
                        *client,
                    )?;
                }
            }
            _ => {
                return logged_err!(
                    "unknown request type at {}|{}",
                    inst_idx,
                    cmd_idx
                )
            }
        }

        Ok(())
    }
}
