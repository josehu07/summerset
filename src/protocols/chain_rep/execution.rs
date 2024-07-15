//! ChainRep -- command execution.

use super::*;

use crate::server::{ApiReply, ApiRequest, RequestId};
use crate::utils::SummersetError;

// ChainRepReplica state machine execution
impl ChainRepReplica {
    /// Handler of state machine exec result chan recv.
    pub(super) fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot_or_req_id, cmd_idx_or_client, read_only) =
            Self::split_command_id(cmd_id);
        if read_only {
            // tail executed read-only command
            debug_assert!(self.is_tail());
            let (req_id, client) =
                (slot_or_req_id as RequestId, cmd_idx_or_client as ClientId);
            pf_trace!(
                "executed read-only cmd for client {} req_id {}",
                client,
                req_id
            );

            // reply back to the client
            if self.external_api.has_client(client) {
                self.external_api.send_reply(
                    ApiReply::Reply {
                        id: req_id,
                        result: Some(cmd_result),
                        redirect: None,
                    },
                    client,
                )?;
                pf_trace!(
                    "replied -> client {} for read-only req {}",
                    client,
                    req_id
                );
            }
        } else {
            // node executed non read-only command
            let (slot, cmd_idx) = (slot_or_req_id, cmd_idx_or_client);
            debug_assert!(slot < self.log.len());
            debug_assert!(cmd_idx < self.log[slot].reqs.len());
            if self.log[slot].status != Status::Propagated {
                return Ok(());
            }
            pf_trace!(
                "executed non read-only cmd of slot {} idx {}",
                slot,
                cmd_idx
            );

            // if I'm the tail, reply back to the client
            if self.is_tail() {
                let (client, ref req) = self.log[slot].reqs[cmd_idx];
                if let ApiRequest::Req { id: req_id, .. } = req {
                    if self.external_api.has_client(client) {
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
            }

            // if all commands in this entry have been executed, set status to
            // Executed and update `exec_bar`
            if cmd_idx == self.log[slot].reqs.len() - 1 {
                self.log[slot].status = Status::Executed;
                pf_debug!("executed all cmds in entry at slot {}", slot);

                // update index of the first non-executed entry
                if slot == self.exec_bar {
                    while self.exec_bar < self.log.len() {
                        if self.log[self.exec_bar].status < Status::Executed {
                            break;
                        }
                        self.exec_bar += 1;
                    }
                }
            }
        }

        Ok(())
    }
}
