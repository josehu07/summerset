//! Bodega -- command execution.

use super::*;

use crate::server::{ApiReply, ApiRequest};
use crate::utils::SummersetError;

// BodegaReplica state machine execution
impl BodegaReplica {
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
        debug_assert!(cmd_idx < inst.reqs.len());
        let (client, ref req) = inst.reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if inst.external && self.external_api.has_client(client) {
                let read_only = cmd_result.read_only();
                self.external_api.send_reply(
                    ApiReply::normal(*req_id, Some(cmd_result)),
                    client,
                )?;
                pf_trace!(
                    "replied -> client {} for slot {} idx {}",
                    client,
                    slot,
                    cmd_idx
                );
                // [for access cnt stats only]
                if self.config.record_node_cnts && read_only {
                    if let Some(leader_bk) = inst.leader_bk.as_ref() {
                        for (peer, flag) in leader_bk.accept_acks.iter() {
                            if peer == self.id || flag {
                                *self
                                    .node_cnts_stats
                                    .get_mut(&peer)
                                    .unwrap() += 1;
                            }
                        }
                    }
                }
            }
        } else {
            return logged_err!("unexpected API request type");
        }

        // if all commands in this instance have been executed, set status to
        // Executed and update `exec_bar`
        if cmd_idx == inst.reqs.len() - 1 {
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
