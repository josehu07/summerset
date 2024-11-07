//! EPaxos -- command execution.
//!
//! NOTE: since Summerset Put commands always return the old value, they are
//!       effectively RMW commands; thus the commit vs. execute distinction in
//!       original EPaxos paper does not make a difference here. Put commands
//!       get replied to client only after execution. This does not make any
//!       difference to the performance metrics that we are interested in.

use super::*;

use crate::server::{ApiReply, ApiRequest};
use crate::utils::SummersetError;

// EPaxosReplica state machine execution
impl EPaxosReplica {
    // FIXME: add correct execution algo. task

    /// Handler of state machine exec result chan recv.
    pub(super) async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(col < self.start_col + self.insts[row].len());
        pf_trace!("executed cmd in instance at slot {} idx {}", slot, cmd_idx);

        let inst = &mut self.insts[row][col - self.start_col];
        debug_assert!(cmd_idx < inst.reqs.len());
        let (client, ref req) = inst.reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if inst.external && self.external_api.has_client(client) {
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
            if col == self.exec_bars[row] {
                while self.exec_bars[row]
                    < self.start_col + self.insts[row].len()
                {
                    let inst = &mut self.insts[row]
                        [self.exec_bars[row] - self.start_col];
                    if inst.status < Status::Executed {
                        break;
                    }
                    self.exec_bars[row] += 1;
                }
            }
        }

        Ok(())
    }
}
