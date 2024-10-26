//! SimplePush -- durable logging.

use super::*;

use crate::server::{ApiRequest, LogActionId, LogResult};
use crate::utils::SummersetError;

// SimplePushReplica durable WAL logging
impl SimplePushReplica {
    /// Handler of durable logging result chan recv.
    pub(super) async fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<WalEntry>,
    ) -> Result<(), SummersetError> {
        let inst_idx = action_id as usize;
        if inst_idx >= self.insts.len() {
            return logged_err!("invalid log action ID {} seen", inst_idx);
        }

        match log_result {
            LogResult::Append { now_size } => {
                debug_assert!(now_size >= self.wal_offset);
                self.wal_offset = now_size;
            }
            _ => {
                return logged_err!(
                    "unexpected log result type for {}: {:?}",
                    inst_idx,
                    log_result
                );
            }
        }

        let inst = &mut self.insts[inst_idx];
        if inst.durable {
            return logged_err!("duplicate log action ID {} seen", inst_idx);
        }
        inst.durable = true;

        // if pushed peers have all replied, submit execution commands
        if inst.pending_peers.count() == 0 {
            for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
                match req {
                    ApiRequest::Req { cmd, .. } => {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(inst_idx, cmd_idx),
                            cmd.clone(),
                        )?
                    }
                    _ => continue, // ignore other types of requests
                }
            }
        }

        // if this instance was pushed from a peer, reply to that peer
        if let Some((peer, src_inst_idx)) = inst.from_peer {
            debug_assert!(inst.pending_peers.count() == 0);
            self.transport_hub.send_msg(
                PushMsg::PushReply {
                    src_inst_idx,
                    num_reqs: inst.reqs.len(),
                },
                peer,
            )?;
        }

        Ok(())
    }
}
