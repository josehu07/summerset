//! ChainRep -- peer-peer messaging.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, LogAction};

// ChainRepReplica peer-peer messages handling
impl ChainRepReplica {
    /// Handler of Propagate message from predecessor.
    fn handle_msg_propagate(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        pf_debug!(self.id; "received Propagate <- {} slot {} num_reqs {}",
                           peer, slot, reqs.len());

        // ignore if Propagate message not from my precessor
        if self.is_head() || self.predecessor().unwrap() != peer {
            return Ok(());
        }

        // locate log entry in memory, filling in null entries if needed
        while self.log.len() <= slot {
            self.log.push(Self::null_log_entry());
        }

        self.log[slot].status = Status::Streaming;
        self.log[slot].reqs.clone_from(&reqs);

        // record the new log entry durably
        self.storage_hub.submit_action(
            slot as LogActionId,
            LogAction::Append {
                entry: WalEntry { slot, reqs },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(self.id; "submitted durable log action for slot {}", slot);

        Ok(())
    }

    /// Handler of Propagate reply from successor.
    fn handle_msg_propagate_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received PropagateReply <- {} slot {}",
                           peer, slot);

        // ignore if Propagate reply not from my successor
        if self.is_tail() || self.successor().unwrap() != peer {
            return Ok(());
        }

        // update log entry status
        debug_assert!(slot < self.log.len());
        if self.log[slot].status != Status::Streaming {
            return Ok(());
        }
        self.log[slot].status = Status::Propagated;

        // update index of the first non-propagated entry
        if slot == self.prop_bar {
            while self.prop_bar < self.log.len() {
                if self.log[self.prop_bar].status < Status::Propagated {
                    break;
                }

                // submit commands in propagated entry to the state machine
                if self.log[self.prop_bar].reqs.is_empty() {
                    self.log[self.prop_bar].status = Status::Executed;
                } else {
                    for (cmd_idx, (_, req)) in
                        self.log[self.prop_bar].reqs.iter().enumerate()
                    {
                        if let ApiRequest::Req { cmd, .. } = req {
                            self.state_machine.submit_cmd(
                                Self::make_command_id(
                                    self.prop_bar,
                                    cmd_idx,
                                    false,
                                ),
                                cmd.clone(),
                            )?;
                        } else {
                            continue; // ignore other types of requests
                        }
                    }
                    pf_trace!(self.id;
                              "submitted {} exec commands for slot {}",
                              self.log[self.prop_bar].reqs.len(), self.prop_bar);
                }

                self.prop_bar += 1;
            }
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    pub fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::Propagate { slot, reqs } => {
                self.handle_msg_propagate(peer, slot, reqs)
            }
            PeerMsg::PropagateReply { slot } => {
                self.handle_msg_propagate_reply(peer, slot)
            }
        }
    }
}
