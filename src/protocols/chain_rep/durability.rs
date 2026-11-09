//! `ChainRep` -- durable logging.

use super::*;
use crate::server::{ApiRequest, LogResult};
use crate::utils::SummersetError;

// ChainRepReplica durable WAL logging
impl ChainRepReplica {
    /// Handler of durable logging result chan recv.
    pub(super) async fn handle_log_result(
        &mut self,
        slot: usize,
        log_result: LogResult<WalEntry>,
    ) -> Result<(), SummersetError> {
        debug_assert!(slot < self.log.len());
        let LogResult::Append { offset, now_size } = log_result else {
            return logged_err!("unexpected log result type: {:?}", log_result);
        };
        debug_assert!(now_size >= self.wal_offset);
        self.wal_offset = now_size;
        // entries complete in log order, so the first one of the slot tells
        // where its records start
        self.log[slot].wal_offset.get_or_insert(offset);
        if self.log[slot].status != Status::Streaming {
            return Ok(());
        }
        pf_trace!("finished durable logging for slot {}", slot);

        // depending on whether I'm the tail...
        if self.is_tail() {
            // no more propagation; update status
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
                        for (cmd_idx, (_, req)) in self.log[self.prop_bar].reqs.iter().enumerate() {
                            if let ApiRequest::Req { cmd, .. } = req {
                                self.state_machine.submit_cmd(
                                    Self::make_command_id(self.prop_bar, cmd_idx, false),
                                    cmd.clone(),
                                )?;
                            }
                        }
                        pf_trace!(
                            "submitted {} exec commands for slot {}",
                            self.log[self.prop_bar].reqs.len(),
                            self.prop_bar
                        );
                    }

                    self.prop_bar += 1;
                }
            }
        } else {
            // propagate down to my successor
            debug_assert!(self.successor().is_some());
            self.transport_hub.send_msg(
                PeerMsg::Propagate {
                    slot,
                    reqs: self.log[slot].reqs.clone(),
                },
                self.successor().unwrap(),
            )?;
            pf_trace!(
                "sent Propagate -> {} for slot {}",
                self.successor().unwrap(),
                slot
            );

            // if I'm not the head, also reply back to my predecessor
            if !self.is_head() {
                debug_assert!(self.predecessor().is_some());
                self.transport_hub.send_msg(
                    PeerMsg::PropagateReply { slot },
                    self.predecessor().unwrap(),
                )?;
                pf_trace!(
                    "sent PropagateReply -> {} for slot {}",
                    self.predecessor().unwrap(),
                    slot
                );
            }
        }

        Ok(())
    }
}
