//! ChainRep -- client request entrance.

use super::*;

use crate::server::{ApiRequest, LogAction};
use crate::utils::SummersetError;

// ChainRepReplica client requests entrance
impl ChainRepReplica {
    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!("got request batch of size {}", batch_size);

        // if I'm not the head for Put requests or not the tail for Get
        // requests, ignore the request batch
        let read_only = req_batch[0].1.read_only(); // NOTE: only checking req 0
        if !((self.is_head() && !read_only) || (self.is_tail() && read_only)) {
            pf_warn!(
                "ignoring request batch: head? {} tail? {} read-only? {}",
                self.is_head(),
                self.is_tail(),
                read_only
            );
            return Ok(());
        }

        // depending on the request type...
        if self.is_tail() {
            // tail receives Get requests; submit to state machine right away
            let num_reqs = req_batch.len();
            for (client, req) in req_batch {
                if !req.read_only() {
                    return logged_err!("non read-only command seen at tail");
                }

                if let ApiRequest::Req { id: req_id, cmd } = req {
                    self.state_machine.submit_cmd(
                        Self::make_command_id(
                            req_id as usize,
                            client as usize,
                            true,
                        ),
                        cmd,
                    )?;
                }
            }
            pf_trace!("submitted {} read-only commands to exec", num_reqs);
        } else {
            // head receives Put requests; record a new log entry durably
            for (_, req) in &req_batch {
                if req.read_only() {
                    return logged_err!("read-only command seen at head");
                }
            }
            let slot = self.first_null_slot();
            self.log[slot].status = Status::Streaming;
            self.log[slot].reqs.clone_from(&req_batch);

            self.storage_hub.submit_action(
                slot as LogActionId,
                LogAction::Append {
                    entry: WalEntry {
                        slot,
                        reqs: req_batch,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!("submitted durable log action for slot {}", slot);
        }

        Ok(())
    }
}
