//! CRaft -- client request entrance.

use super::*;

use crate::utils::{SummersetError, Bitmap, RSCodeword};
use crate::server::{ApiRequest, ApiReply, LogAction};

// CRaftReplica client requests entrance
impl CRaftReplica {
    /// Handler of client request batch chan recv.
    pub fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!(self.id; "got request batch of size {}", batch_size);

        // if I'm not a leader, ignore client requests
        if self.role != Role::Leader {
            for (client, req) in req_batch {
                if let ApiRequest::Req { id: req_id, .. } = req {
                    // tell the client to try on known leader or just the
                    // next ID replica
                    let target = if let Some(peer) = self.leader {
                        peer
                    } else {
                        (self.id + 1) % self.population
                    };
                    self.external_api.send_reply(
                        ApiReply::Reply {
                            id: req_id,
                            result: None,
                            redirect: Some(target),
                        },
                        client,
                    )?;
                    pf_trace!(self.id; "redirected client {} to replica {}",
                                       client, target);
                }
            }
            return Ok(());
        }

        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw = RSCodeword::from_data(
            req_batch,
            self.majority,
            self.population - self.majority,
        )?;
        reqs_cw.compute_parity(Some(&self.rs_coder))?;

        // submit logger action to make this log entry durable
        let slot = self.start_slot + self.log.len();
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, slot, Role::Leader),
            LogAction::Append {
                entry: DurEntry::LogEntry {
                    entry: LogEntry {
                        term: self.curr_term,
                        reqs_cw: if self.full_copy_mode {
                            reqs_cw.subset_copy(
                                &Bitmap::from(
                                    self.population,
                                    (0..self.majority).collect(),
                                ),
                                false,
                            )?
                        } else {
                            reqs_cw.subset_copy(
                                &Bitmap::from(self.population, vec![self.id]),
                                false,
                            )?
                        },
                        external: true,
                        log_offset: 0,
                    },
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(self.id; "submitted leader append log action for slot {}", slot);

        // append an entry to in-memory log
        self.log.push(LogEntry {
            term: self.curr_term,
            reqs_cw,
            external: true,
            log_offset: 0,
        });

        Ok(())
    }
}
