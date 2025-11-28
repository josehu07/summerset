//! `CRaft` -- client request entrance.

use super::*;
use crate::server::{ApiReply, ApiRequest, Command, CommandResult, LogAction};
use crate::utils::{Bitmap, RSCodeword, SummersetError};

// CRaftReplica client requests entrance
impl CRaftReplica {
    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        mut req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!("got request batch of size {}", batch_size);

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
                        ApiReply::redirect(req_id, Some(target)),
                        client,
                    )?;
                    pf_trace!(
                        "redirected client {} to replica {}",
                        client,
                        target
                    );
                }
            }
            return Ok(());
        }

        // [for benchmarking purposes only]
        // if simulating read leases, extract all the reads and immediately
        // reply to them with a dummy value
        if self.config.sim_read_lease {
            for (client, req) in &req_batch {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { .. },
                } = req
                {
                    self.external_api.send_reply(
                        ApiReply::normal(
                            *req_id,
                            Some(CommandResult::Get { value: None }),
                        ),
                        *client,
                    )?;
                    pf_trace!("replied -> client {} for read-only cmd", client);
                }
            }

            req_batch.retain(|(_, req)| req.read_only().is_none());
            if req_batch.is_empty() {
                return Ok(());
            }
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
                                &Bitmap::from((
                                    self.population,
                                    0..self.majority,
                                )),
                                false,
                            )?
                        } else {
                            reqs_cw.subset_copy(
                                &Bitmap::from((self.population, vec![self.id])),
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
        pf_trace!("submitted leader append log action for slot {}", slot);

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
