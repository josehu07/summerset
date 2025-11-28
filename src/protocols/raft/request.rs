//! `Raft` -- client request entrance.

use super::*;
use crate::server::{ApiReply, ApiRequest, Command, CommandResult, LogAction};
use crate::utils::SummersetError;

// RaftReplica client requests entrance
impl RaftReplica {
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

        // append an entry to in-memory log
        let entry = LogEntry {
            term: self.curr_term,
            reqs: req_batch,
            external: true,
            log_offset: 0,
        };
        let slot = self.start_slot + self.log.len();
        self.log.push(entry.clone());

        // submit logger action to make this log entry durable
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, slot, Role::Leader),
            LogAction::Append {
                entry: DurEntry::LogEntry { entry },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!("submitted leader append log action for slot {}", slot);

        Ok(())
    }
}
