//! `QuorumLeases` -- client request entrance.

use super::*;
use crate::server::{ApiReply, ApiRequest, Command, LogAction};
use crate::utils::{Bitmap, SummersetError};

// QuorumLeasesReplica client requests entrance
impl QuorumLeasesReplica {
    /// Treat read requests in the batch specially if:
    ///   - I'm the majority-leased stable leader
    ///   - I'm a majority-leased read leaseholder for a key
    ///   - simulating read leases
    ///
    /// Returns an updated req batch retaining commands that are are decided
    /// should go through normal consensus.
    #[allow(clippy::too_many_lines)]
    async fn treat_read_only_reqs(
        &mut self,
        req_batch: &mut ReqBatch,
    ) -> Result<(), SummersetError> {
        let mut strip_read_only = false;

        if self.is_stable_leader() {
            // conditions of majority-leased stable leader met, can reply
            // read-only commands directly back to clients by simply using
            // the last committed value
            for (client, req) in req_batch.iter() {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    // has to use the `do_sync_cmd()` API
                    let (old_results, cmd_result) = self
                        .state_machine
                        .do_sync_cmd(
                            Self::make_ro_command_id(*client, *req_id),
                            Command::Get { key: key.clone() },
                        )
                        .await?;
                    for (old_id, old_result) in old_results {
                        self.handle_cmd_result(old_id, old_result).await?;
                    }

                    self.external_api.send_reply(
                        ApiReply::normal(*req_id, Some(cmd_result)),
                        *client,
                    )?;
                    pf_trace!(
                        "replied -> client {} for read-only slead",
                        client
                    );
                    // [for access cnt stats only]
                    if self.config.record_node_cnts {
                        *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
                    }

                    strip_read_only = true;
                }
            }
        } else if self.is_local_reader()? {
            // conditions of majority-leased local reader met, can reply
            // read-only commands directly back to clients
            for (client, req) in req_batch.iter() {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    let (api_reply, is_retry) = match self
                        .inspect_highest_slot(key)?
                    {
                        None => {
                            // key not seen at all
                            (
                                ApiReply::normal(
                                    *req_id,
                                    Some(CommandResult::Get { value: None }),
                                ),
                                false,
                            )
                        }
                        Some((_, None)) => {
                            // highest slot not committed (rare case as leases
                            // should get actively revoked upon writes)
                            (
                                ApiReply::rq_retry(
                                    *req_id,
                                    Command::Get { key: key.clone() },
                                    self.leader,
                                ),
                                true,
                            )
                        }
                        Some((_, Some(value))) => {
                            // highest slot committed (should be almost always
                            // the case), can directly reply
                            (
                                ApiReply::normal(
                                    *req_id,
                                    Some(CommandResult::Get {
                                        value: Some(value),
                                    }),
                                ),
                                false,
                            )
                        }
                    };
                    self.external_api.send_reply(api_reply, *client)?;
                    pf_trace!(
                        "replied -> client {} read-only {}",
                        client,
                        if is_retry { "retry" } else { "rgood" }
                    );
                    // [for access cnt stats only]
                    if self.config.record_node_cnts {
                        *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
                    }

                    strip_read_only = true;
                }
            }
        } else if !self.is_leader() || self.bal_prepared == 0 {
            // not a leader and not holding enough read leases, then promptly
            // reply to read-only requests early to let clients retry on leader
            for (client, req) in req_batch.iter() {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    self.external_api.send_reply(
                        ApiReply::rq_retry(
                            *req_id,
                            Command::Get { key: key.clone() },
                            self.leader,
                        ),
                        *client,
                    )?;
                    pf_trace!("replied -> client {} read-only nlead", client);
                    // [for access cnt stats only]
                    if self.config.record_node_cnts {
                        *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
                    }

                    strip_read_only = true;
                }
            }
        }

        if strip_read_only {
            req_batch.retain(|(_, req)| req.read_only().is_none());
        }
        Ok(())
    }

    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        mut req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!("got request batch of size {}", batch_size);

        // examine the read-only requests in the batch and see if I can do
        // anything special about them first
        self.treat_read_only_reqs(&mut req_batch).await?;
        if req_batch.is_empty() {
            return Ok(());
        }

        // if I'm not a prepared leader, ignore client write requests
        if !self.is_leader() || self.bal_prepared == 0 {
            for (client, req) in req_batch {
                #[allow(clippy::match_wildcard_for_single_variants)]
                let req_id = match req {
                    ApiRequest::Req { id: req_id, .. }
                    | ApiRequest::Conf { id: req_id, .. } => Some(req_id),
                    _ => None,
                };
                if let Some(req_id) = req_id {
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
                    // [for access cnt stats only]
                    if self.config.record_node_cnts {
                        *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
                    }
                }
            }
            return Ok(());
        }

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist); fill it up with incoming data
        let slot = self.first_null_slot();
        {
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert_eq!(inst.status, Status::Null);
            inst.reqs.clone_from(&req_batch);
            Self::refresh_highest_slot(
                slot,
                &req_batch,
                &mut self.highest_slot,
            );
            inst.leader_bk = Some(LeaderBookkeeping {
                trigger_slot: 0,
                endprep_slot: 0,
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                accept_acks: Bitmap::new(self.population, false),
                accept_grant_sets: HashMap::new(),
            });
            inst.external = true;
        }

        // start the Accept phase for this instance
        let inst = &mut self.insts[slot - self.start_slot];
        inst.bal = self.bal_prepared;
        inst.status = Status::Accepting;
        pf_debug!("enter Accept phase for slot {} bal {}", slot, inst.bal);

        // record update to largest accepted ballot and corresponding data
        inst.voted = (inst.bal, req_batch.clone());
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, Status::Accepting),
            LogAction::Append {
                entry: WalEntry::AcceptData {
                    slot,
                    ballot: inst.bal,
                    reqs: req_batch.clone(),
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(
            "submitted AcceptData log action for slot {} bal {}",
            slot,
            inst.bal
        );

        // send Accept messages to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Accept {
                slot,
                ballot: inst.bal,
                reqs: req_batch,
            },
            None,
        )?;
        pf_trace!(
            "broadcast Accept messages for slot {} bal {}",
            slot,
            inst.bal
        );

        Ok(())
    }
}
