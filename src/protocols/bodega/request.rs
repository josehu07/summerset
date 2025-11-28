//! `Bodega` -- client request entrance.

use super::*;
use crate::server::{ApiReply, ApiRequest, Command, LogAction};
use crate::utils::{Bitmap, SummersetError};

// BodegaReplica client requests entrance
impl BodegaReplica {
    /// Treats user-initiated configuration change in the batch, applies the
    /// last one in batch and replies to all of them in place here. Returns an
    /// updated req batch.
    async fn treat_conf_change_reqs(
        &mut self,
        mut req_batch: ReqBatch,
    ) -> Result<ReqBatch, SummersetError> {
        let mut changes = vec![];
        for (client, req) in &req_batch {
            if let ApiRequest::Conf { id: req_id, delta } = req {
                changes.push((*client, *req_id, delta.clone()));
            }
        }

        if !changes.is_empty() {
            let last_i = changes.len() - 1;
            for (i, (client, req_id, delta)) in changes.into_iter().enumerate()
            {
                if i < last_i {
                    // not the last conf change req in batch, not applied;
                    // directly reply to client as ignored
                    self.external_api.send_reply(
                        ApiReply::Conf {
                            id: req_id,
                            success: false,
                        },
                        client,
                    )?;
                    pf_trace!(
                        "replied -> client {} bal {} conf ignored",
                        client,
                        self.bal_max_seen
                    );
                } else {
                    // apply to the current config and announce the new config
                    let mut new_conf = self.bodega_conf.clone();
                    if delta.reset {
                        new_conf.reset_full();
                    } else {
                        if let Some(leader) = delta.leader {
                            new_conf.set_leader(leader);
                        }
                        if let Some(responders) = delta.responders {
                            new_conf.set_responders(
                                delta.range.as_ref(),
                                responders,
                                None,
                            )?;
                        }
                    }

                    if new_conf != self.bodega_conf {
                        self.announce_new_conf(new_conf.clone()).await?;
                        self.control_hub.send_ctrl(
                            CtrlMsg::RespondersConf {
                                conf_num: self.bal_max_seen,
                                new_conf,
                            },
                        )?;
                    }

                    // reply to client as successfully applied
                    self.external_api.send_reply(
                        ApiReply::Conf {
                            id: req_id,
                            success: true,
                        },
                        client,
                    )?;
                    pf_trace!(
                        "replied -> client {} bal {} conf applied",
                        client,
                        self.bal_max_seen
                    );
                }
            }
        }

        req_batch.retain(|(_, req)| !req.conf_change());
        Ok(req_batch)
    }

    /// Treats read requests in the batch specially if:
    ///   - I'm the majority-leased stable leader
    ///   - I'm a responder for a key in the majority-leased active config
    ///   - simulating read leases
    ///
    /// Returns an updated req batch retaining commands that are are decided
    /// should go through normal consensus.
    async fn treat_read_only_reqs(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<ReqBatch, SummersetError> {
        let mut filtered = vec![];

        if self.is_stable_leader() {
            // conditions of majority-leased stable leader met, can reply
            // read-only commands directly back to clients by simply using
            // the last committed value
            for (client, req) in req_batch {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    self.read_on_stable_leader(client, req_id, key).await?;
                    continue;
                }
                filtered.push((client, req));
            }
        } else if self.lease_manager.lease_cnt() >= self.quorum_cnt
            || self.config.sim_read_lease
        {
            // else if I have a majority-leased responders config, then process
            // read-only commands for keys where I'm a responder of
            for (client, req) in req_batch {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    if self.is_responder_for(&key) {
                        self.read_on_good_responder(client, req_id, key)?;
                    } else if !self.is_leader() || self.bal_prepared == 0 {
                        // not a responder for this key not the leader, so the
                        // same case as the final else branch below
                        // NOTE: a cleaner implementation should certainly merge
                        //       this with that branch and clean up this func a
                        //       bit; it's getting way too long
                        self.read_on_improper_node(client, req_id, key)?;
                    }
                    continue;
                }
                filtered.push((client, req));
            }
        } else if !self.is_leader() || self.bal_prepared == 0 {
            // else if not a leader and not sure about current config, then
            // promptly reply to read-only requests early to let clients retry
            // on leader
            for (client, req) in req_batch {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    self.read_on_improper_node(client, req_id, key)?;
                    continue;
                }
                filtered.push((client, req));
            }
        } else {
            // otherwise, I'm a (possibly unstable) leader, process the entire
            // request batch as a normal consensus instance
            filtered = req_batch;
        }

        Ok(filtered)
    }

    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!("got request batch of size {}", batch_size);

        // examine the user-initiated configuration change requests in the
        // batch and do them first
        let req_batch = self.treat_conf_change_reqs(req_batch).await?;
        if req_batch.is_empty() {
            return Ok(());
        }

        // examine the read-only requests in the batch and see if I can do
        // anything special about them
        let req_batch = self.treat_read_only_reqs(req_batch).await?;
        if req_batch.is_empty() {
            return Ok(());
        }

        // if I'm not a prepared leader, ignore client write requests
        if !self.is_leader() || self.bal_prepared == 0 {
            for (client, req) in req_batch {
                if let ApiRequest::Req { id: req_id, .. } = req {
                    // tell the client to try on known leader or just the
                    // next ID replica
                    let target = if let Some(peer) = self.bodega_conf.leader {
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
