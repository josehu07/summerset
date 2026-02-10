//! `Bodega` -- local read optimization related actions.

use super::*;

impl BodegaReplica {
    /// Checks if I'm a stable leader as in a majority-leased config.
    #[inline]
    pub(super) fn is_stable_leader(&self) -> bool {
        self.is_leader()
            && self.bal_prepared > 0
            && ((self.bal_max_seen == self.bal_prepared
                 && self.lease_manager.lease_cnt() >= self.quorum_cnt
                 && self.commit_bar >= self.peer_accept_max)
                // [for benchmarking purposes only]
                || self.config.sim_read_lease)
    }

    /// Checks if I'm a responder for a key as in a majority-leased config.
    /// Assumes the `lease_cnt` + 1 >= majority check has been done already.
    #[inline]
    pub(super) fn is_responder_for(&self, key: &String) -> bool {
        (self.bodega_conf.is_responder_by_key(key, self.id)
         && self.commit_bar >= self.peer_accept_max)
        // [for benchmarking purposes only]
        || self.config.sim_read_lease
    }

    /// The commit condition check for normal consensus instances. Besides
    /// requiring an `AcceptReply` quorum size of at least majority, it also
    /// requires that replies from all responders for updated keys have been
    /// received.
    pub(super) fn commit_condition(
        leader_bk: &LeaderBookkeeping,
        req_batch: &ReqBatch,
        quorum_cnt: u8,
        bodega_conf: &RespondersConf<()>,
    ) -> Result<bool, SummersetError> {
        if leader_bk.accept_acks.count() < quorum_cnt {
            return Ok(false);
        }

        for (_, req) in req_batch {
            if let Some(key) = req.write_key()
                && let Some((responders, _)) =
                    bodega_conf.get_responders_by_key(key)
            {
                for (responder, flag) in responders.iter() {
                    if flag && !leader_bk.accept_acks.get(responder)? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    /// Update the `highest_slot` tracking info given a new request batch about
    /// to be saved into a slot.
    pub(super) fn refresh_highest_slot(
        slot: usize,
        reqs: &ReqBatch,
        highest_slot: &mut HashMap<String, usize>,
    ) {
        for (_, req) in reqs {
            if let ApiRequest::Req {
                cmd: Command::Put { key, .. },
                ..
            } = req
            {
                if let Some(highest_slot) = highest_slot.get_mut(key) {
                    *highest_slot = slot.max(*highest_slot);
                } else {
                    highest_slot.insert(key.clone(), slot);
                }
            }
        }
    }

    /// Get the value at the highest slot index ever seen for a key and whether
    /// it has been committed (or has seen >= majority `AcceptNotice`s).
    // NOTE: current implementation might loop through requests in the batch of
    //       that slot at each inspect call; good enough but can be improved
    pub(super) fn inspect_highest_slot(
        &self,
        key: &String,
    ) -> Result<Option<(usize, Option<String>)>, SummersetError> {
        if let Some(&slot) = self.highest_slot.get(key) {
            if slot < self.start_slot
                || slot >= self.start_slot + self.insts.len()
            {
                // slot has been GCed or not locatable for some reason; we play
                // safe and return with not-committed status
                Ok(Some((slot, None)))
            } else {
                let inst = &self.insts[slot - self.start_slot];
                if inst.status < Status::Accepting
                    || (inst.status == Status::Accepting
                        && (inst.replica_bk.is_none()
                            || inst // NOTE: can make nested conditions prettier
                                .replica_bk
                                .as_ref()
                                .unwrap()
                                .accept_notices
                                .count()
                                < self.quorum_cnt))
                {
                    // instance not bound to be committed on me yet
                    Ok(Some((slot, None)))
                } else {
                    // instance committed, return the latest value for the key
                    // in batch
                    for (_, req) in inst.reqs.iter().rev() {
                        if let ApiRequest::Req {
                            cmd: Command::Put { key: k, value },
                            ..
                        } = req
                            && k == key
                        {
                            return Ok(Some((slot, Some(value.clone()))));
                        }
                    }
                    logged_err!(
                        "no write to key '{}' at slot {}; wrong highest_slot",
                        key,
                        slot
                    )
                }
            }
        } else {
            // never seen this key
            Ok(None)
        }
    }

    /// Read only req on key: I am not leader nor responder in a stable config.
    pub(super) fn read_on_improper_node(
        &mut self,
        client: ClientId,
        req_id: RequestId,
        key: String,
    ) -> Result<(), SummersetError> {
        self.external_api.send_reply(
            ApiReply::rq_retry(
                req_id,
                Command::Get { key },
                self.bodega_conf.leader,
            ),
            client,
        )?;
        pf_trace!("replied -> client {} read-only nlead", client);
        // [for access cnt stats only]
        if self.config.record_node_cnts {
            *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
        }

        Ok(())
    }

    /// Read only req on key: I am the stable leader.
    pub(super) async fn read_on_stable_leader(
        &mut self,
        client: ClientId,
        req_id: RequestId,
        key: String,
    ) -> Result<(), SummersetError> {
        // has to use the `do_sync_cmd()` API
        let (old_results, cmd_result) = self
            .state_machine
            .do_sync_cmd(
                Self::make_ro_command_id(client, req_id),
                Command::Get { key },
            )
            .await?;
        for (old_id, old_result) in old_results {
            self.handle_cmd_result(old_id, old_result).await?;
        }

        self.external_api
            .send_reply(ApiReply::normal(req_id, Some(cmd_result)), client)?;
        pf_trace!("replied -> client {} for read-only slead", client);
        // [for access cnt stats only]
        if self.config.record_node_cnts {
            *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
        }

        Ok(())
    }

    /// Read only req on key: I am a responder in a stable config.
    pub(super) fn read_on_good_responder(
        &mut self,
        client: ClientId,
        req_id: RequestId,
        key: String,
    ) -> Result<(), SummersetError> {
        let api_reply = match self.inspect_highest_slot(&key)? {
            None => {
                // key not seen at all
                Some((
                    ApiReply::normal(
                        req_id,
                        Some(CommandResult::Get { value: None }),
                    ),
                    false,
                ))
            }
            Some((slot, None)) => {
                // highest slot not committed
                let inst = &mut self.insts[slot - self.start_slot];
                #[allow(clippy::unnecessary_unwrap)]
                if inst.status != Status::Accepting || inst.replica_bk.is_none()
                {
                    // not even in Accepting status, very rare case; just reject
                    Some((
                        ApiReply::rq_retry(
                            req_id,
                            Command::Get { key },
                            self.bodega_conf.leader,
                        ),
                        true,
                    ))
                } else {
                    // add to this instance slot's holding queue; will be
                    // released when >= majority AcceptNotices received or the
                    // CommitNotice from leader received, whichever is eariler
                    inst.replica_bk.as_mut().unwrap().holding_reads.push((
                        client,
                        ApiRequest::Req {
                            id: req_id,
                            cmd: Command::Get { key },
                        },
                    ));
                    None
                }
            }
            Some((_, Some(value))) => {
                // highest slot committed, can directly reply
                Some((
                    ApiReply::normal(
                        req_id,
                        Some(CommandResult::Get { value: Some(value) }),
                    ),
                    false,
                ))
            }
        };

        if let Some((api_reply, is_retry)) = api_reply {
            self.external_api.send_reply(api_reply, client)?;
            pf_trace!(
                "replied -> client {} read-only {}",
                client,
                if is_retry { "retry" } else { "rgood" }
            );
            // [for access cnt stats only]
            if self.config.record_node_cnts {
                *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
            }
        }

        Ok(())
    }

    /// If in Accepting status and >= majority `AcceptNotice`s received (or even
    /// in Committed status), commit is bound to happen (or has already happened)
    /// for an instance, so we can now reply to all holding reads. Otherwise,
    /// this function is called to trigger prompt rejections of held read reqs.
    pub(super) fn release_held_read_reqs(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        let inst = &mut self.insts[slot - self.start_slot];
        if inst.replica_bk.is_none() {
            return Ok(());
        }
        let replica_bk = inst.replica_bk.as_mut().unwrap();
        if inst.status < Status::Accepting
            || (inst.status == Status::Accepting
                && replica_bk.accept_notices.count() < self.quorum_cnt)
        {
            for (client, req) in replica_bk.holding_reads.drain(..) {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    self.external_api.send_reply(
                        ApiReply::rq_retry(
                            req_id,
                            Command::Get { key },
                            self.bodega_conf.leader,
                        ),
                        client,
                    )?;
                    pf_trace!("replied -> client {} read-only hfail", client);
                    // [for access cnt stats only]
                    if self.config.record_node_cnts {
                        *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
                    }
                }
            }
            return Ok(());
        }

        // commit is bound to happen, can reply with the contained values
        'reqs_loop: for (client, req) in replica_bk.holding_reads.drain(..) {
            if let ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { key },
            } = req
            {
                // locate the last write in batch for the key
                for (_, req) in inst.reqs.iter().rev() {
                    if let ApiRequest::Req {
                        cmd: Command::Put { key: k, value },
                        ..
                    } = req
                        && k == &key
                    {
                        self.external_api.send_reply(
                            ApiReply::normal(
                                req_id,
                                Some(CommandResult::Get {
                                    value: Some(value.clone()),
                                }),
                            ),
                            client,
                        )?;
                        pf_trace!(
                            "replied -> client {} read-only hgood",
                            client
                        );
                        // [for access cnt stats only]
                        if self.config.record_node_cnts {
                            *self.node_cnts_stats.get_mut(&self.id).unwrap() +=
                                1;
                        }
                        continue 'reqs_loop;
                    }
                }
                // although should not reach here, playing safe...
                self.external_api.send_reply(
                    ApiReply::rq_retry(
                        req_id,
                        Command::Get { key },
                        self.bodega_conf.leader,
                    ),
                    client,
                )?;
                pf_trace!("replied -> client {} read-only hfail", client);
                // [for access cnt stats only]
                if self.config.record_node_cnts {
                    *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
                }
            }
        }

        Ok(())
    }
}
