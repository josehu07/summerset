//! `Bodega` -- local read optimization related actions.

use std::mem;

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
                && let Some((responders, _)) = bodega_conf.get_responders_by_key(key)
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

    /// Records an early notice of `peer` having durably accepted an instance at
    /// `ballot`, where `peer` may be myself once my own accept is logged, and
    /// releases the read reqs held on it that the notices now suffice for.
    pub(super) fn record_accept_notice(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        let Some(replica_bk) = self.insts[slot - self.start_slot].replica_bk.as_mut() else {
            return Ok(());
        };
        debug_assert!(ballot >= replica_bk.accept_notices_bal);
        if ballot > replica_bk.accept_notices_bal {
            replica_bk.accept_notices_bal = ballot;
            replica_bk.accept_notices.clear();
        }
        replica_bk.accept_notices.set(peer, true)?;

        if replica_bk.accept_notices.count() >= self.quorum_cnt {
            self.release_held_read_reqs(slot)?;
        }
        Ok(())
    }

    /// Checks if what an instance writes to a key can be served to reads: it
    /// has committed, or it is Accepting with `AcceptNotice`s of its ballot
    /// from a majority, my own durable accept counting as one (so is bound to
    /// commit), that include the leader and all other responders for the key
    /// (so none of them serves an older value anymore). The leader records no
    /// notices, so it waits for the commit.
    fn is_locally_readable(&self, inst: &Instance, key: &String) -> bool {
        if inst.status != Status::Accepting {
            return inst.status >= Status::Committed;
        }
        let Some(replica_bk) = inst.replica_bk.as_ref() else {
            return false;
        };
        let notices = &replica_bk.accept_notices;
        let noticed = |peer| notices.get(peer).unwrap_or(false);
        replica_bk.accept_notices_bal == inst.bal
            && notices.count() >= self.quorum_cnt
            && self.bodega_conf.leader.is_some_and(noticed)
            && self
                .bodega_conf
                .get_responders_by_key(key)
                .is_some_and(|(responders, _)| {
                    responders
                        .iter()
                        .all(|(r, flag)| !flag || r == self.id || noticed(r))
                })
    }

    /// Checks if, with me as leader, responders for a key may get to serve the
    /// latest write to it ahead of my executed state: early `AcceptNotice`s
    /// are on, it has follower responders, and the write has not committed in
    /// order yet. If so, I must serve the key as a responder would.
    fn responders_may_outrun(&self, key: &String) -> bool {
        self.config.urgent_accept_notice
            && self
                .highest_slot
                .get(key)
                .is_some_and(|&slot| slot >= self.commit_bar)
            && self
                .bodega_conf
                .get_responders_by_key(key)
                .is_some_and(|(responders, _)| {
                    responders.iter().any(|(r, flag)| flag && r != self.id)
                })
    }

    /// Finds the value of the last write to a key in a request batch.
    fn last_write_to_key<'a>(reqs: &'a ReqBatch, key: &String) -> Option<&'a String> {
        reqs.iter().rev().find_map(|(_, req)| match req {
            ApiRequest::Req {
                cmd: Command::Put { key: k, value },
                ..
            } if k == key => Some(value),
            _ => None,
        })
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
    /// it has been committed (or has seen enough `AcceptNotice`s).
    // NOTE: current implementation might loop through requests in the batch of
    //       that slot at each inspect call; good enough but can be improved
    fn inspect_highest_slot(&self, key: &String) -> Option<(usize, Option<String>)> {
        if let Some(&slot) = self.highest_slot.get(key) {
            if slot < self.start_slot || slot >= self.start_slot + self.insts.len() {
                // slot has been GCed or not locatable for some reason; we play
                // safe and return with not-committed status
                Some((slot, None))
            } else {
                let inst = &self.insts[slot - self.start_slot];
                if self.is_locally_readable(inst, key) {
                    // instance committed, return the latest value for the key
                    // in batch; if the write seen there got replaced (e.g., by
                    // a new leader's Prepare), play safe as not committed
                    Some((slot, Self::last_write_to_key(&inst.reqs, key).cloned()))
                } else {
                    // instance not bound to be committed on me yet
                    Some((slot, None))
                }
            }
        } else {
            // never seen this key
            None
        }
    }

    /// Like `inspect_highest_slot()`, but a key whose writes I know of have
    /// all been compacted into the snapshot (or recovered from it, thus never
    /// seen) gets its executed value, as of the slot right before my log.
    pub(super) async fn inspect_latest_write(
        &mut self,
        cmd_id: CommandId,
        key: &String,
    ) -> Result<Option<(usize, Option<String>)>, SummersetError> {
        if self.start_slot == 0
            || self
                .highest_slot
                .get(key)
                .is_some_and(|&slot| slot >= self.start_slot)
        {
            return Ok(self.inspect_highest_slot(key));
        }
        let (old_results, cmd_result) = self
            .state_machine
            .do_sync_cmd(cmd_id, Command::Get { key: key.clone() }, None)
            .await?;
        for (old_id, old_result) in old_results {
            self.handle_cmd_result(old_id, old_result).await?;
        }
        let CommandResult::Get { value } = cmd_result else {
            return logged_err!("unexpected command result type");
        };
        Ok(value.map(|value| (self.start_slot - 1, Some(value))))
    }

    /// Read only req on key: I am not leader nor responder in a stable config.
    pub(super) fn read_on_improper_node(
        &mut self,
        client: ClientId,
        req_id: RequestId,
        key: String,
    ) -> Result<(), SummersetError> {
        self.external_api.send_reply(
            ApiReply::rq_retry(req_id, Command::Get { key }, self.bodega_conf.leader),
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
        if self.responders_may_outrun(&key) {
            return self.read_on_good_responder(client, req_id, key).await;
        }

        // has to use the `do_sync_cmd()` API
        let (old_results, cmd_result) = self
            .state_machine
            .do_sync_cmd(
                Self::make_ro_command_id(client, req_id),
                Command::Get { key },
                None,
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

    /// Read only req on key: I am a responder in a stable config, or its
    /// leader acting as one.
    pub(super) async fn read_on_good_responder(
        &mut self,
        client: ClientId,
        req_id: RequestId,
        key: String,
    ) -> Result<(), SummersetError> {
        let api_reply = match self
            .inspect_latest_write(Self::make_ro_command_id(client, req_id), &key)
            .await?
        {
            None => {
                // key not seen at all
                Some((
                    ApiReply::normal(req_id, Some(CommandResult::Get { value: None })),
                    false,
                ))
            }
            Some((slot, None)) => {
                // highest slot not committed
                if let Some(inst) = slot
                    .checked_sub(self.start_slot)
                    .and_then(|idx| self.insts.get_mut(idx))
                    && inst.status == Status::Accepting
                    && (inst.replica_bk.is_some() || self.bodega_conf.is_leader(self.id))
                {
                    // add to this instance slot's holding queue; will be
                    // released when enough AcceptNotices are received or it
                    // commits, whichever is earlier
                    inst.holding_reads.push((
                        client,
                        ApiRequest::Req {
                            id: req_id,
                            cmd: Command::Get { key },
                        },
                    ));
                    None
                } else {
                    // not even in Accepting status, very rare case; just reject
                    Some((
                        ApiReply::rq_retry(req_id, Command::Get { key }, self.bodega_conf.leader),
                        true,
                    ))
                }
            }
            Some((_, Some(value))) => {
                // highest slot committed, can directly reply
                Some((
                    ApiReply::normal(req_id, Some(CommandResult::Get { value: Some(value) })),
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

    /// Replies to the read reqs held on an instance: with the values it writes
    /// to their keys where those have become readable, or with rejections if
    /// it has fallen out of Accepting status (so they get retried promptly);
    /// the rest stay held.
    pub(super) fn release_held_read_reqs(&mut self, slot: usize) -> Result<(), SummersetError> {
        let idx = slot - self.start_slot;
        let mut still_held = vec![];
        for (client, req) in mem::take(&mut self.insts[idx].holding_reads) {
            let ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { key },
            } = &req
            else {
                continue;
            };

            let inst = &self.insts[idx];
            let value = if self.is_locally_readable(inst, key) {
                // although the write should be found, playing safe...
                Self::last_write_to_key(&inst.reqs, key)
            } else if inst.status == Status::Accepting {
                still_held.push((client, req));
                continue;
            } else {
                None
            };

            let (api_reply, tag) = if let Some(value) = value {
                let result = CommandResult::Get {
                    value: Some(value.clone()),
                };
                (ApiReply::normal(*req_id, Some(result)), "hgood")
            } else {
                let cmd = Command::Get { key: key.clone() };
                let leader = self.bodega_conf.leader;
                (ApiReply::rq_retry(*req_id, cmd, leader), "hfail")
            };
            self.external_api.send_reply(api_reply, client)?;
            pf_trace!("replied -> client {} read-only {}", client, tag);
            // [for access cnt stats only]
            if self.config.record_node_cnts {
                *self.node_cnts_stats.get_mut(&self.id).unwrap() += 1;
            }
        }
        self.insts[idx].holding_reads = still_held;

        Ok(())
    }
}
