//! MultiPaxos -- near quorum read optimization.

use super::*;

impl MultiPaxosReplica {
    /// Update the highest_slot tracking info given a new request batch about
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

    /// Get the value at the highest slot index ever seen for a key and its
    /// current commit status.
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
                if inst.status < Status::Committed {
                    // instance not committed on me yet
                    Ok(Some((slot, None)))
                } else {
                    // instance committed, return the latest value for the key
                    // in batch
                    for (_, req) in inst.reqs.iter().rev() {
                        if let ApiRequest::Req {
                            cmd: Command::Put { key: k, value },
                            ..
                        } = req
                        {
                            if k == key {
                                return Ok(Some((slot, Some(value.clone()))));
                            }
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

    /// Handler of ReadQuery message from a peer issuer.
    pub(super) async fn handle_msg_read_query(
        &mut self,
        peer: ReplicaId,
        reads: ReqBatch,
    ) -> Result<(), SummersetError> {
        debug_assert_ne!(reads.len(), 0);
        let rq_id = (
            reads[0].0,
            if let ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { .. },
            } = reads[0].1
            {
                req_id
            } else {
                return logged_err!("non-Get request in ReadQuery");
            },
        );
        pf_trace!(
            "received ReadQuery <- {} for rq_id {}.{}",
            peer,
            rq_id.0,
            rq_id.1
        );

        // check if I'm currently a stable majority-leased leader
        if self.is_stable_leader() {
            // yes, can directly reply with my latest committed value for each
            // key and shortcut the read quorum
            let mut replies = Vec::with_capacity(reads.len());
            for (client, req) in reads {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    // has to use the `do_sync_cmd()` API
                    let (old_results, cmd_result) = self
                        .state_machine
                        .do_sync_cmd(
                            Self::make_ro_command_id(client, req_id),
                            Command::Get { key: key.clone() },
                        )
                        .await?;
                    for (old_id, old_result) in old_results {
                        self.handle_cmd_result(old_id, old_result).await?;
                    }

                    if let CommandResult::Get { value } = cmd_result {
                        // dummy
                        replies.push(value.map(|v| {
                            (
                                0, // using 0 as dummy slot number
                                Some(v),
                            )
                        }));
                    } else {
                        return logged_err!("non-Get request in ReadQuery");
                    }
                } else {
                    return logged_err!("non-Get request in ReadQuery");
                }
            }

            self.transport_hub.send_msg(
                PeerMsg::ReadQueryReply {
                    rq_id,
                    replies,
                    from_leader: true,
                },
                peer,
            )?;
            pf_trace!(
                "sent ReadQueryReply -> {} for rq_id {}.{} leader",
                peer,
                rq_id.0,
                rq_id.1
            );
        } else {
            // no, reply with my latest knowledge about each key in batch
            let mut replies = Vec::with_capacity(reads.len());
            for (_, req) in reads {
                if let ApiRequest::Req {
                    cmd: Command::Get { key },
                    ..
                } = req
                {
                    replies.push(self.inspect_highest_slot(&key)?);
                } else {
                    return logged_err!("non-Get request in ReadQuery");
                }
            }

            self.transport_hub.send_msg(
                PeerMsg::ReadQueryReply {
                    rq_id,
                    replies,
                    from_leader: false,
                },
                peer,
            )?;
            pf_trace!(
                "sent ReadQueryReply -> {} for rq_id {}.{} normal",
                peer,
                rq_id.0,
                rq_id.1
            );
        }

        Ok(())
    }

    /// Handler of ReadQuery reply from a peer.
    pub(super) fn handle_msg_read_query_reply(
        &mut self,
        peer: ReplicaId,
        rq_id: (ClientId, RequestId),
        replies: Vec<Option<(usize, Option<String>)>>,
        from_leader: bool,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "received ReadQueryReply <- {} for rq_id {}.{}",
            peer,
            rq_id.0,
            rq_id.1
        );

        // if am a quorum read reply I'm still waiting on:
        let mut can_reply = false;
        if let Some(rq_bk) = self.quorum_reads.get_mut(&rq_id) {
            if from_leader {
                // if reply from stable leader, can directly use the results
                // and shortcut the read quorum
                rq_bk.max_replies = replies;
                can_reply = true;
            } else if !rq_bk.rq_acks.get(peer)? {
                // update my knowledge at the highest slot seen for each key
                debug_assert_eq!(replies.len(), rq_bk.reads.len());
                debug_assert_eq!(replies.len(), rq_bk.max_replies.len());
                for (reply, max_reply) in
                    replies.into_iter().zip(rq_bk.max_replies.iter_mut())
                {
                    match reply {
                        None => {}
                        Some((slot, None)) => match *max_reply {
                            None => {
                                *max_reply = Some((slot, None));
                            }
                            Some((max_slot, _)) => {
                                if slot > max_slot {
                                    *max_reply = Some((slot, None));
                                }
                            }
                        },
                        Some((slot, Some(value))) => match *max_reply {
                            None => {
                                *max_reply = Some((slot, None));
                            }
                            Some((max_slot, None)) => {
                                if slot >= max_slot {
                                    *max_reply = Some((slot, Some(value)));
                                }
                            }
                            Some((max_slot, Some(ref max_value))) => {
                                #[allow(clippy::comparison_chain)]
                                if slot > max_slot {
                                    *max_reply = Some((slot, Some(value)));
                                } else if slot == max_slot
                                    && max_value != &value
                                {
                                    return logged_err!(
                                        "conflicting committed value at slot {}",
                                        slot
                                    );
                                }
                            }
                        },
                    }
                }
                rq_bk.rq_acks.set(peer, true)?;

                // if majority number of replies reached, can decide to reply
                // to clients now
                if rq_bk.rq_acks.count() >= self.quorum_cnt {
                    pf_debug!(
                        "enough ReadQuery replies got for rq_id {}.{}",
                        rq_id.0,
                        rq_id.1
                    );
                    can_reply = true;
                }
            }
        }

        // if can safely reply to clients now:
        if can_reply {
            let rq_bk = self.quorum_reads.remove(&rq_id).unwrap();
            for ((client, req), reply) in
                rq_bk.reads.into_iter().zip(rq_bk.max_replies.into_iter())
            {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    let (api_reply, is_retry) = match reply {
                        None => {
                            // no one in quorum knows about this key, safely
                            // reply not found
                            (
                                ApiReply::normal(
                                    req_id,
                                    Some(CommandResult::Get { value: None }),
                                ),
                                false,
                            )
                        }
                        Some((_, None)) => {
                            // highest slot for this key not committed, should
                            // fallback to slow path
                            (
                                ApiReply::rq_retry(
                                    req_id,
                                    Command::Get { key },
                                ),
                                true,
                            )
                        }
                        Some((_, Some(value))) => {
                            // highest slot for this key committed, safely use
                            // its value
                            (
                                ApiReply::normal(
                                    req_id,
                                    Some(CommandResult::Get {
                                        value: Some(value),
                                    }),
                                ),
                                false,
                            )
                        }
                    };
                    self.external_api.send_reply(api_reply, client)?;
                    pf_trace!(
                        "replied -> client {} for near-read {}",
                        client,
                        if is_retry { "retry" } else { "rgood" }
                    );
                } else {
                    return logged_err!(
                        "non-Get request found in rq_bk reads field"
                    );
                }
            }
        }

        Ok(())
    }
}
