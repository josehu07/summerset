//! MultiPaxos -- client request entrance.

use super::*;

use crate::server::{ApiReply, ApiRequest, Command, LogAction};
use crate::utils::{Bitmap, SummersetError};

// MultiPaxosReplica client requests entrance
impl MultiPaxosReplica {
    /// Treat read requests in the batch specially if:
    ///   - I'm the majority-leased stable leader
    ///   - simulating read leases
    ///   - near quorum read optimization is on
    ///
    /// `req_batch` will be updated to retain commands that are decided should
    /// go through normal consensus.
    async fn treat_read_only_reqs(
        &mut self,
        req_batch: &mut ReqBatch,
    ) -> Result<(), SummersetError> {
        let mut strip_read_only = false;

        if self.is_stable_leader() {
            // conditions of majority-leased stable leader met, can reply
            // read-only commands directly back to clients
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
                    pf_trace!("replied -> client {} for read-only cmd", client);

                    strip_read_only = true;
                }
            }
        } else if (!self.is_leader() || self.bal_prepared == 0)
            && self.config.enable_quorum_reads
        {
            // if near quorum read optimization is on, broadcast ReadQueries
            // for reads. Reference:
            //   https://www.usenix.org/system/files/hotstorage19-paper-charapko.pdf
            let mut rq_id = (0, 0);
            let mut rq_bk = ReadQueryBookkeeping {
                reads: vec![],
                rq_acks: Bitmap::new(self.population, false),
                max_replies: vec![],
            };

            for (client, req) in req_batch.iter() {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                } = req
                {
                    rq_bk.reads.push((*client, req.clone()));
                    rq_bk.max_replies.push(self.inspect_highest_slot(key)?);

                    if !strip_read_only {
                        rq_id = (*client, *req_id);
                        strip_read_only = true;
                    }
                }
            }

            if strip_read_only {
                // broadcast ReadQuery to peers
                self.transport_hub.bcast_msg(
                    PeerMsg::ReadQuery {
                        reads: rq_bk.reads.clone(),
                    },
                    None,
                )?;
                pf_debug!(
                    "broadcast ReadQuery messages for rq_id {}.{}",
                    rq_id.0,
                    rq_id.1
                );

                // mark myself as replied
                rq_bk.rq_acks.set(self.id, true)?;
                self.quorum_reads.insert(rq_id, rq_bk);
            }
        }

        if strip_read_only {
            req_batch.retain(|(_, req)| !req.read_only());
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

        // if I'm a majority-leased leader or if simulating read leases, extract
        // all the reads and immediately reply to them
        self.treat_read_only_reqs(&mut req_batch).await?;
        if req_batch.is_empty() {
            return Ok(());
        }

        // if I'm not a prepared leader, ignore client requests
        if !self.is_leader() || self.bal_prepared == 0 {
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

        // [for perf breakdown only]
        if let Some(sw) = self.bd_stopwatch.as_mut() {
            sw.record_now(slot, 0, None)?;
        }

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

    /// [for stale read profiling]
    pub(super) fn val_ver_of_first_key(
        &mut self,
    ) -> Result<Option<(String, usize)>, SummersetError> {
        let (mut key, mut ver) = (None, 0);
        for inst in self.insts.iter_mut() {
            if inst.status >= Status::Committed {
                for (_, req) in &inst.reqs {
                    if let ApiRequest::Req {
                        cmd: Command::Put { key: k, .. },
                        ..
                    } = req
                    {
                        if key.is_none() {
                            key = Some(k.into());
                        } else if key.as_ref().unwrap() == k {
                            ver += 1;
                        }
                    }
                }
            }
        }

        Ok(key.map(|k| (k, ver)))
    }
}
