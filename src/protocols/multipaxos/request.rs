//! MultiPaxos -- client request entrance.

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::server::{ApiRequest, ApiReply, LogAction, Command, CommandResult};

// MultiPaxosReplica client requests entrance
impl MultiPaxosReplica {
    /// Handler of client request batch chan recv.
    pub fn handle_req_batch(
        &mut self,
        mut req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!(self.id; "got request batch of size {}", batch_size);

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

        // if simulating read leases, extract all the reads and immediately
        // reply to them with a dummy value
        // TODO: only for benchmarking purposes
        if self.config.sim_read_lease {
            for (client, req) in &req_batch {
                if let ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { .. },
                } = req
                {
                    self.external_api.send_reply(
                        ApiReply::Reply {
                            id: *req_id,
                            result: Some(CommandResult::Get { value: None }),
                            redirect: None,
                        },
                        *client,
                    )?;
                    pf_trace!(self.id; "replied -> client {} for read-only cmd", client);
                }
            }

            req_batch.retain(|(_, req)| {
                !matches!(
                    req,
                    ApiRequest::Req {
                        cmd: Command::Get { .. },
                        ..
                    }
                )
            });
            if req_batch.is_empty() {
                return Ok(());
            }
        }

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist); fill it up with incoming data
        let slot = self.first_null_slot();
        {
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert_eq!(inst.status, Status::Null);
            inst.reqs = req_batch.clone();
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
        pf_debug!(self.id; "enter Accept phase for slot {} bal {}",
                           slot, inst.bal);

        // [for perf breakdown]
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
        pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                           slot, inst.bal);

        // send Accept messages to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Accept {
                slot,
                ballot: inst.bal,
                reqs: req_batch,
            },
            None,
        )?;
        pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                           slot, inst.bal);

        Ok(())
    }

    /// [for stale read profiling]
    pub fn val_ver_of_first_key(
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
