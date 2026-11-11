//! `Crossword` -- client request entrance.

use std::collections::HashMap;

use super::*;
use crate::server::{ApiReply, ApiRequest, Command, CommandResult};
use crate::utils::{Bitmap, SummersetError};

// CrosswordReplica client requests entrance
impl CrosswordReplica {
    /// Handler of client request batch chan recv.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn handle_req_batch(
        &mut self,
        mut req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!("got request batch of size {}", batch_size);

        // if I'm not a leader, ignore client requests
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
                    self.external_api
                        .send_reply(ApiReply::redirect(req_id, Some(target)), client)?;
                    pf_trace!("redirected client {} to replica {}", client, target);
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
                        ApiReply::normal(*req_id, Some(CommandResult::Get { value: None })),
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

        // [for perf breakdown only]
        let slot = self.first_null_slot()?;
        if self.bal_prepared > 0
            && let Some(sw) = self.bd_stopwatch.as_mut()
        {
            sw.record_now(slot, 0, None)?;
        }

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist), and start its Accept phase with the
        // new value
        {
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert_eq!(inst.status, Status::Null);
            inst.leader_bk = Some(LeaderBookkeeping {
                trigger_slot: 0,
                endprep_slot: 0,
                prepare_acks: Bitmap::new(self.population, false),
                prepare_values: HashMap::new(),
                accept_acks: HashMap::new(),
            });
            inst.external = true;
            inst.bal = self.bal_prepared;
        }

        // [for perf breakdown only]
        if let Some(sw) = self.bd_stopwatch.as_mut() {
            sw.record_now(slot, 1, None)?;
        }

        self.propose_new_value(slot, req_batch)
    }

    /// [for stale read profiling]
    pub(super) fn val_ver_of_first_key(
        &mut self,
    ) -> Result<Option<(String, usize)>, SummersetError> {
        let (mut key, mut ver) = (None, 0);
        for inst in &mut self.insts {
            if inst.status >= Status::Committed
                && inst.reqs_cw.avail_shards() >= inst.reqs_cw.num_data_shards()
            {
                if inst.reqs_cw.avail_data_shards() < inst.reqs_cw.num_data_shards() {
                    inst.reqs_cw
                        .reconstruct_data(Self::coder_of(&mut self.rs_coders, &inst.reqs_cw)?)?;
                }

                for (_, req) in inst.reqs_cw.get_data()? {
                    if let ApiRequest::Req {
                        cmd: Command::Put { key: k, .. },
                        ..
                    } = req
                    {
                        if let Some(kk) = &key
                            && kk == k
                        {
                            ver += 1;
                        } else {
                            key = Some(k.into());
                        }
                    }
                }
            }
        }

        Ok(key.map(|k| (k, ver)))
    }
}
