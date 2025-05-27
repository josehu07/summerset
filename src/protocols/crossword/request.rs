//! Crossword -- client request entrance.

use std::collections::HashMap;

use super::*;

use crate::server::{ApiReply, ApiRequest, Command, CommandResult, LogAction};
use crate::utils::{Bitmap, RSCodeword, SummersetError};

// CrosswordReplica client requests entrance
impl CrosswordReplica {
    /// Handler of client request batch chan recv.
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

        // [for perf breakdown only]
        let slot = self.first_null_slot()?;
        if self.bal_prepared > 0 {
            if let Some(sw) = self.bd_stopwatch.as_mut() {
                sw.record_now(slot, 0, None)?;
            }
        }

        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw = RSCodeword::from_data(
            req_batch,
            self.rs_data_shards,
            self.rs_total_shards - self.rs_data_shards,
        )?;
        reqs_cw.compute_parity(Some(&self.rs_coder))?;

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist); fill it up with incoming data
        {
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert_eq!(inst.status, Status::Null);
            inst.reqs_cw = reqs_cw;
            inst.leader_bk = Some(LeaderBookkeeping {
                trigger_slot: 0,
                endprep_slot: 0,
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                accept_acks: HashMap::new(),
            });
            inst.external = true;
        }

        // start the Accept phase for this instance
        let inst = &mut self.insts[slot - self.start_slot];
        inst.bal = self.bal_prepared;
        inst.status = Status::Accepting;

        // [for perf breakdown only]
        if let Some(sw) = self.bd_stopwatch.as_mut() {
            sw.record_now(slot, 1, None)?;
        }

        let assignment = Self::pick_assignment_policy(
            self.assignment_adaptive,
            self.assignment_balanced,
            &self.init_assignment,
            &self.brr_assignments,
            self.rs_data_shards,
            self.majority,
            self.config.fault_tolerance,
            inst.reqs_cw.data_len(),
            &self.linreg_model,
            self.config.b_to_d_threshold,
            &self.qdisc_info,
            self.heartbeater.peer_alive(),
        );
        pf_debug!(
            "enter Accept phase for slot {} bal {} asgmt {}",
            slot,
            inst.bal,
            Self::assignment_to_string(assignment)
        );

        // record update to largest accepted ballot and corresponding data
        let subset_copy = inst
            .reqs_cw
            .subset_copy(&assignment[self.id as usize], false)?;
        inst.assignment.clone_from(assignment);
        inst.voted = (inst.bal, subset_copy.clone());
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, Status::Accepting),
            LogAction::Append {
                entry: WalEntry::AcceptData {
                    slot,
                    ballot: inst.bal,
                    // persist only some shards on myself
                    reqs_cw: subset_copy,
                    assignment: assignment.clone(),
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(
            "submitted AcceptData log action for slot {} bal {}",
            slot,
            inst.bal
        );

        // send Accept messages to all peers, each getting its subset of
        // shards of data
        let now_us = self.startup_time.elapsed().as_micros();
        for peer in 0..self.population {
            if peer == self.id {
                continue;
            }
            self.transport_hub.send_msg(
                PeerMsg::Accept {
                    slot,
                    ballot: inst.bal,
                    reqs_cw: inst
                        .reqs_cw
                        .subset_copy(&assignment[peer as usize], false)?,
                    assignment: assignment.clone(),
                },
                peer,
            )?;
            if self.heartbeater.peer_alive().get(peer)? {
                self.pending_accepts
                    .get_mut(&peer)
                    .unwrap()
                    .push_back((now_us, slot));
            }
        }
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
            if inst.status >= Status::Committed
                && inst.reqs_cw.avail_shards() >= self.rs_data_shards
            {
                if inst.reqs_cw.avail_data_shards() < self.rs_data_shards {
                    inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }

                for (_, req) in inst.reqs_cw.get_data()? {
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
