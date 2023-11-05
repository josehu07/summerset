//! Crossword -- client request entrance.

use std::collections::HashMap;

use super::*;

use crate::utils::{SummersetError, Bitmap, RSCodeword};
use crate::server::{ApiRequest, ApiReply, LogAction};

// CrosswordReplica client requests entrance
impl CrosswordReplica {
    /// Handler of client request batch chan recv.
    pub fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!(self.id; "got request batch of size {}", batch_size);

        // if I'm not a leader, ignore client requests
        if !self.is_leader() {
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

        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw = RSCodeword::from_data(
            req_batch,
            self.rs_data_shards,
            self.rs_total_shards - self.rs_data_shards,
        )?;
        reqs_cw.compute_parity(Some(&self.rs_coder))?;

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist); fill it up with incoming data
        let slot = self.first_null_slot()?;
        {
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert_eq!(inst.status, Status::Null);
            inst.reqs_cw = reqs_cw;
            inst.leader_bk = Some(LeaderBookkeeping {
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                accept_acks: HashMap::new(),
            });
            inst.external = true;
        }

        // decide whether we can enter fast path for this instance
        if self.bal_prepared == 0 {
            // slow case: Prepare phase not done yet. Initiate a Prepare round
            // if none is on the fly, or just wait for some Prepare reply to
            // trigger my Accept phase
            if self.bal_prep_sent == 0 {
                self.bal_prep_sent =
                    self.make_greater_ballot(self.bal_max_seen);
                self.bal_max_seen = self.bal_prep_sent;
            }

            let inst = &mut self.insts[slot - self.start_slot];
            inst.bal = self.bal_prep_sent;
            inst.status = Status::Preparing;
            pf_debug!(self.id; "enter Prepare phase for slot {} bal {}",
                               slot, inst.bal);

            // record update to largest prepare ballot
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Preparing),
                LogAction::Append {
                    entry: WalEntry::PrepareBal {
                        slot,
                        ballot: self.bal_prep_sent,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                               slot, inst.bal);

            // send Prepare messages to all peers
            self.transport_hub.bcast_msg(
                PeerMsg::Prepare {
                    slot,
                    ballot: self.bal_prep_sent,
                },
                None,
            )?;
            pf_trace!(self.id; "broadcast Prepare messages for slot {} bal {}",
                               slot, inst.bal);
        } else {
            // normal case: Prepare phase covered, only do the Accept phase
            let inst = &mut self.insts[slot - self.start_slot];
            inst.bal = self.bal_prepared;
            inst.status = Status::Accepting;
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
                &self.peer_alive,
            );
            pf_debug!(self.id; "enter Accept phase for slot {} bal {} asgmt {}",
                               slot, inst.bal, Self::assignment_to_string(assignment));

            // record update to largest accepted ballot and corresponding data
            let subset_copy = inst
                .reqs_cw
                .subset_copy(&assignment[self.id as usize], false)?;
            inst.assignment = assignment.clone();
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
            pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                               slot, inst.bal);

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
                if self.peer_alive.get(peer)? {
                    self.pending_accepts
                        .get_mut(&peer)
                        .unwrap()
                        .push_back((now_us, slot));
                }
            }
            pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                               slot, inst.bal);
        }

        Ok(())
    }
}
