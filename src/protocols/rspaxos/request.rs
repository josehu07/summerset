//! RS-Paxos -- client request entrance.

use super::*;

use crate::server::{ApiReply, ApiRequest, Command, CommandResult, LogAction};
use crate::utils::{Bitmap, RSCodeword, SummersetError};

// RSPaxosReplica client requests entrance
impl RSPaxosReplica {
    /// Handler of client request batch chan recv.
    pub(super) fn handle_req_batch(
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
                        ApiReply::Reply {
                            id: req_id,
                            result: None,
                            redirect: Some(target),
                        },
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
                    pf_trace!("replied -> client {} for read-only cmd", client);
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

        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw = RSCodeword::from_data(
            req_batch,
            self.majority,
            self.population - self.majority,
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
        let subset_copy = inst.reqs_cw.subset_copy(
            &Bitmap::from((self.population, vec![self.id])),
            false,
        )?;
        inst.voted = (inst.bal, subset_copy.clone());
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, Status::Accepting),
            LogAction::Append {
                entry: WalEntry::AcceptData {
                    slot,
                    ballot: inst.bal,
                    // persist only one shard on myself
                    reqs_cw: subset_copy,
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(
            "submitted AcceptData log action for slot {} bal {}",
            slot,
            inst.bal
        );

        // send Accept messages to all peers, each getting one shard of data
        for peer in 0..self.population {
            if peer == self.id {
                continue;
            }
            self.transport_hub.send_msg(
                PeerMsg::Accept {
                    slot,
                    ballot: inst.bal,
                    reqs_cw: inst.reqs_cw.subset_copy(
                        &Bitmap::from((self.population, vec![peer])),
                        false,
                    )?,
                },
                peer,
            )?;
        }
        pf_trace!(
            "broadcast Accept messages for slot {} bal {}",
            slot,
            inst.bal
        );

        Ok(())
    }
}
