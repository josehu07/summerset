//! EPaxos -- client request entrance.

use super::*;

use crate::server::{ApiReply, ApiRequest, LogAction};
use crate::utils::{Bitmap, SummersetError};

// EPaxosReplica client requests entrance
impl EPaxosReplica {
    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!("got request batch of size {}", batch_size);

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
