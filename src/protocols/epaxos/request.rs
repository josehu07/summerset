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

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist); fill it up with incoming data
        let slot = self.first_null_slot(self.id);
        let (row, col) = slot.unpack();
        {
            // compute the dependencies set of this request batch and proper
            // sequence number
            let ballot = self.my_default_ballot();
            let deps = self.identify_deps(&req_batch);
            let seq = 1 + self.max_seq_num(&deps);

            let inst = &mut self.insts[row][col - self.start_col];
            debug_assert_eq!(inst.status, Status::Null);
            inst.bal = ballot;
            inst.seq = seq;
            inst.deps = deps;
            inst.reqs.clone_from(&req_batch);
            Self::refresh_highest_cols(
                slot,
                &req_batch,
                self.population,
                &mut self.highest_cols,
            );
            inst.leader_bk = Some(LeaderBookkeeping {
                pre_accept_acks: Bitmap::new(self.population, false),
                pre_accept_replies: vec![],
                accept_acks: Bitmap::new(self.population, false),
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                prepare_voteds: vec![],
            });
            inst.external = true;
        }

        // start the fast-path PreAccept phase for this instance
        let inst = &mut self.insts[row][col - self.start_col];
        inst.status = Status::PreAccepting;
        pf_debug!("enter PreAccept phase for slot {} bal {}", slot, inst.bal);

        // record update to largest accepted ballot and corresponding data
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, Status::PreAccepting),
            LogAction::Append {
                entry: WalEntry::PreAcceptSlot {
                    slot,
                    ballot: inst.bal,
                    seq: inst.seq,
                    deps: inst.deps.clone(),
                    reqs: req_batch.clone(),
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(
            "submitted PreAcceptSlot log action for slot {} bal {} seq {} deps {}",
            slot,
            inst.bal,
            inst.seq,
            inst.deps,
        );

        // send Accept messages to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::PreAccept {
                slot,
                ballot: inst.bal,
                seq: inst.seq,
                deps: inst.deps.clone(),
                reqs: req_batch,
            },
            None,
        )?;
        pf_trace!(
            "broadcast PreAccept messages for slot {} bal {} seq {} deps {}",
            slot,
            inst.bal,
            inst.seq,
            inst.deps,
        );

        Ok(())
    }
}
