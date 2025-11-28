//! `SimplePush` -- client request entrance.

use super::*;
use crate::client::ClientId;
use crate::server::{ApiRequest, LogAction, LogActionId};
use crate::utils::{Bitmap, SummersetError};

// SimplePushReplica client requests entrance
impl SimplePushReplica {
    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);

        // target peers to push to
        let mut target = Bitmap::new(self.population, false);
        let mut peer_cnt = 0;
        for peer in 0..self.population {
            if peer_cnt == self.config.rep_degree {
                break;
            }
            if peer == self.id {
                continue;
            }
            target.set(peer, true)?;
            peer_cnt += 1;
        }

        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            pending_peers: target.clone(),
            execed: vec![false; batch_size],
            from_peer: None,
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst);

        // submit log action to make this instance durable
        let wal_entry = WalEntry::FromClient {
            reqs: req_batch.clone(),
        };
        self.storage_hub.submit_action(
            inst_idx as LogActionId,
            LogAction::Append {
                entry: wal_entry,
                sync: true,
            },
        )?;

        // send push message to chosen peers
        self.transport_hub.bcast_msg(
            PushMsg::Push {
                src_inst_idx: inst_idx,
                reqs: req_batch,
            },
            Some(target),
        )?;

        Ok(())
    }
}
