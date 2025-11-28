//! `RepNothing` -- client request entrance.

use super::*;
use crate::client::ClientId;
use crate::server::{ApiRequest, LogAction, LogActionId};
use crate::utils::SummersetError;

// RepNothingReplica client requests entrance
impl RepNothingReplica {
    /// Handler of client request batch chan recv.
    pub(super) async fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);

        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            execed: vec![false; batch_size],
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst);

        // submit log action to make this instance durable
        let wal_entry = WalEntry { reqs: req_batch };
        self.storage_hub.submit_action(
            inst_idx as LogActionId,
            LogAction::Append {
                entry: wal_entry,
                sync: self.config.logger_sync,
            },
        )?;

        Ok(())
    }
}
