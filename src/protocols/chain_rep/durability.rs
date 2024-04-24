//! ChainRep -- durable logging.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, LogResult, LogActionId};

// ChainRepReplica durable WAL logging
impl ChainRepReplica {
    /// Handler of durable logging result chan recv.
    pub fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<WalEntry>,
    ) -> Result<(), SummersetError> {
        unimplemented!()
    }
}
