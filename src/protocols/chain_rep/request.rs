//! ChainRep -- client request entrance.

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::server::{ApiRequest, ApiReply, LogAction, Command, CommandResult};

// ChainRepReplica client requests entrance
impl ChainRepReplica {
    /// Handler of client request batch chan recv.
    pub fn handle_req_batch(
        &mut self,
        mut req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!(self.id; "got request batch of size {}", batch_size);

        unimplemented!()
    }
}
