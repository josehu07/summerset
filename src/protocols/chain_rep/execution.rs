//! ChainRep -- command execution.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, ApiReply};

// ChainRepReplica state machine execution
impl ChainRepReplica {
    /// Handler of state machine exec result chan recv.
    pub fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        unimplemented!()
    }
}
