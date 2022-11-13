//! Summerset replication protocol traits, to be implemented by all replicator
//! module implementations.

use crate::smr_client::ClientRequestSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::utils::SummersetError;

/// Interface that every replicator server module variant must provide.
pub trait ReplicatorServerNode: std::fmt::Debug + Send + Sync {
    /// Submit a command to the replication protocol module to run whatever
    /// work is required by the protocol. Upon the execution point, executes
    /// the command on the state machine.
    fn replicate(
        &self,
        cmd: Command,
        sm: &StateMachine,
    ) -> Result<CommandResult, SummersetError>;
}

/// Interface that every replicator client stub variant must provide.
pub trait ReplicatorClientStub: std::fmt::Debug + Send + Sync {
    /// Complete a command by sending it to server(s) and wait until its
    /// acknowledgement. Depending on the protocol, this may require multiple
    /// back-and-forth communication rounds, e.g., to find the current leader.
    fn complete(
        &self,
        cmd: Command,
        sender: &mut ClientRequestSender,
    ) -> Result<CommandResult, SummersetError>;
}
