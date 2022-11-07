use crate::statemach::{Command, CommandResult, StateMachine};

/// Interface that every replication protocol variant must provide.
pub trait Replicator: std::fmt::Debug + Send + Sync {
    /// Submit a command to the replication protocol module to run whatever
    /// work is required by the protocol. Upon the execution point, executes
    /// the command on the state machine.
    fn replicate(&self, cmd: Command, sm: &StateMachine) -> CommandResult;
}
