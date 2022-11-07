mod statemach;
mod replicator;
mod smr;

use statemach::{Command, CommandResult, StateMachine};
use replicator::Replicator;
pub use smr::SMRProtocol;

/// Custom error type for various run-time errors.
#[derive(Debug)]
pub enum SummersetError {
    EmptyKey,
    WrongCommandType,
}

/// Server node struct, consisting of a replicator module and a state machine.
/// The replicator module type is specified at new(). The state machine is
/// currently a simple in-memory key-value HashMap. Anything required to be
/// persistently durable should be made so inside the replicator module. The
/// state machine HashMap is volatile.
#[derive(Debug)]
pub struct SummersetNode {
    replicator: Box<dyn Replicator>,
    kvlocal: StateMachine,
}

impl SummersetNode {
    /// Creates a new SummersetNode running given replication protocol type.
    pub fn new(protocol: SMRProtocol) -> Self {
        SummersetNode {
            replicator: protocol.new_replicator(),
            kvlocal: StateMachine::new(),
        }
    }

    /// Handle client Get request.
    pub fn handle_get(
        &self,
        key: &str,
    ) -> Result<Option<String>, SummersetError> {
        // key must not be empty
        if key.is_empty() {
            return Err(SummersetError::EmptyKey);
        }
        let cmd = Command::Get { key: key.into() };

        // invoke replicator, which runs the replication protocol and applies
        // execution at its chosen execution point
        match self.replicator.replicate(cmd, &self.kvlocal) {
            CommandResult::GetResult { value } => Ok(value),
            _ => Err(SummersetError::WrongCommandType),
        }
    }

    /// Handle client Put request.
    pub fn handle_put(
        &self,
        key: &str,
        value: &str,
    ) -> Result<Option<String>, SummersetError> {
        // key must not be empty
        if key.is_empty() {
            return Err(SummersetError::EmptyKey);
        }
        let cmd = Command::Put {
            key: key.into(),
            value: value.into(),
        };

        // invoke replicator, which runs the replication protocol and applies
        // execution at its chosen execution point
        match self.replicator.replicate(cmd, &self.kvlocal) {
            CommandResult::PutResult { old_value } => Ok(old_value),
            _ => Err(SummersetError::WrongCommandType),
        }
    }
}

impl Default for SummersetNode {
    fn default() -> Self {
        // default constructor is of protocol type DoNothing
        Self::new(SMRProtocol::DoNothing)
    }
}
