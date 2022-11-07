use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::Replicator;

/// DoNothing replication protocol. Immediately executes given command on
/// state machine without doing anything else.
#[derive(Debug, Default)]
pub struct DoNothing {}

impl DoNothing {
    /// Create a new DoNothing protocol replicator.
    pub fn new() -> Self {
        DoNothing {}
    }
}

impl Replicator for DoNothing {
    fn replicate(&self, cmd: Command, sm: &StateMachine) -> CommandResult {
        // do nothing and simply execute the command on state machine
        // immediately
        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        sm.execute(&cmd)
    }
}
