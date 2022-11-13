//! Replication protocol: do nothing.

use crate::smr_client::ClientRequestSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{ReplicatorServerNode, ReplicatorClientStub};
use crate::utils::{SummersetError, InitError};

use rand::seq::SliceRandom;

/// DoNothing replication protocol server module. Immediately executes given
/// command on state machine without doing anything else.
#[derive(Debug, Default)]
pub struct DoNothingServerNode {}

impl DoNothingServerNode {
    /// Create a new DoNothing protocol server module.
    #[allow(clippy::ptr_arg)]
    pub fn new(_peers: &Vec<String>) -> Result<Self, InitError> {
        Ok(DoNothingServerNode {})
    }
}

impl ReplicatorServerNode for DoNothingServerNode {
    /// Do nothing and immediately execute the command on state machine.
    fn replicate(
        &self,
        cmd: Command,
        sm: &StateMachine,
    ) -> Result<CommandResult, SummersetError> {
        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        Ok(sm.execute(&cmd))
    }
}

/// DoNothing replication protocol client stub.
#[derive(Debug, Default)]
pub struct DoNothingClientStub {
    /// List of server nodes addresses.
    servers: Vec<String>,
}

impl DoNothingClientStub {
    /// Create a new DoNothing protocol client stub.
    pub fn new(servers: &Vec<String>) -> Result<Self, InitError> {
        if servers.is_empty() {
            Err(InitError("servers list is empty".into()))
        } else {
            Ok(DoNothingClientStub {
                servers: servers.clone(),
            })
        }
    }
}

impl ReplicatorClientStub for DoNothingClientStub {
    /// Complete the given command by sending it to the currently connected
    /// server and trust the result unconditionally. If haven't established
    /// any connection, randomly pick a server and connect to it now.
    fn complete(
        &self,
        cmd: Command,
        sender: &mut ClientRequestSender,
    ) -> Result<CommandResult, SummersetError> {
        if sender.connections.is_empty() {
            sender.connect(
                self.servers.choose(&mut rand::thread_rng()).unwrap(),
            )?;
        }
        sender.issue(0, cmd)
    }
}
