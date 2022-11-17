//! Summerset replication protocol traits, to be implemented by all replicator
//! module implementations.

use crate::smr_server::ServerRpcSender;
use crate::smr_client::ClientRpcSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::utils::{SummersetError, InitError};

use std::sync::Mutex;

/// Interface that every replicator server module variant must provide.
pub trait ReplicatorServerNode: std::fmt::Debug + Send + Sync {
    /// Create a new server replicator module.
    #[allow(clippy::ptr_arg)]
    fn new(
        peers: &Vec<String>,
        sender: &mut ServerRpcSender,
    ) -> Result<Self, InitError>
    where
        Self: std::marker::Sized;

    /// Submit a command to the replication protocol module to run whatever
    /// work is required by the protocol. Upon the execution point, executes
    /// the command on the state machine.
    ///
    /// This takes an immutable reference to `self` because the replicator
    /// module is supposed to be shared across multiple server threads. Any
    /// concurrency issues should be resolved inside the implementation, e.g.,
    /// using `Mutex`.
    fn replicate(
        &self,
        cmd: Command,
        sender: &Mutex<ServerRpcSender>,
        sm: &StateMachine,
    ) -> Result<CommandResult, SummersetError>;
}

/// Interface that every replicator client stub variant must provide.
pub trait ReplicatorClientStub: std::fmt::Debug + Send + Sync {
    /// Create a new client replicator stub.
    #[allow(clippy::ptr_arg)]
    fn new(
        servers: &Vec<String>,
        sender: &mut ClientRpcSender,
    ) -> Result<Self, InitError>
    where
        Self: std::marker::Sized;

    /// Complete a command by sending it to server(s) and wait until its
    /// acknowledgement. Depending on the protocol, this may require multiple
    /// back-and-forth communication rounds, e.g., to find the current leader.
    ///
    /// This takes a mutable reference to `self` because the replicator client
    /// stub is supposed to be used by only one client thread.
    fn complete(
        &mut self,
        cmd: Command,
        sender: &mut ClientRpcSender,
    ) -> Result<CommandResult, SummersetError>;
}
