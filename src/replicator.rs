//! Summerset replication protocol traits, to be implemented by all replicator
//! module implementations.

use crate::smr_server::ServerRpcSender;
use crate::smr_client::ClientRpcSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::utils::{SummersetError, InitError};

use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::runtime::Runtime;

/// Interface that every replicator server module variant must provide.
pub trait ReplicatorServerNode: std::fmt::Debug + Send + Sync {
    /// Create a new server replicator module. The internal communication
    /// service between servers is also spawned here.
    fn new(
        peers: Vec<String>,
        smr_addr: SocketAddr,
        main_runtime: &Runtime, // for starting internal communication service
    ) -> Result<Self, InitError>
    where
        Self: std::marker::Sized;

    /// Establish connections to peers.
    fn connect_peers(
        &mut self,
        sender: &mut ServerRpcSender,
    ) -> Result<(), InitError>;

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
    fn new(servers: Vec<String>) -> Result<Self, InitError>
    where
        Self: std::marker::Sized;

    /// Establish connection(s) to server(s).
    fn connect_servers(
        &mut self,
        sender: &mut ClientRpcSender,
    ) -> Result<(), InitError>;

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
