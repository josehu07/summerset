//! Summerset replication protocol traits, to be implemented by all replicator
//! module implementations.

use std::marker::Sized;
use std::fmt;
use std::sync::Arc;

use crate::smr_server::SummersetServerNode;
use crate::smr_client::SummersetClientStub;
use crate::statemach::{Command, CommandResult};
use crate::utils::{SummersetError, InitError};

use tonic::transport;

/// Interface that every replicator server module variant must provide.
#[tonic::async_trait]
pub trait ReplicatorServerNode: fmt::Debug + Send + Sync {
    /// Create a new server replicator module.
    fn new(peers: Vec<String>) -> Result<Self, InitError>
    where
        Self: Sized;

    /// Establish connections to peers. This should be called after the
    /// internal communication services between servers are spawned.
    ///
    /// This takes an immutable reference to `self` because the replicator
    /// module is supposed to be shared across multiple server threads. Any
    /// concurrency issues should be resolved inside the implementation, e.g.,
    /// using `Mutex`. Similar semantics apply to the referenced
    /// `SummersetServerNode`, which should be the struct containing `self`.
    async fn connect_peers(
        &self,
        node: &SummersetServerNode,
    ) -> Result<(), InitError>;

    /// Submit a command to the replication protocol module to run whatever
    /// work is required by the protocol. Upon the execution point, executes
    /// the command on the state machine.
    ///
    /// This takes an immutable reference to `self` because the replicator
    /// module is supposed to be shared across multiple server threads. Any
    /// concurrency issues should be resolved inside the implementation, e.g.,
    /// using `Mutex`. Similar semantics apply to the referenced
    /// `SummersetServerNode`, which should be the struct containing `self`.
    async fn replicate(
        &self,
        cmd: Command,
        node: &SummersetServerNode,
    ) -> Result<CommandResult, SummersetError>;
}

/// Interface that every protocol-specific internal communication service struct
/// should provide.
pub trait ReplicatorCommService: fmt::Debug {
    /// Create a new internal communication service struct.
    fn new(node: Arc<SummersetServerNode>) -> Result<Self, InitError>
    where
        Self: Sized;

    /// Convert the holder struct into a tonic service that owns the struct,
    /// and build a new tonic Router for the service. Returns `None` if the
    /// protocol in use does not have internal communication protos.
    ///
    /// Using `self: Box<Self>` trick here so that it is callable on trait
    /// objects, i.e., `Box<dyn ReplicatorCommServcie>`s. Concrete impls will
    /// move the actual `self` out of the box if needed.
    fn build_tonic_router(self: Box<Self>)
        -> Option<transport::server::Router>;
}

/// Interface that every replicator client stub variant must provide.
#[tonic::async_trait]
pub trait ReplicatorClientStub: fmt::Debug + Send + Sync {
    /// Create a new client replicator stub.
    fn new(servers: Vec<String>) -> Result<Self, InitError>
    where
        Self: Sized;

    /// Establish connection(s) to server(s).
    ///
    /// This takes an immutable reference to `self` because the replicator
    /// stub is allowed to be shared across multiple client threads. Any
    /// concurrency issues should be resolved inside the implementation, e.g.,
    /// using `Mutex`. Similar semantics apply to the referenced
    /// `SummersetClientStub`, which should be the struct containing `self`.
    async fn connect_servers(
        &self,
        stub: &SummersetClientStub,
    ) -> Result<(), InitError>;

    /// Complete a command by sending it to server(s) and wait until its
    /// acknowledgement. Depending on the protocol, this may require multiple
    /// back-and-forth communication rounds, e.g., to find the current leader.
    ///
    /// This takes an immutable reference to `self` because the replicator
    /// stub is allowed to be shared across multiple client threads. Any
    /// concurrency issues should be resolved inside the implementation, e.g.,
    /// using `Mutex`. Similar semantics apply to the referenced
    /// `SummersetClientStub`, which should be the struct containing `self`.
    async fn complete(
        &self,
        cmd: Command,
        stub: &SummersetClientStub,
    ) -> Result<CommandResult, SummersetError>;
}
