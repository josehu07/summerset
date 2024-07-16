//! Summerset generic client traits to be implemented by all protocol-specific
//! client stub structs.

use std::net::SocketAddr;

use crate::client::ClientCtrlStub;
use crate::server::{ApiReply, ApiRequest};
use crate::utils::SummersetError;

use async_trait::async_trait;

/// Client stub ID type.
pub type ClientId = u64;

/// Client trait to be implement by all protocol-specific client structs.
#[async_trait]
pub trait GenericEndpoint {
    /// Creates a new client stub and sets up required functionality modules
    /// according to protocol-specific logic.
    async fn new_and_setup(
        ctrl_bind: SocketAddr, // bind address for the socket connecting to manager
        api_bind_base: SocketAddr, // base bind address for sockets connecting to servers
        manager: SocketAddr,       // remote address of manager oracle
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError>
    where
        Self: Sized;

    /// Establishes connection to the service (or re-joins the service)
    /// according to protocol-specific logic.
    async fn connect(&mut self) -> Result<(), SummersetError>;

    /// Leaves the service: forgets about the current TCP connections and send
    /// leave notifications according to protocol-specific logic. If `permanent`
    /// is true, the connection to cluster manager oracle is also dropped.
    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError>;

    /// Sends a request to the service according to protocol-specific logic.
    fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError>;

    /// Receives a reply from the service according to protocol-specific logic.
    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError>;

    /// Gets my client ID.
    fn id(&self) -> ClientId;

    /// Gets a mutable reference to the control stub for sending control
    /// requests and receiving control replies for testing purposes.
    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub;
}
