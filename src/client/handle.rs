//! Summerset generic client trait to be implemented by all protocol-specific
//! client stub structs.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, ApiRequest, ApiReply};

use async_trait::async_trait;

/// Client stub ID type.
pub type ClientId = u64;

/// Client trait to be implement by all protocol-specific client structs.
#[async_trait]
pub trait GenericClient {
    /// Creates a new client stub.
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>, // protocol-specific config in TOML format
    ) -> Result<Self, SummersetError>
    where
        Self: Sized;

    /// Establishes connection to the service according to protocol-specific
    /// logic.
    async fn setup(&mut self) -> Result<(), SummersetError>;

    /// Sends a request to the service according to protocol-specific logic.
    async fn send_req(&mut self, req: ApiRequest)
        -> Result<(), SummersetError>;

    /// Receives a reply from the service according to protocol-specific logic.
    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError>;
}
