//! Summerset generic client trait to be implemented by all protocol-specific
//! client stub structs.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::ReplicaId;
use crate::client::{ClientSendStub, ClientRecvStub};

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
    /// logic, returning two owned TCP connection halves (for possibly
    /// open-loop clients).
    async fn connect(
        &mut self,
    ) -> Result<(ClientSendStub, ClientRecvStub), SummersetError>;
}
