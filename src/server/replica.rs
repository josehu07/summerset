//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::utils::SummersetError;

use async_trait::async_trait;

/// Server replica ID type.
pub type ReplicaId = u8;

/// Replica trait to be implement by all protocol-specific server structs.
#[async_trait]
pub trait GenericReplica {
    /// Creates a new replica module.
    fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        config_str: Option<&str>, // protocol-specific config in TOML format
    ) -> Result<Self, SummersetError>
    where
        Self: Sized;

    /// Sets up required functionality modules according to protocol-specific
    /// logic.
    async fn setup(
        &mut self,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
    ) -> Result<(), SummersetError>;

    /// Main event loop logic of running this replica.
    async fn run(&mut self);
}
