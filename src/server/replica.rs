//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::net::SocketAddr;

use crate::utils::SummersetError;

use async_trait::async_trait;

/// Server replica ID type.
pub type ReplicaId = u8;

/// Replica trait to be implement by all protocol-specific server structs.
#[async_trait]
pub trait GenericReplica {
    /// Creates a new replica module and sets up required functionality modules
    /// according to protocol-specific logic.
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        manager: SocketAddr, // remote address of manager oracle
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError>
    where
        Self: Sized;

    /// Main event loop logic of running this replica.
    async fn run(&mut self);
}
