//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::net::SocketAddr;

use crate::utils::SummersetError;

use async_trait::async_trait;

use tokio::sync::watch;

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

    /// Main event loop logic of running this replica. Returns `Ok(true)` if
    /// terminated normally and wants to restart (e.g., receiving a reset
    /// control message) or `Ok(false)` if terminated normally and does not
    /// want to restart (e.g., receiving a termination signal).
    async fn run(
        &mut self,
        rx_term: watch::Receiver<bool>, // termination signals channel
    ) -> Result<bool, SummersetError>;

    /// Gets my replica ID.
    fn id(&self) -> ReplicaId;
}
