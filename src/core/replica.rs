//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::path::Path;
use std::net::SocketAddr;

use crate::core::utils::SummersetError;

use async_trait::async_trait;

use tokio::time::Duration;

/// Server replica ID type.
pub type ReplicaId = u8;

/// Replica trait to be implement by all protocol-specific server structs.
#[async_trait]
pub trait GenericReplica {
    /// Create new replica module and setup required functionality modules.
    async fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        backer_file: Option<Path>,
        batch_interval: Duration,
        chan_cap_base: usize,
    ) -> Result<Self, SummersetError>;

    /// Main event loop logic of running this replica.
    async fn run(&mut self) -> Result<(), SummersetError>;
}
