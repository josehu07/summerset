//! Replication protocol: replicate nothing.
//!
//! Immediately logs given command and executes given command on the state
//! machine upon receiving a client command, and does nothing else.

use std::path::Path;
use std::net::SocketAddr;

use crate::core::utils::SummersetError;
use crate::core::replica::{GenericReplica, ReplicaId};
use crate::core::client::{GenericClient, ClientId};
use crate::core::external::ExternalApi;
use crate::core::statemach::{StateMachine, Command};
use crate::core::storage::StorageHub;

use tokio::time::Duration;

use log::error;

/// Log entry type.
struct LogEntry {
    cmd: Command,
}

/// RepNothing server replica module.
#[derive(Debug)]
pub struct RepNothingReplica<'r> {
    /// Is this replica running?
    running: bool,

    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    population: u8,

    /// Address string for peer-to-peer connections.
    smr_addr: SocketAddr,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Backer file path.
    backer_file: Path,

    /// Client requqest batch interval.
    batch_interval: Duration,

    /// Base capacity of channels across modules.
    chan_cap_base: usize,

    /// ExternalApi module.
    external_api: Option<ExternalApi<'r, Self>>,

    /// StateMachine module.
    state_machine: Option<StateMachine<'r, Self>>,

    /// StorageHub module.
    storage_hub: Option<StorageHub<'r, Self, LogEntry>>,
    // TransportHub module not needed here.
}

impl<'r> RepNothingReplica<'r> {
    /// Creates a new RepNothing replica.
    pub fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        backer_file: Path,
        batch_interval: Duration,
        chan_cap_base: usize,
    ) -> Result<Self, SummersetError> {
        if population == 0 {
            return Err(SummersetError(format!(
                "invalid population {}",
                population
            )));
        }
        if id >= population {
            return Err(SummersetError(format!(
                "invalid replica ID {} / {}",
                id, population
            )));
        }
        if smr_addr == api_addr {
            return logged_err!(
                id,
                "smr_addr and api_addr are the same '{}'",
                smr_addr
            );
        }

        Ok(RepNothingReplica {
            running: false,
            id,
            population,
            smr_addr,
            api_addr,
            backer_file,
            batch_interval,
            chan_cap_base,
            external_api: None,
            state_machine: None,
            storage_hub: None,
        })
    }
}

#[async_trait]
impl GenericReplica for RepNothingReplica {
    fn id(&self) -> ReplicaId {
        self.id
    }

    fn population(&self) -> u8 {
        self.population
    }

    fn smr_addr(&self) -> &str {
        &self.smr_addr
    }

    fn api_addr(&self) -> &str {
        &self.api_addr
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        let mut external_api = ExternalApi::new(&self)
    }

    async fn run(&mut self) -> Result<(), SummersetError> {}
}
