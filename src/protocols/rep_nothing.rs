//! Replication protocol: replicate nothing.
//!
//! Immediately logs given command and executes given command on the state
//! machine upon receiving a client command, and does nothing else.

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::core::utils::SummersetError;
use crate::core::replica::{GenericReplica, ReplicaId};
use crate::core::client::{GenericClient, ClientId};
use crate::core::external::{ExternalApi, ApiRequest, ApiReply};
use crate::core::statemach::{StateMachine, Command};
use crate::core::storage::StorageHub;

use async_trait::async_trait;

use tokio::time::Duration;

use log::error;

/// Log entry type.
struct LogEntry {
    cmd: Command,
}

/// RepNothing server replica module.
#[derive(Debug)]
pub struct RepNothingReplica {
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
    external_api: Option<ExternalApi>,

    /// StateMachine module.
    state_machine: Option<StateMachine>,

    /// StorageHub module.
    storage_hub: Option<StorageHub<LogEntry>>,
    // TransportHub module not needed here.
}

#[async_trait]
impl GenericReplica for RepNothingReplica {
    async fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        backer_file: Option<Path>,
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
        if batch_interval < Duration::from_micros(1) {
            return logged_err!(
                id,
                "batch_interval '{}' too small",
                batch_interval
            );
        }
        if chan_cap_base == 0 {
            return logged_err!(id, "invalid chan_cap_base {}", chan_cap_base);
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

    async fn run(&mut self) -> Result<(), SummersetError> {
        todo!();
    }
}

/// RepNothing client-side module.
pub struct RepNothingClient {
    /// Client ID.
    id: ClientId,
}

#[async_trait]
impl GenericClient for RepNothingClient {
    async fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
    ) -> Result<Self, SummersetError> {
        todo!();
    }

    async fn send_req(&self, req: ApiRequest) -> Result<(), SummersetError> {
        todo!()
    }

    async fn recv_reply(&self) -> Result<ApiReply, SummersetError> {
        todo!()
    }
}
