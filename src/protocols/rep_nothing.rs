//! Replication protocol: replicate nothing.
//!
//! Immediately logs given command and executes given command on the state
//! machine upon receiving a client command, and does nothing else.

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{
    GenericReplica, ReplicaId, StateMachine, Command, ExternalApi, StorageHub,
};
use crate::client::{
    GenericClient, ClientId, ClientApiStub, ClientSendStub, ClientRecvStub,
};

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;

/// Log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum LogEntry {
    Cmd { cmd: Command },
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct RepNothingReplicaConfig {
    /// Client request batching interval in microsecs.
    batch_interval_us: u64,

    /// Path to backing file.
    backer_path: String,

    /// Base capacity for most channels.
    base_chan_cap: usize,

    /// Capacity for req/reply channels.
    api_chan_cap: usize,
}

impl Default for RepNothingReplicaConfig {
    fn default() -> Self {
        RepNothingReplicaConfig {
            batch_interval_us: 1000,
            backer_path: "/tmp/summerset.rep_nothing.wal".into(),
            base_chan_cap: 1000,
            api_chan_cap: 10000,
        }
    }
}

/// RepNothing server replica module.
pub struct RepNothingReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    population: u8,

    /// Address string for peer-to-peer connections.
    smr_addr: SocketAddr,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Configuraiton parameters struct.
    config: RepNothingReplicaConfig,

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
    fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        config_str: Option<&str>,
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
                id;
                "smr_addr and api_addr are the same '{}'",
                smr_addr
            );
        }

        let config = parsed_config!(config_str => RepNothingReplicaConfig; batch_interval_us, backer_path, base_chan_cap, api_chan_cap)?;
        if config.batch_interval_us == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_us '{}'",
                config.batch_interval_us
            );
        }
        if config.base_chan_cap == 0 {
            return logged_err!(
                id;
                "invalid config.base_chan_cap {}",
                config.base_chan_cap
            );
        }
        if config.api_chan_cap == 0 {
            return logged_err!(
                id;
                "invalid config.api_chan_cap {}",
                config.api_chan_cap
            );
        }

        Ok(RepNothingReplica {
            id,
            population,
            smr_addr,
            api_addr,
            config,
            external_api: None,
            state_machine: None,
            storage_hub: None,
        })
    }

    async fn setup(
        &mut self,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
    ) -> Result<(), SummersetError> {
        let state_machine = StateMachine::new(self.id);
        state_machine
            .setup(self.config.base_chan_cap, self.config.base_chan_cap)
            .await?;
        self.state_machine = Some(state_machine);

        let storage_hub = StorageHub::new(self.id);
        storage_hub
            .setup(
                &Path::new(&self.config.backer_path),
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;
        self.storage_hub = Some(storage_hub);

        let external_api = ExternalApi::new(self.id);
        external_api
            .setup(
                self.api_addr,
                Duration::from_micros(self.config.batch_interval_us),
                self.config.api_chan_cap,
                self.config.api_chan_cap,
            )
            .await?;
        self.external_api = Some(external_api);

        Ok(())
    }

    async fn run(&mut self) -> Result<(), SummersetError> {
        loop {
            todo!();
        }
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct RepNothingClientConfig {
    /// Which server to pick.
    server_id: ReplicaId,
}

impl Default for RepNothingClientConfig {
    fn default() -> Self {
        RepNothingClientConfig { server_id: 0 }
    }
}

/// RepNothing client-side module.
pub struct RepNothingClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: RepNothingClientConfig,
}

#[async_trait]
impl GenericClient for RepNothingClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.len() == 0 {
            return logged_err!(id; "empty servers list");
        }

        let config =
            parsed_config!(config_str => RepNothingClientConfig; server_id)?;
        if !servers.contains_key(&config.server_id) {
            return logged_err!(
                id;
                "server_id {} not found in servers",
                config.server_id
            );
        }

        Ok(RepNothingClient {
            id,
            servers,
            config,
        })
    }

    async fn connect(
        &mut self,
    ) -> Result<(ClientSendStub, ClientRecvStub), SummersetError> {
        let api_stub = ClientApiStub::new(self.id);
        api_stub.connect(self.servers[&self.config.server_id]).await
    }
}
