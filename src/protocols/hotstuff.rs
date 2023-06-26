//! Replication protocol: HotStuff

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{
    ReplicaId, StateMachine, CommandResult, CommandId, ExternalApi, ApiRequest,
    ApiReply, StorageHub, LogAction, LogResult, LogActionId, GenericReplica,
};
use crate::client::{
    ClientId, ClientApiStub, ClientSendStub, ClientRecvStub, GenericClient,
};

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigHotStuff {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

    /// Path to backing file.
    pub backer_path: String,

    /// Base capacity for most channels.
    pub base_chan_cap: usize,

    /// Capacity for req/reply channels.
    pub api_chan_cap: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigHotStuff {
    fn default() -> Self {
        ReplicaConfigHotStuff {
            batch_interval_us: 1000,
            backer_path: "/tmp/summerset.hotstuff.wal".into(),
            base_chan_cap: 1000,
            api_chan_cap: 10000,
        }
    }
}

/// Log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct LogEntry {
    reqs: Vec<(ClientId, ApiRequest)>,
}

/// In-memory instance containing a commands batch.
// TODO: add encryption
struct Instance {
    reqs: Vec<(ClientId, ApiRequest)>,
    durable: bool,
    execed: Vec<bool>,
}

/// HotStuff server replica module
pub struct HotStuffReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    _population: u8,

    /// Address string for peer-to-peer connections.
    _smr_addr: SocketAddr,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Configuraiton parameters struct.
    config: ReplicaConfigHotStuff,

    /// Map from peer replica ID -> address.
    _peer_addrs: HashMap<ReplicaId, SocketAddr>,

    /// ExternalApi module.
    external_api: Option<ExternalApi>,

    /// StateMachine module.
    state_machine: Option<StateMachine>,

    /// StorageHub module.
    storage_hub: Option<StorageHub<LogEntry>>,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Current durable log file offset.
    log_offset: usize,
}

// TODO: add server implementation
impl HotStuffReplica {

}

#[async_trait]
impl GenericReplica for HotStuffReplica {
    fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        todo!();
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        todo!();
    }

    async fn run(&mut self) {
        todo!();
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigHotStuff {
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigHotStuff {
    fn default() -> Self {
        ClientConfigHotStuff {}
    }
}

/// HotStuff client-side module.
pub struct HotStuffClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigHotStuff,

    /// Stubs for communicating with servers. 
    stubs: HashMap<ReplicaId, (ClientSendStub, ClientRecvStub)>,
}

#[async_trait]
impl GenericClient for HotStuffClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.is_empty() {
            return logged_err!(id; "empty servers list");
        }

        // let config = parsed_config!(config_str => ClientConfigHotStuff; )?;
        // TODO: 
        let config = ClientConfigHotStuff {};

        Ok(HotStuffClient {
            id,
            servers,
            config,
            stubs: HashMap::new(),
        })
    }

    async fn connect(
        &mut self,
    ) -> Result<(ClientSendStub, ClientRecvStub), SummersetError> {

        // TODO: make this concurrent
        // for (id, addr) in self.servers.iter() {

        // }
        todo!();
        // let api_stub = ClientApiStub::new(self.id);
        // api_stub.connect(self.servers[&self.config.server_id]).await
    }
}
