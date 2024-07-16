//! Replication protocol: Chain Replication.
//!
//! A partical implementation without proper fault-tolerance. References:
//!   - <https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf>
//!   - <https://www.usenix.org/conference/atc22/presentation/fouto>

mod control;
mod durability;
mod execution;
mod messages;
mod recovery;
mod request;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, CommandId, CommandResult, ControlHub, ExternalApi,
    GenericReplica, LogActionId, ReplicaId, StateMachine, StorageHub,
    TransportHub,
};
use crate::utils::SummersetError;

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::time::Duration;

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigChainRep {
    /// Client request batching interval in millisecs.
    pub batch_interval_ms: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing log file.
    pub backer_path: String,

    /// Whether to call `fsync()`/`fdatasync()` on logger.
    pub logger_sync: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigChainRep {
    fn default() -> Self {
        ReplicaConfigChainRep {
            batch_interval_ms: 10,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.chain_rep.wal".into(),
            logger_sync: false,
        }
    }
}

/// Log entry status enum.
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize,
)]
pub(crate) enum Status {
    Null = 0,
    Streaming = 1,
    Propagated = 2,
    Executed = 3,
}

/// Request batch type.
pub(crate) type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// In-memory log entry containing a commands batch.
pub(crate) struct LogEntry {
    /// Log entry status.
    status: Status,

    /// Batch of client requests.
    reqs: ReqBatch,

    /// Offset of first durable WAL log entry related to this entry.
    wal_offset: usize,
}

/// Stable storage WAL log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) struct WalEntry {
    slot: usize,
    reqs: ReqBatch,
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum PeerMsg {
    /// Propagate message from predecessor to successor.
    Propagate { slot: usize, reqs: ReqBatch },

    /// Propagate reply from successor to predecessor (mimics tracking of
    /// message delivery status).
    PropagateReply { slot: usize },
}

/// ChainRep server replica module.
pub(crate) struct ChainRepReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Configuration parameters struct.
    config: ReplicaConfigChainRep,

    /// Address string for client requests API.
    _api_addr: SocketAddr,

    /// Address string for internal peer-peer communication.
    _p2p_addr: SocketAddr,

    /// ControlHub module.
    control_hub: ControlHub,

    /// ExternalApi module.
    external_api: ExternalApi,

    /// StateMachine module.
    state_machine: StateMachine,

    /// StorageHub module.
    storage_hub: StorageHub<WalEntry>,

    /// TransportHub module.
    transport_hub: TransportHub<PeerMsg>,

    /// In-memory log of entries.
    log: Vec<LogEntry>,

    /// Index of the first non-propagated log entry.
    prop_bar: usize,

    /// Index of the first non-executed log entry.
    /// It is always true that exec_bar <= prop_bar <= log.len()
    exec_bar: usize,

    /// Current durable WAL log file offset.
    wal_offset: usize,
}

// ChainRepReplica common helpers
impl ChainRepReplica {
    /// Am I the head node?
    #[inline]
    fn is_head(&self) -> bool {
        self.id == 0
    }

    /// Am I the tail node?
    #[inline]
    fn is_tail(&self) -> bool {
        self.id == self.population - 1
    }

    /// Who's my predecessor?
    #[inline]
    fn predecessor(&self) -> Option<ReplicaId> {
        if self.is_head() {
            None
        } else {
            Some(self.id - 1)
        }
    }

    /// Who's my successor?
    #[inline]
    fn successor(&self) -> Option<ReplicaId> {
        if self.is_tail() {
            None
        } else {
            Some(self.id + 1)
        }
    }

    /// Create an empty null log entry.
    #[inline]
    fn null_log_entry() -> LogEntry {
        LogEntry {
            status: Status::Null,
            reqs: vec![],
            wal_offset: 0,
        }
    }

    /// Locate the first null log entry or append one if no holes exist.
    fn first_null_slot(&mut self) -> usize {
        for s in self.exec_bar..self.log.len() {
            if self.log[s].status == Status::Null {
                return s;
            }
        }
        self.log.push(Self::null_log_entry());
        self.log.len() - 1
    }

    /// Compose CommandId from:
    ///   - slot index & command index within if non read-only
    ///   - request ID & client ID if read-only
    #[inline]
    fn make_command_id(
        slot_or_req_id: usize,
        cmd_idx_or_client: usize,
        read_only: bool,
    ) -> CommandId {
        debug_assert!(slot_or_req_id <= (u32::MAX as usize));
        debug_assert!(cmd_idx_or_client <= ((u32::MAX >> 1) as usize));
        let mut cmd_id =
            ((slot_or_req_id << 32) | (cmd_idx_or_client << 1)) as CommandId;
        if read_only {
            cmd_id += 1;
        }
        cmd_id
    }

    /// Decompose CommandId into:
    ///   - slot index & command index within if non read-only
    ///   - request ID & client ID if read-only
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize, bool) {
        let slot_or_req_id = (command_id >> 32) as usize;
        let cmd_idx_or_client = ((command_id & ((1 << 32) - 1)) >> 1) as usize;
        let read_only = (command_id % 2) == 1;
        (slot_or_req_id, cmd_idx_or_client, read_only)
    }
}

#[async_trait]
impl GenericReplica for ChainRepReplica {
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        ctrl_bind: SocketAddr,
        p2p_bind_base: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a server ID
        let mut control_hub =
            ControlHub::new_and_setup(ctrl_bind, manager).await?;
        let id = control_hub.me;
        let population = control_hub.population;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ReplicaConfigChainRep;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync)?;
        if config.batch_interval_ms == 0 {
            return logged_err!(
                "invalid config.batch_interval_ms '{}'",
                config.batch_interval_ms
            );
        }

        // setup state machine module
        let state_machine = StateMachine::new_and_setup(id).await?;

        // setup storage hub module
        let storage_hub =
            StorageHub::new_and_setup(id, Path::new(&config.backer_path))
                .await?;

        // setup transport hub module
        let mut transport_hub =
            TransportHub::new_and_setup(id, population, p2p_addr).await?;

        // ask for the list of peers to proactively connect to. Do this after
        // transport hub has been set up, so that I will be able to accept
        // later peer connections
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::ChainRep,
            api_addr,
            p2p_addr,
        })?;
        let to_peers = if let CtrlMsg::ConnectToPeers { to_peers, .. } =
            control_hub.recv_ctrl().await?
        {
            to_peers
        } else {
            return logged_err!("unexpected ctrl msg type received");
        };

        // proactively connect to some peers, then wait for all population
        // have been connected with me
        for (peer, conn_addr) in to_peers {
            let bind_addr = SocketAddr::new(
                p2p_bind_base.ip(),
                p2p_bind_base.port() + peer as u16,
            );
            transport_hub
                .connect_to_peer(peer, bind_addr, conn_addr)
                .await?;
        }
        transport_hub.wait_for_group(population).await?;

        // setup external API module, ready to take in client requests
        let external_api = ExternalApi::new_and_setup(
            id,
            api_addr,
            Duration::from_millis(config.batch_interval_ms),
            config.max_batch_size,
        )
        .await?;

        Ok(ChainRepReplica {
            id,
            population,
            config,
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
            control_hub,
            external_api,
            state_machine,
            storage_hub,
            transport_hub,
            log: vec![],
            prop_bar: 0,
            exec_bar: 0,
            wal_offset: 0,
        })
    }

    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
        // recover the log & state from durable WAL log
        self.recover_from_wal().await?;

        // main event loop
        let mut paused = false;
        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.get_req_batch(), if !paused => {
                    if let Err(e) = req_batch {
                        pf_error!("error getting req batch: {}", e);
                        continue;
                    }
                    let req_batch = req_batch.unwrap();
                    if let Err(e) = self.handle_req_batch(req_batch) {
                        pf_error!("error handling req batch: {}", e);
                    }
                },

                // durable logging result
                log_result = self.storage_hub.get_result(), if !paused => {
                    if let Err(e) = log_result {
                        pf_error!("error getting log result: {}", e);
                        continue;
                    }
                    let (action_id, log_result) = log_result.unwrap();
                    if let Err(e) = self.handle_log_result(action_id, log_result) {
                        pf_error!("error handling log result {}: {}",
                                           action_id, e);
                    }
                },

                // message from peer
                msg = self.transport_hub.recv_msg(), if !paused => {
                    if let Err(_e) = msg {
                        // NOTE: commented out to prevent console lags
                        // during benchmarking
                        // pf_error!("error receiving peer msg: {}", e);
                        continue;
                    }
                    let (peer, msg) = msg.unwrap();
                    if let Err(e) = self.handle_msg_recv(peer, msg) {
                        pf_error!("error handling msg recv <- {}: {}", peer, e);
                    }
                },

                // state machine execution result
                cmd_result = self.state_machine.get_result(), if !paused => {
                    if let Err(e) = cmd_result {
                        pf_error!("error getting cmd result: {}", e);
                        continue;
                    }
                    let (cmd_id, cmd_result) = cmd_result.unwrap();
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result) {
                        pf_error!("error handling cmd result {}: {}", cmd_id, e);
                    }
                },

                // manager control message
                ctrl_msg = self.control_hub.recv_ctrl() => {
                    if let Err(e) = ctrl_msg {
                        pf_error!("error getting ctrl msg: {}", e);
                        continue;
                    }
                    let ctrl_msg = ctrl_msg.unwrap();
                    match self.handle_ctrl_msg(ctrl_msg, &mut paused).await {
                        Ok(terminate) => {
                            if let Some(restart) = terminate {
                                return Ok(restart);
                            }
                        },
                        Err(e) => {
                            pf_error!("error handling ctrl msg: {}", e);
                        }
                    }
                },

                // receiving termination signal
                _ = rx_term.changed() => {
                    pf_warn!("server caught termination signal");
                    return Ok(false);
                }
            }
        }
    }

    fn id(&self) -> ReplicaId {
        self.id
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigChainRep {
    /// Which server to consider as head initially.
    pub init_head_id: ReplicaId,

    /// Which server to consider as tail initially.
    pub init_tail_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigChainRep {
    fn default() -> Self {
        ClientConfigChainRep {
            init_head_id: 0,
            init_tail_id: 9, // will cap to the last server
        }
    }
}

/// ChainRep client-side module.
pub(crate) struct ChainRepClient {
    /// Client ID.
    id: ClientId,

    /// Configuration parameters struct.
    _config: ClientConfigChainRep,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Current head server ID.
    head_id: ReplicaId,

    /// Current tail server ID.
    tail_id: ReplicaId,

    /// Was the last request sent read_only?
    last_read_only: bool,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stubs for communicating with the head server.
    /// Separated out to make `tokio::select!` happy.
    head_api_stub: Option<ClientApiStub>,

    /// API stubs for communicating with the tail server.
    /// Separated out to make `tokio::select!` happy.
    tail_api_stub: Option<ClientApiStub>,

    /// API stubs for communicating with middle servers.
    mid_api_stubs: HashMap<ReplicaId, ClientApiStub>,

    /// Base bind address for sockets connecting to servers.
    api_bind_base: SocketAddr,
}

#[async_trait]
impl GenericEndpoint for ChainRepClient {
    async fn new_and_setup(
        ctrl_bind: SocketAddr,
        api_bind_base: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub =
            ClientCtrlStub::new_by_connect(ctrl_bind, manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigChainRep;
                                    init_head_id, init_tail_id)?;
        let init_head_id = config.init_head_id;
        let init_tail_id = config.init_tail_id;

        Ok(ChainRepClient {
            id,
            _config: config,
            servers: HashMap::new(),
            head_id: init_head_id,
            tail_id: init_tail_id,
            last_read_only: false,
            ctrl_stub,
            head_api_stub: None,
            tail_api_stub: None,
            mid_api_stubs: HashMap::new(),
            api_bind_base,
        })
    }

    async fn connect(&mut self) -> Result<(), SummersetError> {
        // disallow reconnection without leaving
        if self.head_api_stub.is_some() || self.tail_api_stub.is_some() {
            return logged_err!("reconnecting without leaving");
        }

        // ask the manager about the list of active servers
        let mut sent =
            self.ctrl_stub.send_req(Some(&CtrlRequest::QueryInfo))?;
        while !sent {
            sent = self.ctrl_stub.send_req(None)?;
        }

        let reply = self.ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::QueryInfo {
                population,
                servers_info,
            } => {
                // shift to a new head_id/tail_id if current one not active
                debug_assert!(!servers_info.is_empty());
                while !servers_info.contains_key(&self.head_id)
                    || servers_info[&self.head_id].is_paused
                {
                    self.head_id = (self.head_id + 1) % population;
                }
                while !servers_info.contains_key(&self.tail_id)
                    || servers_info[&self.tail_id].is_paused
                {
                    self.tail_id =
                        if self.tail_id == 0 || self.tail_id >= population {
                            population - 1
                        } else {
                            self.tail_id - 1
                        };
                }
                if self.head_id == self.tail_id {
                    return logged_err!(
                        "head & tail resolve to the same server {}",
                        self.head_id
                    );
                }

                // establish connection to all servers
                self.servers = servers_info
                    .into_iter()
                    .map(|(id, info)| (id, info.api_addr))
                    .collect();
                for (&id, &server) in &self.servers {
                    pf_debug!("connecting to server {} '{}'...", id, server);
                    let bind_addr = SocketAddr::new(
                        self.api_bind_base.ip(),
                        self.api_bind_base.port() + id as u16,
                    );
                    let api_stub = ClientApiStub::new_by_connect(
                        self.id, bind_addr, server,
                    )
                    .await?;

                    if id == self.head_id {
                        self.head_api_stub = Some(api_stub);
                    } else if id == self.tail_id {
                        self.tail_api_stub = Some(api_stub);
                    } else {
                        self.mid_api_stubs.insert(id, api_stub);
                    }
                }

                debug_assert!(self.head_api_stub.is_some());
                debug_assert!(self.tail_api_stub.is_some());
                Ok(())
            }

            _ => logged_err!("unexpected reply type received"),
        }
    }

    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        // send leave notification to all servers
        self.mid_api_stubs
            .insert(self.head_id, self.head_api_stub.take().unwrap());
        self.mid_api_stubs
            .insert(self.tail_id, self.tail_api_stub.take().unwrap());
        for (id, mut api_stub) in self.mid_api_stubs.drain() {
            let mut sent = api_stub.send_req(Some(&ApiRequest::Leave))?;
            while !sent {
                sent = api_stub.send_req(None)?;
            }

            // NOTE: commented out the following wait to avoid accidental
            // hanging upon leaving
            // while api_stub.recv_reply().await? != ApiReply::Leave {}
            pf_debug!("left server connection {}", id);
        }

        // if permanently leaving, send leave notification to the manager
        if permanent {
            let mut sent =
                self.ctrl_stub.send_req(Some(&CtrlRequest::Leave))?;
            while !sent {
                sent = self.ctrl_stub.send_req(None)?;
            }

            while self.ctrl_stub.recv_reply().await? != CtrlReply::Leave {}
            pf_debug!("left manager connection");
        }

        Ok(())
    }

    fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError> {
        let read_only = match req {
            Some(req) if req.read_only() => true,
            None if self.last_read_only => true,
            _ => false,
        };

        debug_assert!(self.head_api_stub.is_some());
        debug_assert!(self.tail_api_stub.is_some());
        if read_only {
            self.tail_api_stub.as_mut().unwrap().send_req(req)
        } else {
            self.head_api_stub.as_mut().unwrap().send_req(req)
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        debug_assert!(self.head_api_stub.is_some());
        debug_assert!(self.tail_api_stub.is_some());
        let head_stub = self.head_api_stub.as_mut().unwrap();
        let tail_stub = self.tail_api_stub.as_mut().unwrap();

        let reply = tokio::select! {
            reply = head_stub.recv_reply() => { reply? },
            reply = tail_stub.recv_reply() => { reply? },
        };

        // ignoring the `redirect` field here...
        Ok(reply)
    }

    fn id(&self) -> ClientId {
        self.id
    }

    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        &mut self.ctrl_stub
    }
}
