//! Replication protocol: replicate nothing.
//!
//! Immediately logs given command and executes given command on the state
//! machine upon receiving a client command, and does nothing else.

mod control;
mod durability;
mod execution;
mod recovery;
mod request;

use std::net::SocketAddr;
use std::path::Path;

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, CommandId, ControlHub, ExternalApi, GenericReplica,
    ReplicaId, StateMachine, StorageHub,
};
use crate::utils::SummersetError;

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::time::Duration;

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigRepNothing {
    /// Client request batching interval in millisecs.
    pub batch_interval_ms: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing file.
    pub backer_path: String,

    /// Whether to call `fsync()`/`fdatasync()` on logger.
    pub logger_sync: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigRepNothing {
    fn default() -> Self {
        ReplicaConfigRepNothing {
            batch_interval_ms: 10,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.rep_nothing.wal".into(),
            logger_sync: false,
        }
    }
}

/// WAL log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) struct WalEntry {
    reqs: Vec<(ClientId, ApiRequest)>,
}

/// In-memory instance containing a commands batch.
pub(crate) struct Instance {
    reqs: Vec<(ClientId, ApiRequest)>,
    durable: bool,
    execed: Vec<bool>,
}

/// RepNothing server replica module.
// TransportHub module not needed here.
pub(crate) struct RepNothingReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Configuration parameters struct.
    config: ReplicaConfigRepNothing,

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

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Current durable WAL log file offset.
    wal_offset: usize,
}

// RepNothingReplica common helpers
impl RepNothingReplica {
    /// Compose CommandId from instance index & command index within.
    #[inline]
    fn make_command_id(inst_idx: usize, cmd_idx: usize) -> CommandId {
        debug_assert!(inst_idx <= (u32::MAX as usize));
        debug_assert!(cmd_idx <= (u32::MAX as usize));
        (inst_idx << 32 | cmd_idx) as CommandId
    }

    /// Decompose CommandId into instance index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let inst_idx = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (inst_idx, cmd_idx)
    }
}

#[async_trait]
impl GenericReplica for RepNothingReplica {
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        ctrl_bind: SocketAddr,
        _p2p_bind_base: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a server ID
        let mut control_hub =
            ControlHub::new_and_setup(ctrl_bind, manager).await?;
        let id = control_hub.me;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ReplicaConfigRepNothing;
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

        // TransportHub is not needed in RepNothing

        // tell the manager tha I have joined
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::RepNothing,
            api_addr,
            p2p_addr,
        })?;
        control_hub.recv_ctrl().await?;

        // setup external API module, ready to take in client requests
        let external_api = ExternalApi::new_and_setup(
            id,
            api_addr,
            Duration::from_millis(config.batch_interval_ms),
            config.max_batch_size,
        )
        .await?;

        Ok(RepNothingReplica {
            id,
            config,
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
            control_hub,
            external_api,
            state_machine,
            storage_hub,
            insts: vec![],
            wal_offset: 0,
        })
    }

    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
        // recover state from durable storage WAL log
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
                        pf_error!("error handling log result {}: {}", action_id, e);
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
                                pf_warn!(
                                    "server got {} req",
                                    if restart { "restart" } else { "shutdown" });
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
pub struct ClientConfigRepNothing {
    /// Which server to pick.
    pub server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigRepNothing {
    fn default() -> Self {
        ClientConfigRepNothing { server_id: 0 }
    }
}

/// RepNothing client-side module.
pub(crate) struct RepNothingClient {
    /// Client ID.
    id: ClientId,

    /// Configuration parameters struct.
    config: ClientConfigRepNothing,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stub for communicating with the current server.
    api_stub: Option<ClientApiStub>,

    /// Base bind address for sockets connecting to servers.
    api_bind_base: SocketAddr,
}

#[async_trait]
impl GenericEndpoint for RepNothingClient {
    async fn new_and_setup(
        ctrl_base: SocketAddr,
        api_bind_base: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub =
            ClientCtrlStub::new_by_connect(ctrl_base, manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigRepNothing;
                                    server_id)?;

        Ok(RepNothingClient {
            id,
            config,
            ctrl_stub,
            api_stub: None,
            api_bind_base,
        })
    }

    async fn connect(&mut self) -> Result<(), SummersetError> {
        // disallow reconnection without leaving
        if self.api_stub.is_some() {
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
                // find a server to connect to, starting from provided server_id
                debug_assert!(!servers_info.is_empty());
                while !servers_info.contains_key(&self.config.server_id) {
                    self.config.server_id =
                        (self.config.server_id + 1) % population;
                }
                // connect to that server
                pf_debug!(
                    "connecting to server {} '{}'...",
                    self.config.server_id,
                    servers_info[&self.config.server_id].api_addr
                );
                let bind_addr = SocketAddr::new(
                    self.api_bind_base.ip(),
                    self.api_bind_base.port() + self.config.server_id as u16,
                );
                let api_stub = ClientApiStub::new_by_connect(
                    self.id,
                    bind_addr,
                    servers_info[&self.config.server_id].api_addr,
                )
                .await?;
                self.api_stub = Some(api_stub);
                Ok(())
            }
            _ => logged_err!("unexpected reply type received"),
        }
    }

    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        // send leave notification to current connected server
        if let Some(mut api_stub) = self.api_stub.take() {
            let mut sent = api_stub.send_req(Some(&ApiRequest::Leave))?;
            while !sent {
                sent = api_stub.send_req(None)?;
            }

            while api_stub.recv_reply().await? != ApiReply::Leave {}
            pf_debug!("left current server connection");
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
        match self.api_stub {
            Some(ref mut api_stub) => api_stub.send_req(req),
            None => Err(SummersetError::msg("client not set up")),
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        match self.api_stub {
            Some(ref mut api_stub) => api_stub.recv_reply().await,
            None => Err(SummersetError::msg("client not set up")),
        }
    }

    fn id(&self) -> ClientId {
        self.id
    }

    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        &mut self.ctrl_stub
    }
}
