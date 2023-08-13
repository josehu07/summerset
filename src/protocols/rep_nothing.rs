//! Replication protocol: replicate nothing.
//!
//! Immediately logs given command and executes given command on the state
//! machine upon receiving a client command, and does nothing else.

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{
    ReplicaId, StateMachine, CommandResult, CommandId, ExternalApi, ApiRequest,
    ApiReply, StorageHub, LogAction, LogResult, LogActionId, GenericReplica,
};
use crate::client::{
    ClientId, ClientApiStub, ClientSendStub, ClientRecvStub, GenericEndpoint,
};

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigRepNothing {
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
impl Default for ReplicaConfigRepNothing {
    fn default() -> Self {
        ReplicaConfigRepNothing {
            batch_interval_us: 1000,
            backer_path: "/tmp/summerset.rep_nothing.wal".into(),
            base_chan_cap: 100000,
            api_chan_cap: 1000000,
        }
    }
}

/// Log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct LogEntry {
    reqs: Vec<(ClientId, ApiRequest)>,
}

/// In-memory instance containing a commands batch.
struct Instance {
    reqs: Vec<(ClientId, ApiRequest)>,
    durable: bool,
    execed: Vec<bool>,
}

/// RepNothing server replica module.
// TransportHub module not needed here.
pub struct RepNothingReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    _population: u8,

    /// Configuraiton parameters struct.
    config: ReplicaConfigRepNothing,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Local address strings for peer-peer connections.
    _conn_addrs: HashMap<ReplicaId, SocketAddr>,

    /// Map from peer replica ID -> address.
    _peer_addrs: HashMap<ReplicaId, SocketAddr>,

    /// ExternalApi module.
    external_api: ExternalApi,

    /// StateMachine module.
    state_machine: StateMachine,

    /// StorageHub module.
    storage_hub: StorageHub<LogEntry>,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Current durable log file offset.
    log_offset: usize,
}

impl RepNothingReplica {
    /// Compose CommandId from instance index & command index within.
    fn make_command_id(inst_idx: usize, cmd_idx: usize) -> CommandId {
        assert!(inst_idx <= (u32::MAX as usize));
        assert!(cmd_idx <= (u32::MAX as usize));
        (inst_idx << 32 | cmd_idx) as CommandId
    }

    /// Decompose CommandId into instance index & command index within.
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let inst_idx = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (inst_idx, cmd_idx)
    }

    /// Handler of client request batch chan recv.
    async fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        assert!(batch_size > 0);

        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            execed: vec![false; batch_size],
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst);

        // submit log action to make this instance durable
        let log_entry = LogEntry { reqs: req_batch };
        self.storage_hub
            .submit_action(
                inst_idx as LogActionId,
                LogAction::Append {
                    entry: log_entry,
                    sync: true,
                },
            )
            .await?;

        Ok(())
    }

    /// Handler of durable logging result chan recv.
    async fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<LogEntry>,
    ) -> Result<(), SummersetError> {
        let inst_idx = action_id as usize;
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid log action ID {} seen", inst_idx);
        }

        match log_result {
            LogResult::Append { now_size } => {
                assert!(now_size >= self.log_offset);
                self.log_offset = now_size;
            }
            _ => {
                return logged_err!(self.id; "unexpected log result type for {}: {:?}", inst_idx, log_result);
            }
        }

        let inst = &mut self.insts[inst_idx];
        if inst.durable {
            return logged_err!(self.id; "duplicate log action ID {} seen", inst_idx);
        }
        inst.durable = true;

        // submit execution commands in order
        for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
            match req {
                ApiRequest::Req { cmd, .. } => {
                    self.state_machine
                        .submit_cmd(
                            Self::make_command_id(inst_idx, cmd_idx),
                            cmd.clone(),
                        )
                        .await?
                }
                _ => continue, // ignore other types of requests
            }
        }

        Ok(())
    }

    /// Handler of state machine exec result chan recv.
    async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (inst_idx, cmd_idx) = Self::split_command_id(cmd_id);
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, inst_idx, cmd_idx);
        }

        let inst = &mut self.insts[inst_idx];
        if cmd_idx >= inst.reqs.len() {
            return logged_err!(self.id; "invalid command ID {} ({}|{}) seen", cmd_id, inst_idx, cmd_idx);
        }
        if inst.execed[cmd_idx] {
            return logged_err!(self.id; "duplicate command index {}|{}", inst_idx, cmd_idx);
        }
        if !inst.durable {
            return logged_err!(self.id; "instance {} is not durable yet", inst_idx);
        }
        inst.execed[cmd_idx] = true;

        // reply to the corresponding client of this request
        let (client, req) = &inst.reqs[cmd_idx];
        match req {
            ApiRequest::Req { id: req_id, .. } => {
                self.external_api
                    .send_reply(
                        ApiReply::Reply {
                            id: *req_id,
                            result: Some(cmd_result),
                            redirect: None,
                        },
                        *client,
                    )
                    .await?;
            }
            _ => {
                return logged_err!(self.id; "unknown request type at {}|{}", inst_idx, cmd_idx)
            }
        }

        Ok(())
    }
}

#[async_trait]
impl GenericReplica for RepNothingReplica {
    fn new(
        id: ReplicaId,
        population: u8,
        api_addr: SocketAddr,
        conn_addrs: HashMap<ReplicaId, SocketAddr>,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
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
        if conn_addrs.len() != peer_addrs.len() {
            return logged_err!(
                id;
                "size of conn_addrs {} != size of peer_addrs {}",
                conn_addrs.len(), peer_addrs.len()
            );
        }

        let config = parsed_config!(config_str => ReplicaConfigRepNothing;
                                    batch_interval_us, backer_path, base_chan_cap,
                                    api_chan_cap)?;
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
            _population: population,
            config,
            api_addr,
            _conn_addrs: conn_addrs,
            _peer_addrs: peer_addrs,
            external_api: ExternalApi::new(id),
            state_machine: StateMachine::new(id),
            storage_hub: StorageHub::new(id),
            insts: vec![],
            log_offset: 0,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        self.state_machine
            .setup(self.config.api_chan_cap, self.config.api_chan_cap)
            .await?;

        self.storage_hub
            .setup(
                Path::new(&self.config.backer_path),
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;

        self.external_api
            .setup(
                self.api_addr,
                Duration::from_micros(self.config.batch_interval_us),
                self.config.api_chan_cap,
                self.config.api_chan_cap,
            )
            .await?;

        Ok(())
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.get_req_batch() => {
                    if let Err(e) = req_batch {
                        pf_error!(self.id; "error getting req batch: {}", e);
                        continue;
                    }
                    let req_batch = req_batch.unwrap();
                    if let Err(e) = self.handle_req_batch(req_batch).await {
                        pf_error!(self.id; "error handling req batch: {}", e);
                    }
                },

                // durable logging result
                log_result = self.storage_hub.get_result() => {
                    if let Err(e) = log_result {
                        pf_error!(self.id; "error getting log result: {}", e);
                        continue;
                    }
                    let (action_id, log_result) = log_result.unwrap();
                    if let Err(e) = self.handle_log_result(action_id, log_result).await {
                        pf_error!(self.id; "error handling log result {}: {}", action_id, e);
                    }
                },

                // state machine execution result
                cmd_result = self.state_machine.get_result() => {
                    if let Err(e) = cmd_result {
                        pf_error!(self.id; "error getting cmd result: {}", e);
                        continue;
                    }
                    let (cmd_id, cmd_result) = cmd_result.unwrap();
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result).await {
                        pf_error!(self.id; "error handling cmd result {}: {}", cmd_id, e);
                    }
                },
            }
        }
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
pub struct RepNothingClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigRepNothing,

    /// Stubs for communicating with the service.
    stubs: Option<(ClientSendStub, ClientRecvStub)>,
}

#[async_trait]
impl GenericEndpoint for RepNothingClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.is_empty() {
            return logged_err!(id; "empty servers list");
        }

        let config = parsed_config!(config_str => ClientConfigRepNothing;
                                    server_id)?;
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
            stubs: None,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        let api_stub = ClientApiStub::new(self.id);
        api_stub
            .connect(self.servers[&self.config.server_id])
            .await
            .map(|stubs| {
                self.stubs = Some(stubs);
            })
    }

    fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError> {
        match self.stubs {
            Some((ref mut send_stub, _)) => Ok(send_stub.send_req(req)?),
            None => logged_err!(self.id; "client is not set up"),
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        match self.stubs {
            Some((_, ref mut recv_stub)) => recv_stub.recv_reply().await,
            None => logged_err!(self.id; "client is not set up"),
        }
    }
}
