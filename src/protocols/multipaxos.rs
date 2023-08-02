//! Replication protocol: MultiPaxos.
//!
//! Multi-decree Paxos protocol. See:
//!   - https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf
//!   - https://dl.acm.org/doi/pdf/10.1145/1281100.1281103
//!   - https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf
//!   - https://github.com/josehu07/learn-tla/tree/main/Dr.-TLA%2B-selected/multipaxos_practical

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::{SummersetError, ReplicaMap};
use crate::server::{
    ReplicaId, StateMachine, CommandResult, CommandId, ExternalApi, ApiRequest,
    ApiReply, StorageHub, LogAction, LogResult, LogActionId, TransportHub,
    GenericReplica,
};
use crate::client::{
    ClientId, ClientApiStub, ClientSendStub, ClientRecvStub, GenericClient,
};

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigMultiPaxos {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

    /// Path to backing file.
    pub backer_path: String,

    /// Number of peer servers to push each command to.
    pub rep_degree: u8,

    /// Base capacity for most channels.
    pub base_chan_cap: usize,

    /// Capacity for req/reply channels.
    pub api_chan_cap: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigMultiPaxos {
    fn default() -> Self {
        ReplicaConfigMultiPaxos {
            batch_interval_us: 1000,
            backer_path: "/tmp/summerset.multipaxos.wal".into(),
            rep_degree: 2,
            base_chan_cap: 1000,
            api_chan_cap: 10000,
        }
    }
}

/// Stable storage log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum LogEntry {}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PeerMsg {}

/// In-memory instance containing a commands batch.
struct Instance {
    reqs: Vec<(ClientId, ApiRequest)>,
}

/// MultiPaxos server replica module.
pub struct MultiPaxosReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    population: u8,

    /// Address string for peer-to-peer connections.
    smr_addr: SocketAddr,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Configuraiton parameters struct.
    config: ReplicaConfigMultiPaxos,

    /// Map from peer replica ID -> address.
    peer_addrs: HashMap<ReplicaId, SocketAddr>,

    /// ExternalApi module.
    external_api: ExternalApi,

    /// StateMachine module.
    state_machine: StateMachine,

    /// StorageHub module.
    storage_hub: StorageHub<LogEntry>,

    /// TransportHub module.
    transport_hub: TransportHub<PeerMsg>,

    /// Do I think I am the leader?
    is_leader: bool,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Current durable log file offset.
    log_offset: usize,
}

impl MultiPaxosReplica {
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

    /// Helper for replying back to client for a normal command.
    async fn reply_to_client(
        &mut self,
        client: ClientId,
        req: ApiRequest,
        result: Option<CommandResult>,
        redirect: Option<ReplicaId>,
    ) -> Result<(), SummersetError> {
        if let ApiRequest::Req { id: req_id, .. } = req {
            self.external_api
                .send_reply(
                    ApiReply::Reply {
                        id: req_id,
                        result,
                        redirect,
                    },
                    client,
                )
                .await
        } else {
            logged_err!(self.id; "not a normal command request to reply to")
        }
    }

    /// Handler of client request batch chan recv.
    async fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        assert!(batch_size > 0);

        // if I'm not a leader, ignore client requests
        if !self.is_leader {
            for (client, req) in req_batch {
                self.reply_to_client(
                    client,
                    req,
                    None,
                    // tell the client to try on the next replica
                    Some((self.id + 1) % self.population),
                )
                .await?;
            }
            return Ok(());
        }

        Ok(())
    }

    /// Handler of durable logging result chan recv.
    async fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<LogEntry>,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Handler of push message from peer.
    async fn handle_push_msg(
        &mut self,
        peer: ReplicaId,
        src_inst_idx: usize,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Handler of push reply from peer.
    async fn handle_push_reply(
        &mut self,
        peer: ReplicaId,
        inst_idx: usize,
        num_reqs: usize,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Handler of state machine exec result chan recv.
    async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        Ok(())
    }
}

#[async_trait]
impl GenericReplica for MultiPaxosReplica {
    fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if population < 3 {
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

        let config = parsed_config!(config_str => ReplicaConfigMultiPaxos;
                                    batch_interval_us, backer_path, rep_degree,
                                    base_chan_cap, api_chan_cap)?;
        if config.batch_interval_us == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_us '{}'",
                config.batch_interval_us
            );
        }
        if config.rep_degree >= population {
            return logged_err!(
                id;
                "invalid config.rep_degree {}",
                config.rep_degree
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

        Ok(MultiPaxosReplica {
            id,
            population,
            smr_addr,
            api_addr,
            config,
            peer_addrs,
            external_api: ExternalApi::new(id),
            state_machine: StateMachine::new(id),
            storage_hub: StorageHub::new(id),
            transport_hub: TransportHub::new(id, population),
            is_leader: false,
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

        self.transport_hub
            .setup(
                self.smr_addr,
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;
        if !self.peer_addrs.is_empty() {
            self.transport_hub.group_connect(&self.peer_addrs).await?;
        }

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
        // TODO: proper leader election
        if self.id == 0 {
            self.is_leader = true;
        }

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

                // message from peer
                msg = self.transport_hub.recv_msg() => {
                    if let Err(e) = msg {
                        pf_error!(self.id; "error receiving peer msg: {}", e);
                        continue;
                    }
                    let (peer, msg) = msg.unwrap();
                    match msg {
                        PushMsg::Push { src_inst_idx, reqs } => {
                            if let Err(e) = self.handle_push_msg(peer, src_inst_idx, reqs).await {
                                pf_error!(self.id; "error handling peer msg: {}", e);
                            }
                        },
                        PushMsg::PushReply { src_inst_idx, num_reqs } => {
                            if let Err(e) = self.handle_push_reply(peer, src_inst_idx, num_reqs).await {
                                pf_error!(self.id; "error handling peer reply: {}", e);
                            }
                        },
                    }
                }

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
pub struct ClientConfigMultiPaxos {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigMultiPaxos {
    fn default() -> Self {
        ClientConfigMultiPaxos { init_server_id: 0 }
    }
}

/// MultiPaxos client-side module.
pub struct MultiPaxosClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigMultiPaxos,

    /// CLient API stub for creating new connections.
    api_stub: ClientApiStub,

    /// Connection stubs for communicating with the current chosen server.
    conn_stubs: Option<(ClientSendStub, ClientRecvStub)>,
}

#[async_trait]
impl GenericClient for MultiPaxosClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.is_empty() {
            return logged_err!(id; "empty servers list");
        }

        let config = parsed_config!(config_str => ClientConfigMultiPaxos;
                                    init_server_id)?;
        if !servers.contains_key(&config.init_server_id) {
            return logged_err!(
                id;
                "init_server_id {} not found in servers",
                config.init_server_id
            );
        }

        let api_stub = ClientApiStub::new(id);

        Ok(MultiPaxosClient {
            id,
            servers,
            config,
            api_stub,
            conn_stubs: None,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        self.api_stub
            .connect(self.servers[&self.config.init_server_id])
            .await
            .map(|stubs| {
                self.conn_stubs = Some(stubs);
            })
    }

    async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        match self.conn_stubs {
            Some((ref mut send_stub, _)) => {
                send_stub.send_req(req).await?;
                Ok(())
            }
            None => logged_err!(self.id; "client is not set up"),
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        match self.conn_stubs {
            Some((ref mut send_stub, ref mut recv_stub)) => {
                let reply = recv_stub.recv_reply().await?;

                if let ApiReply::Reply {
                    result, redirect, ..
                } = reply
                {
                    // if the current server redirects me to a different server
                    if result.is_none() && redirect.is_some() {
                        let redirect_id = redirect.unwrap();
                        assert!((redirect_id as usize) < self.servers.len());
                        send_stub.send_req(ApiRequest::Leave).await?;
                        self.api_stub
                            .connect(self.servers[&redirect_id])
                            .await
                            .map(|stubs| {
                                self.conn_stubs = Some(stubs);
                            });
                    }
                }

                Ok(reply)
            }
            None => logged_err!(self.id; "client is not set up"),
        }
    }
}
