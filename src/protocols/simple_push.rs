//! Replication protocol: simple push.
//!
//! Immediately logs given command and pushes the command to some other peer
//! replicas. Upon receiving acknowledgement from all peers, executes the
//! command on the state machine and replies.

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
pub struct ReplicaConfigSimplePush {
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
impl Default for ReplicaConfigSimplePush {
    fn default() -> Self {
        ReplicaConfigSimplePush {
            batch_interval_us: 1000,
            backer_path: "/tmp/summerset.simple_push.wal".into(),
            rep_degree: 2,
            base_chan_cap: 1000,
            api_chan_cap: 10000,
        }
    }
}

/// Log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum LogEntry {
    FromClient {
        reqs: Vec<(ClientId, ApiRequest)>,
    },
    PeerPushed {
        peer: ReplicaId,
        reqs: Vec<(ClientId, ApiRequest)>,
    },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PushMsg {
    Push {
        src_inst_idx: usize,
        reqs: Vec<(ClientId, ApiRequest)>,
    },
    PushReply {
        src_inst_idx: usize,
        num_reqs: usize,
    },
}

/// In-memory instance containing a commands batch.
struct Instance {
    reqs: Vec<(ClientId, ApiRequest)>,
    durable: bool,
    pending_peers: ReplicaMap,
    execed: Vec<bool>,
    from_peer: Option<(ReplicaId, usize)>, // peer ID, peer inst_idx
}

/// SimplePush server replica module.
pub struct SimplePushReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    population: u8,

    /// Address string for peer-to-peer connections.
    smr_addr: SocketAddr,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Configuraiton parameters struct.
    config: ReplicaConfigSimplePush,

    /// Map from peer replica ID -> address.
    peer_addrs: HashMap<ReplicaId, SocketAddr>,

    /// ExternalApi module.
    external_api: Option<ExternalApi>,

    /// StateMachine module.
    state_machine: Option<StateMachine>,

    /// StorageHub module.
    storage_hub: Option<StorageHub<LogEntry>>,

    /// TransportHub module.
    transport_hub: Option<TransportHub<PushMsg>>,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Current durable log file offset.
    log_offset: usize,
}

impl SimplePushReplica {
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
        if batch_size == 0 {
            return Ok(());
        }

        // target peers to push to
        let mut target = ReplicaMap::new(self.population, false)?;
        let mut peer_cnt = 0;
        for peer in 0..self.population {
            if peer_cnt == self.config.rep_degree {
                break;
            }
            if peer == self.id {
                continue;
            }
            target.set(peer, true)?;
            peer_cnt += 1;
        }

        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            pending_peers: target.clone(),
            execed: vec![false; batch_size],
            from_peer: None,
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst); // TODO: snapshotting & garbage collection

        // submit log action to make this instance durable
        let log_entry = LogEntry::FromClient {
            reqs: req_batch.clone(),
        };
        self.storage_hub
            .as_mut()
            .unwrap()
            .submit_action(
                inst_idx as u64,
                LogAction::Append {
                    entry: log_entry,
                    offset: self.log_offset,
                },
            )
            .await?;

        // send push message to chosen peers
        self.transport_hub
            .as_mut()
            .unwrap()
            .send_msg(
                PushMsg::Push {
                    src_inst_idx: inst_idx,
                    reqs: req_batch,
                },
                target,
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
            LogResult::Append { ok, offset } => {
                if !ok {
                    return logged_err!(self.id; "log action Append for {} failed: {}", inst_idx, offset);
                }
                assert!(offset >= self.log_offset);
                self.log_offset = offset;
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

        // if pushed peers have all replied, submit execution commands
        if inst.pending_peers.count() == 0 {
            for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
                match req {
                    ApiRequest::Req { cmd, .. } => {
                        self.state_machine
                            .as_mut()
                            .unwrap()
                            .submit_cmd(
                                Self::make_command_id(inst_idx, cmd_idx),
                                cmd.clone(),
                            )
                            .await?
                    }
                    _ => continue, // ignore other types of requests
                }
            }
        }

        // if this instance was pushed from a peer, reply to that peer
        if let Some((peer, src_inst_idx)) = inst.from_peer {
            assert!(inst.pending_peers.count() == 0);
            let mut target = ReplicaMap::new(self.population, false)?;
            target.set(peer, true)?;
            self.transport_hub
                .as_mut()
                .unwrap()
                .send_msg(
                    PushMsg::PushReply {
                        src_inst_idx,
                        num_reqs: inst.reqs.len(),
                    },
                    target,
                )
                .await?;
        }

        Ok(())
    }

    /// Handler of push message from peer.
    async fn handle_push_msg(
        &mut self,
        peer: ReplicaId,
        src_inst_idx: usize,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            pending_peers: ReplicaMap::new(self.population, false)?,
            execed: vec![false; req_batch.len()],
            from_peer: Some((peer, src_inst_idx)),
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst); // TODO: snapshotting & garbage collection

        // submit log action to make this instance durable
        let log_entry = LogEntry::PeerPushed {
            peer,
            reqs: req_batch.clone(),
        };
        self.storage_hub
            .as_mut()
            .unwrap()
            .submit_action(
                inst_idx as u64,
                LogAction::Append {
                    entry: log_entry,
                    offset: self.log_offset,
                },
            )
            .await?;

        Ok(())
    }

    /// Handler of push reply from peer.
    async fn handle_push_reply(
        &mut self,
        peer: ReplicaId,
        inst_idx: usize,
        num_reqs: usize,
    ) -> Result<(), SummersetError> {
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid src_inst_idx {} seen", inst_idx);
        }

        let inst = &mut self.insts[inst_idx];
        if inst.from_peer.is_some() {
            return logged_err!(self.id; "from_peer should not be set for {}", inst_idx);
        }
        if inst.pending_peers.count() == 0 {
            return logged_err!(self.id; "pending_peers already 0 for {}", inst_idx);
        }
        if !inst.pending_peers.get(peer)? {
            return logged_err!(self.id; "unexpected push reply from peer {} for {}",
                                        peer, inst_idx);
        }
        if num_reqs != inst.reqs.len() {
            return logged_err!(self.id; "num_reqs mismatch: expected {}, got {}",
                                        inst.reqs.len(), num_reqs);
        }
        inst.pending_peers.set(peer, false)?;

        // if pushed peers have all replied and the logging on myself has
        // completed as well, submit execution commands
        if inst.pending_peers.count() == 0 && inst.durable {
            for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
                match req {
                    ApiRequest::Req { cmd, .. } => {
                        self.state_machine
                            .as_mut()
                            .unwrap()
                            .submit_cmd(
                                Self::make_command_id(inst_idx, cmd_idx),
                                cmd.clone(),
                            )
                            .await?
                    }
                    _ => continue, // ignore other types of requests
                }
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
        if inst.pending_peers.count() > 0 {
            return logged_err!(self.id; "instance {} has pending peers", inst_idx);
        }
        inst.execed[cmd_idx] = true;

        // if this instance was directly from client, reply to the
        // corresponding client of this request
        if inst.from_peer.is_none() {
            let (client, req) = &inst.reqs[cmd_idx];
            match req {
                ApiRequest::Req { id: req_id, .. } => {
                    self.external_api
                        .as_mut()
                        .unwrap()
                        .send_reply(
                            ApiReply::Reply {
                                id: *req_id,
                                result: cmd_result,
                            },
                            *client,
                        )
                        .await?;
                }
                _ => {
                    return logged_err!(self.id; "unknown request type at {}|{}", inst_idx, cmd_idx)
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl GenericReplica for SimplePushReplica {
    fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
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
        if smr_addr == api_addr {
            return logged_err!(
                id;
                "smr_addr and api_addr are the same '{}'",
                smr_addr
            );
        }

        let config = parsed_config!(config_str => ReplicaConfigSimplePush;
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

        Ok(SimplePushReplica {
            id,
            population,
            smr_addr,
            api_addr,
            config,
            peer_addrs,
            external_api: None,
            state_machine: None,
            storage_hub: None,
            transport_hub: None,
            insts: vec![],
            log_offset: 0,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        let mut state_machine = StateMachine::new(self.id);
        state_machine
            .setup(self.config.api_chan_cap, self.config.api_chan_cap)
            .await?;
        self.state_machine = Some(state_machine);

        let mut storage_hub = StorageHub::new(self.id);
        storage_hub
            .setup(
                Path::new(&self.config.backer_path),
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;
        self.storage_hub = Some(storage_hub);

        let mut transport_hub = TransportHub::new(self.id, self.population);
        transport_hub
            .setup(
                self.smr_addr,
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;
        if !self.peer_addrs.is_empty() {
            transport_hub.group_connect(&self.peer_addrs).await?;
        }
        self.transport_hub = Some(transport_hub);

        let mut external_api = ExternalApi::new(self.id);
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

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.as_mut().unwrap().get_req_batch() => {
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
                log_result = self.storage_hub.as_mut().unwrap().get_result() => {
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
                msg = self.transport_hub.as_mut().unwrap().recv_msg() => {
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
                cmd_result = self.state_machine.as_mut().unwrap().get_result() => {
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
pub struct ClientConfigSimplePush {
    /// Which server to pick.
    pub server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigSimplePush {
    fn default() -> Self {
        ClientConfigSimplePush { server_id: 0 }
    }
}

/// SimplePush client-side module.
pub struct SimplePushClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigSimplePush,

    /// Stubs for communicating with the service.
    stubs: Option<(ClientSendStub, ClientRecvStub)>,
}

#[async_trait]
impl GenericClient for SimplePushClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.is_empty() {
            return logged_err!(id; "empty servers list");
        }

        let config = parsed_config!(config_str => ClientConfigSimplePush;
                                    server_id)?;
        if !servers.contains_key(&config.server_id) {
            return logged_err!(
                id;
                "server_id {} not found in servers",
                config.server_id
            );
        }

        Ok(SimplePushClient {
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
                ()
            })
    }

    async fn request(
        &mut self,
        req: ApiRequest,
    ) -> Result<ApiReply, SummersetError> {
        match &mut self.stubs {
            Some((ref mut send_stub, ref mut recv_stub)) => {
                send_stub.send_req(req).await?;
                recv_stub.recv_reply().await
            }
            None => logged_err!(self.id; "RepNothing client is not set up."),
        }
    }
}
