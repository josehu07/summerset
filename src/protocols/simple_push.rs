//! Replication protocol: simple push.
//!
//! Immediately logs given command and pushes the command to some other peer
//! replicas. Upon receiving acknowledgement from all peers, executes the
//! command on the state machine and replies.

use std::path::Path;
use std::net::SocketAddr;

use crate::utils::{SummersetError, Bitmap};
use crate::manager::{CtrlMsg, CtrlRequest, CtrlReply};
use crate::server::{
    ReplicaId, ControlHub, StateMachine, CommandResult, CommandId, ExternalApi,
    ApiRequest, ApiReply, StorageHub, LogAction, LogResult, LogActionId,
    TransportHub, GenericReplica,
};
use crate::client::{ClientId, ClientApiStub, ClientCtrlStub, GenericEndpoint};
use crate::protocols::SmrProtocol;

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;
use tokio::sync::watch;

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigSimplePush {
    /// Client request batching interval in millisecs.
    pub batch_interval_ms: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing file.
    pub backer_path: String,

    /// Number of peer servers to push each command to.
    pub rep_degree: u8,

    // Performance simulation params (all zeros means no perf simulation):
    pub perf_storage_a: u64,
    pub perf_storage_b: u64,
    pub perf_network_a: u64,
    pub perf_network_b: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigSimplePush {
    fn default() -> Self {
        ReplicaConfigSimplePush {
            batch_interval_ms: 10,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.simple_push.wal".into(),
            rep_degree: 2,
            perf_storage_a: 0,
            perf_storage_b: 0,
            perf_network_a: 0,
            perf_network_b: 0,
        }
    }
}

/// WAL log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
enum WalEntry {
    FromClient {
        reqs: Vec<(ClientId, ApiRequest)>,
    },
    PeerPushed {
        peer: ReplicaId,
        src_inst_idx: usize,
        reqs: Vec<(ClientId, ApiRequest)>,
    },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
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
    pending_peers: Bitmap,
    execed: Vec<bool>,
    from_peer: Option<(ReplicaId, usize)>, // peer ID, peer inst_idx
}

/// SimplePush server replica module.
pub struct SimplePushReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Configuration parameters struct.
    config: ReplicaConfigSimplePush,

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
    transport_hub: TransportHub<PushMsg>,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Current durable WAL log file offset.
    wal_offset: usize,
}

// SimplePushReplica common helpers
impl SimplePushReplica {
    /// Compose CommandId from instance index & command index within.
    #[inline]
    fn make_command_id(inst_idx: usize, cmd_idx: usize) -> CommandId {
        debug_assert!(inst_idx <= (u32::MAX as usize));
        debug_assert!(cmd_idx <= (u32::MAX as usize));
        ((inst_idx << 32) | cmd_idx) as CommandId
    }

    /// Decompose CommandId into instance index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let inst_idx = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (inst_idx, cmd_idx)
    }
}

// SimplePushReplica client requests entrance
impl SimplePushReplica {
    /// Handler of client request batch chan recv.
    fn handle_req_batch(
        &mut self,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);

        // target peers to push to
        let mut target = Bitmap::new(self.population, false);
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
        self.insts.push(inst);

        // submit log action to make this instance durable
        let wal_entry = WalEntry::FromClient {
            reqs: req_batch.clone(),
        };
        self.storage_hub.submit_action(
            inst_idx as LogActionId,
            LogAction::Append {
                entry: wal_entry,
                sync: true,
            },
        )?;

        // send push message to chosen peers
        self.transport_hub.bcast_msg(
            PushMsg::Push {
                src_inst_idx: inst_idx,
                reqs: req_batch,
            },
            Some(target),
        )?;

        Ok(())
    }
}

// SimplePushReplica durable WAL logging
impl SimplePushReplica {
    /// Handler of durable logging result chan recv.
    fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<WalEntry>,
    ) -> Result<(), SummersetError> {
        let inst_idx = action_id as usize;
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid log action ID {} seen", inst_idx);
        }

        match log_result {
            LogResult::Append { now_size } => {
                debug_assert!(now_size >= self.wal_offset);
                self.wal_offset = now_size;
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
                        self.state_machine.submit_cmd(
                            Self::make_command_id(inst_idx, cmd_idx),
                            cmd.clone(),
                        )?
                    }
                    _ => continue, // ignore other types of requests
                }
            }
        }

        // if this instance was pushed from a peer, reply to that peer
        if let Some((peer, src_inst_idx)) = inst.from_peer {
            debug_assert!(inst.pending_peers.count() == 0);
            self.transport_hub.send_msg(
                PushMsg::PushReply {
                    src_inst_idx,
                    num_reqs: inst.reqs.len(),
                },
                peer,
            )?;
        }

        Ok(())
    }
}

// SimplePushReplica peer-peer messages handling
impl SimplePushReplica {
    /// Handler of push message from peer.
    fn handle_push_msg(
        &mut self,
        peer: ReplicaId,
        src_inst_idx: usize,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            pending_peers: Bitmap::new(self.population, false),
            execed: vec![false; req_batch.len()],
            from_peer: Some((peer, src_inst_idx)),
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst);

        // submit log action to make this instance durable
        let wal_entry = WalEntry::PeerPushed {
            peer,
            src_inst_idx,
            reqs: req_batch.clone(),
        };
        self.storage_hub.submit_action(
            inst_idx as LogActionId,
            LogAction::Append {
                entry: wal_entry,
                sync: true,
            },
        )?;

        Ok(())
    }

    /// Handler of push reply from peer.
    fn handle_push_reply(
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
                        self.state_machine.submit_cmd(
                            Self::make_command_id(inst_idx, cmd_idx),
                            cmd.clone(),
                        )?
                    }
                    _ => continue, // ignore other types of requests
                }
            }
        }

        Ok(())
    }
}

// SimplePushReplica state machine execution
impl SimplePushReplica {
    /// Handler of state machine exec result chan recv.
    fn handle_cmd_result(
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
                    if self.external_api.has_client(*client) {
                        self.external_api.send_reply(
                            ApiReply::Reply {
                                id: *req_id,
                                result: Some(cmd_result),
                                redirect: None,
                            },
                            *client,
                        )?;
                    }
                }
                _ => {
                    return logged_err!(self.id; "unknown request type at {}|{}", inst_idx, cmd_idx)
                }
            }
        }

        Ok(())
    }
}

// SimplePushReplica control messages handling
impl SimplePushReplica {
    /// Handler of ResetState control message.
    async fn handle_ctrl_reset_state(
        &mut self,
        durable: bool,
    ) -> Result<(), SummersetError> {
        // send leave notification to peers and wait for their replies
        self.transport_hub.leave().await?;

        // send leave notification to manager and wait for its reply
        self.control_hub.send_ctrl(CtrlMsg::Leave)?;
        while self.control_hub.recv_ctrl().await? != CtrlMsg::LeaveReply {}

        // if `durable` is false, truncate backer file
        if !durable {
            // use 0 as a special log action ID here
            self.storage_hub
                .submit_action(0, LogAction::Truncate { offset: 0 })?;
            loop {
                let (action_id, log_result) =
                    self.storage_hub.get_result().await?;
                if action_id == 0 {
                    if log_result
                        != (LogResult::Truncate {
                            offset_ok: true,
                            now_size: 0,
                        })
                    {
                        return logged_err!(self.id; "failed to truncate log to 0");
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    /// Handler of Pause control message.
    fn handle_ctrl_pause(
        &mut self,
        paused: &mut bool,
    ) -> Result<(), SummersetError> {
        pf_warn!(self.id; "server got pause req");
        *paused = true;
        self.control_hub.send_ctrl(CtrlMsg::PauseReply)?;
        Ok(())
    }

    /// Handler of Resume control message.
    fn handle_ctrl_resume(
        &mut self,
        paused: &mut bool,
    ) -> Result<(), SummersetError> {
        pf_warn!(self.id; "server got resume req");
        *paused = false;
        self.control_hub.send_ctrl(CtrlMsg::ResumeReply)?;
        Ok(())
    }

    /// Synthesized handler of manager control messages. If ok, returns
    /// `Some(true)` if decides to terminate and reboot, `Some(false)` if
    /// decides to shutdown completely, and `None` if not terminating.
    async fn handle_ctrl_msg(
        &mut self,
        msg: CtrlMsg,
        paused: &mut bool,
    ) -> Result<Option<bool>, SummersetError> {
        match msg {
            CtrlMsg::ResetState { durable } => {
                self.handle_ctrl_reset_state(durable).await?;
                Ok(Some(true))
            }

            CtrlMsg::Pause => {
                self.handle_ctrl_pause(paused)?;
                Ok(None)
            }

            CtrlMsg::Resume => {
                self.handle_ctrl_resume(paused)?;
                Ok(None)
            }

            _ => Ok(None), // ignore all other types
        }
    }
}

// SimplePushReplica recovery from WAL log
impl SimplePushReplica {
    /// Recover state from durable storage WAL log.
    async fn recover_from_wal(&mut self) -> Result<(), SummersetError> {
        debug_assert_eq!(self.wal_offset, 0);
        loop {
            // using 0 as a special log action ID
            self.storage_hub.submit_action(
                0,
                LogAction::Read {
                    offset: self.wal_offset,
                },
            )?;
            let (_, log_result) = self.storage_hub.get_result().await?;

            match log_result {
                LogResult::Read {
                    entry: Some(entry),
                    end_offset,
                } => {
                    let (from_peer, reqs) = match entry {
                        WalEntry::FromClient { reqs } => (None, reqs),
                        WalEntry::PeerPushed {
                            peer,
                            src_inst_idx,
                            reqs,
                        } => (Some((peer, src_inst_idx)), reqs),
                    };
                    // execute all commands on state machine synchronously
                    for (_, req) in reqs.clone() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            // using 0 as a special command ID
                            self.state_machine.submit_cmd(0, cmd)?;
                            let _ = self.state_machine.get_result().await?;
                        }
                    }
                    // rebuild in-memory log entry
                    let num_reqs = reqs.len();
                    self.insts.push(Instance {
                        reqs,
                        durable: true,
                        pending_peers: Bitmap::new(self.population, false),
                        execed: vec![true; num_reqs],
                        from_peer,
                    });
                    // update log offset
                    self.wal_offset = end_offset;
                }
                LogResult::Read { entry: None, .. } => {
                    // end of log reached
                    break;
                }
                _ => {
                    return logged_err!(self.id; "unexpected log result type");
                }
            }
        }

        // do an extra Truncate to remove paritial entry at the end if any
        self.storage_hub.submit_action(
            0,
            LogAction::Truncate {
                offset: self.wal_offset,
            },
        )?;
        let (_, log_result) = self.storage_hub.get_result().await?;
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = log_result
        {
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type")
        }
    }
}

#[async_trait]
impl GenericReplica for SimplePushReplica {
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
        let config = parsed_config!(config_str => ReplicaConfigSimplePush;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, rep_degree,
                                    perf_storage_a, perf_storage_b,
                                    perf_network_a, perf_network_b)?;
        if config.batch_interval_ms == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_ms '{}'",
                config.batch_interval_ms
            );
        }

        // setup state machine module
        let state_machine = StateMachine::new_and_setup(id).await?;

        // setup storage hub module
        let storage_hub = StorageHub::new_and_setup(
            id,
            Path::new(&config.backer_path),
            if config.perf_storage_a == 0 && config.perf_storage_b == 0 {
                None
            } else {
                Some((config.perf_storage_a, config.perf_storage_b))
            },
        )
        .await?;

        // setup transport hub module
        let mut transport_hub = TransportHub::new_and_setup(
            id,
            population,
            p2p_addr,
            if config.perf_network_a == 0 && config.perf_network_b == 0 {
                None
            } else {
                Some((config.perf_network_a, config.perf_network_b))
            },
        )
        .await?;

        // ask for the list of peers to proactively connect to. Do this after
        // transport hub has been set up, so that I will be able to accept
        // later peer connections
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::SimplePush,
            api_addr,
            p2p_addr,
        })?;
        let to_peers = if let CtrlMsg::ConnectToPeers { to_peers, .. } =
            control_hub.recv_ctrl().await?
        {
            to_peers
        } else {
            return logged_err!(id; "unexpected ctrl msg type received");
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

        Ok(SimplePushReplica {
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
                        pf_error!(self.id; "error getting req batch: {}", e);
                        continue;
                    }
                    let req_batch = req_batch.unwrap();
                    if let Err(e) = self.handle_req_batch(req_batch) {
                        pf_error!(self.id; "error handling req batch: {}", e);
                    }
                },

                // durable logging result
                log_result = self.storage_hub.get_result(), if !paused => {
                    if let Err(e) = log_result {
                        pf_error!(self.id; "error getting log result: {}", e);
                        continue;
                    }
                    let (action_id, log_result) = log_result.unwrap();
                    if let Err(e) = self.handle_log_result(action_id, log_result) {
                        pf_error!(self.id; "error handling log result {}: {}", action_id, e);
                    }
                },

                // message from peer
                msg = self.transport_hub.recv_msg(), if !paused => {
                    if let Err(_e) = msg {
                        // NOTE: commented out to prevent console lags
                        // during benchmarking
                        // pf_error!(self.id; "error receiving peer msg: {}", e);
                        continue;
                    }
                    let (peer, msg) = msg.unwrap();
                    match msg {
                        PushMsg::Push { src_inst_idx, reqs } => {
                            if let Err(e) = self.handle_push_msg(peer, src_inst_idx, reqs) {
                                pf_error!(self.id; "error handling peer msg: {}", e);
                            }
                        },
                        PushMsg::PushReply { src_inst_idx, num_reqs } => {
                            if let Err(e) = self.handle_push_reply(peer, src_inst_idx, num_reqs) {
                                pf_error!(self.id; "error handling peer reply: {}", e);
                            }
                        },
                    }

                },

                // state machine execution result
                cmd_result = self.state_machine.get_result(), if !paused => {
                    if let Err(e) = cmd_result {
                        pf_error!(self.id; "error getting cmd result: {}", e);
                        continue;
                    }
                    let (cmd_id, cmd_result) = cmd_result.unwrap();
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result) {
                        pf_error!(self.id; "error handling cmd result {}: {}", cmd_id, e);
                    }
                },

                // manager control message
                ctrl_msg = self.control_hub.recv_ctrl() => {
                    if let Err(e) = ctrl_msg {
                        pf_error!(self.id; "error getting ctrl msg: {}", e);
                        continue;
                    }
                    let ctrl_msg = ctrl_msg.unwrap();
                    match self.handle_ctrl_msg(ctrl_msg, &mut paused).await {
                        Ok(terminate) => {
                            if let Some(restart) = terminate {
                                pf_warn!(
                                    self.id;
                                    "server got {} req",
                                    if restart { "restart" } else { "shutdown" });
                                return Ok(restart);
                            }
                        },
                        Err(e) => {
                            pf_error!(self.id; "error handling ctrl msg: {}", e);
                        }
                    }
                },

                // receiving termination signal
                _ = rx_term.changed() => {
                    pf_warn!(self.id; "server caught termination signal");
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

    /// Configuration parameters struct.
    config: ClientConfigSimplePush,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stub for communicating with the current server.
    api_stub: Option<ClientApiStub>,

    /// Base bind address for sockets connecting to servers.
    api_bind_base: SocketAddr,
}

#[async_trait]
impl GenericEndpoint for SimplePushClient {
    async fn new_and_setup(
        ctrl_base: SocketAddr,
        api_bind_base: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("c"; "connecting to manager '{}'...", manager);
        let ctrl_stub =
            ClientCtrlStub::new_by_connect(ctrl_base, manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigSimplePush;
                                    server_id)?;

        Ok(SimplePushClient {
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
            return logged_err!(self.id; "reconnecting without leaving");
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
                pf_debug!(self.id; "connecting to server {} '{}'...",
                                   self.config.server_id,
                                   servers_info[&self.config.server_id].api_addr);
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
            _ => logged_err!(self.id; "unexpected reply type received"),
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
            pf_debug!(self.id; "left current server connection");
            api_stub.forget();
        }

        // if permanently leaving, send leave notification to the manager
        if permanent {
            let mut sent =
                self.ctrl_stub.send_req(Some(&CtrlRequest::Leave))?;
            while !sent {
                sent = self.ctrl_stub.send_req(None)?;
            }

            while self.ctrl_stub.recv_reply().await? != CtrlReply::Leave {}
            pf_debug!(self.id; "left manager connection");
        }

        Ok(())
    }

    fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError> {
        match self.api_stub {
            Some(ref mut api_stub) => api_stub.send_req(req),
            None => Err(SummersetError("client not set up".into())),
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        match self.api_stub {
            Some(ref mut api_stub) => api_stub.recv_reply().await,
            None => Err(SummersetError("client not set up".into())),
        }
    }

    fn id(&self) -> ClientId {
        self.id
    }

    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        &mut self.ctrl_stub
    }
}
