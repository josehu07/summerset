//! Replication protocol: replicate nothing.
//!
//! Immediately logs given command and executes given command on the state
//! machine upon receiving a client command, and does nothing else.

use std::path::Path;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::manager::{CtrlMsg, CtrlRequest, CtrlReply};
use crate::server::{
    ReplicaId, ControlHub, StateMachine, CommandResult, CommandId, ExternalApi,
    ApiRequest, ApiReply, StorageHub, LogAction, LogResult, LogActionId,
    GenericReplica,
};
use crate::client::{ClientId, ClientApiStub, ClientCtrlStub, GenericEndpoint};
use crate::protocols::SmrProtocol;

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;
use tokio::sync::mpsc;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigRepNothing {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

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
            batch_interval_us: 1000,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.rep_nothing.wal".into(),
            logger_sync: false,
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
    fn handle_req_batch(
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
        self.storage_hub.submit_action(
            inst_idx as LogActionId,
            LogAction::Append {
                entry: log_entry,
                sync: self.config.logger_sync,
            },
        )?;

        Ok(())
    }

    /// Handler of durable logging result chan recv.
    fn handle_log_result(
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
                ApiRequest::Req { cmd, .. } => self.state_machine.submit_cmd(
                    Self::make_command_id(inst_idx, cmd_idx),
                    cmd.clone(),
                )?,
                _ => continue, // ignore other types of requests
            }
        }

        Ok(())
    }

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
        inst.execed[cmd_idx] = true;

        // reply to the corresponding client of this request
        let (client, req) = &inst.reqs[cmd_idx];
        match req {
            ApiRequest::Req { id: req_id, .. } => {
                self.external_api.send_reply(
                    ApiReply::Reply {
                        id: *req_id,
                        result: Some(cmd_result),
                        redirect: None,
                    },
                    *client,
                )?;
            }
            _ => {
                return logged_err!(self.id; "unknown request type at {}|{}", inst_idx, cmd_idx)
            }
        }

        Ok(())
    }

    /// Synthesized handler of manager control messages.
    fn handle_ctrl_msg(&mut self, _msg: CtrlMsg) -> Result<(), SummersetError> {
        // TODO: fill this when more control message types added
        Ok(())
    }
}

#[async_trait]
impl GenericReplica for RepNothingReplica {
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let config = parsed_config!(config_str => ReplicaConfigRepNothing;
                                    batch_interval_us, max_batch_size,
                                    backer_path, logger_sync)?;
        // connect to the cluster manager and get assigned a server ID
        let mut control_hub = ControlHub::new_and_setup(manager).await?;
        let id = control_hub.me;

        if config.batch_interval_us == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_us '{}'",
                config.batch_interval_us
            );
        }

        // tell the manager tha I have joined
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::RepNothing,
            api_addr,
            p2p_addr,
        })?;
        control_hub.recv_ctrl().await?;

        let state_machine = StateMachine::new_and_setup(id).await?;

        let storage_hub =
            StorageHub::new_and_setup(id, Path::new(&config.backer_path))
                .await?;

        // TransportHub is not needed in RepNothing

        let external_api = ExternalApi::new_and_setup(
            id,
            api_addr,
            Duration::from_micros(config.batch_interval_us),
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
            log_offset: 0,
        })
    }

    async fn run(&mut self) -> Result<bool, SummersetError> {
        // set up termination signals handler
        let (tx_term, mut rx_term) = mpsc::unbounded_channel();
        ctrlc::set_handler(move || {
            if let Err(e) = tx_term.send(true) {
                pf_error!("s"; "error sending to term channel: {}", e);
            }
        })?;

        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.get_req_batch() => {
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
                log_result = self.storage_hub.get_result() => {
                    if let Err(e) = log_result {
                        pf_error!(self.id; "error getting log result: {}", e);
                        continue;
                    }
                    let (action_id, log_result) = log_result.unwrap();
                    if let Err(e) = self.handle_log_result(action_id, log_result) {
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
                    if let Err(e) = self.handle_ctrl_msg(ctrl_msg) {
                        pf_error!(self.id; "error handling ctrl msg: {}", e);
                    }
                },

                // receiving termination signal
                _ = rx_term.recv() => {
                    pf_warn!(self.id; "server caught termination signal");
                    return Ok(false);
                }
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

    /// Address of the cluster manager oracle.
    manager: SocketAddr,

    /// Configuration parameters struct.
    config: ClientConfigRepNothing,

    /// Control API stub to the cluster manager.
    ctrl_stub: Option<ClientCtrlStub>,

    /// API stubs for communicating with servers.
    api_stub: Option<ClientApiStub>,
}

#[async_trait]
impl GenericEndpoint for RepNothingClient {
    fn new(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let config = parsed_config!(config_str => ClientConfigRepNothing;
                                    server_id)?;

        Ok(RepNothingClient {
            id: 255, // nil at this time
            manager,
            config,
            ctrl_stub: None,
            api_stub: None,
        })
    }

    async fn connect(&mut self) -> Result<ClientId, SummersetError> {
        // disallow reconnection without leaving
        if self.api_stub.is_some() {
            return logged_err!(self.id; "reconnecting without leaving");
        }

        // if ctrl_stubs not established yet, connect to the manager
        if self.ctrl_stub.is_none() {
            let ctrl_stub =
                ClientCtrlStub::new_by_connect(self.manager).await?;
            self.id = ctrl_stub.id;
            self.ctrl_stub = Some(ctrl_stub);
        }
        let ctrl_stub = self.ctrl_stub.as_mut().unwrap();

        // ask the manager about the list of active servers
        let mut sent = ctrl_stub.send_req(Some(&CtrlRequest::QueryInfo))?;
        while !sent {
            sent = ctrl_stub.send_req(None)?;
        }

        let reply = ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::QueryInfo { servers } => {
                // connect to the one with server ID in config
                let api_stub = ClientApiStub::new_by_connect(
                    self.id,
                    servers[&self.config.server_id],
                )
                .await?;
                self.api_stub = Some(api_stub);
                Ok(self.id)
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

            let reply = api_stub.recv_reply().await?;
            match reply {
                ApiReply::Leave => {
                    pf_info!(self.id; "left current server connection");
                    api_stub.forget();
                }
                _ => {
                    return logged_err!(self.id; "unexpected reply type received");
                }
            }
        }

        // if permanently leaving, send leave notification to the manager
        if permanent {
            // disallow multiple permanent leaving
            if self.ctrl_stub.is_none() {
                return logged_err!(self.id; "repeated permanent leaving");
            }

            if let Some(mut ctrl_stub) = self.ctrl_stub.take() {
                let mut sent = ctrl_stub.send_req(Some(&CtrlRequest::Leave))?;
                while !sent {
                    sent = ctrl_stub.send_req(None)?;
                }

                let reply = ctrl_stub.recv_reply().await?;
                match reply {
                    CtrlReply::Leave => {
                        pf_info!(self.id; "left current manager connection");
                        ctrl_stub.forget();
                    }
                    _ => {
                        return logged_err!(self.id; "unexpected reply type received");
                    }
                }
            }
        }

        Ok(())
    }

    fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError> {
        match self.api_stub {
            Some(ref mut api_stub) => api_stub.send_req(req),
            None => logged_err!(self.id; "client is not set up"),
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        match self.api_stub {
            Some(ref mut api_stub) => api_stub.recv_reply().await,
            None => logged_err!(self.id; "client is not set up"),
        }
    }
}
