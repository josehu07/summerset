//! Replication protocol: CRaft (Coded-Raft).
//!
//! Raft with erasure coding and fall-back mechanism. References:
//!   - <https://www.usenix.org/conference/fast20/presentation/wang-zizhong>

mod control;
mod durability;
mod execution;
mod leadership;
mod messages;
mod recovery;
mod request;
mod snapshot;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, CommandId, ControlHub, ExternalApi, GenericReplica,
    LogActionId, ReplicaId, StateMachine, StorageHub, TransportHub,
};
use crate::utils::{Bitmap, RSCodeword, SummersetError, Timer};

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::time::{self, Duration, Interval, MissedTickBehavior};

use reed_solomon_erasure::galois_8::ReedSolomon;

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigCRaft {
    /// Client request batching interval in millisecs.
    pub batch_interval_ms: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing log file.
    pub backer_path: String,

    /// Whether to call `fsync()`/`fdatasync()` on logger.
    pub logger_sync: bool,

    /// Min timeout of not hearing any heartbeat from leader in millisecs.
    pub hb_hear_timeout_min: u64,
    /// Max timeout of not hearing any heartbeat from leader in millisecs.
    pub hb_hear_timeout_max: u64,

    /// Interval of leader sending AppendEntries heartbeats to followers.
    pub hb_send_interval_ms: u64,

    /// Disable heartbeat timer (to force a deterministic leader during tests).
    pub disable_hb_timer: bool,

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Fault-tolerance level.
    pub fault_tolerance: u8,

    /// Maximum chunk size of any bulk of messages.
    pub msg_chunk_size: usize,

    /// Simulate local read lease implementation?
    // TODO: actual read lease impl later? (won't affect anything about
    // evalutaion results though)
    pub sim_read_lease: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigCRaft {
    fn default() -> Self {
        ReplicaConfigCRaft {
            batch_interval_ms: 10,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.craft.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 1500,
            hb_hear_timeout_max: 2000,
            hb_send_interval_ms: 20,
            disable_hb_timer: false,
            snapshot_path: "/tmp/summerset.craft.snap".into(),
            snapshot_interval_s: 0,
            fault_tolerance: 0,
            msg_chunk_size: 10,
            sim_read_lease: false,
        }
    }
}

/// Term number type, defined for better code readability.
pub(crate) type Term = u64;

/// Request batch type (i.e., the "command" in an entry).
///
/// NOTE: the originally presented Raft algorithm does not explicitly mention
/// batching, but instead hides it with the heartbeats: every AppendEntries RPC
/// from the leader basically batches all commands it has received since the
/// last sent heartbeat. Here, to make this implementation more comparable to
/// MultiPaxos, we trigger batching also explicitly.
pub(crate) type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// In-mem + persistent entry of log, containing a term and a (possibly
/// partial) commands batch.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) struct LogEntry {
    /// Term number.
    term: Term,

    /// Shards of a batch of client requests.
    reqs_cw: RSCodeword<ReqBatch>,

    /// True if from external client, else false.
    external: bool,

    /// Offset in durable log file of this entry. This field is not maintained
    /// in durable storage itself, where it is typically 0. It is maintained
    /// only in the in-memory log.
    log_offset: usize,
}

/// Stable storage log entry type.
///
/// NOTE: Raft makes the persistent log exactly mirror the in-memory log, so
/// the backer file is not a WAL log in runtime operation; it might get
/// overwritten, etc.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum DurEntry {
    /// Durable metadata.
    Metadata {
        curr_term: Term,
        voted_for: Option<ReplicaId>,
    },

    /// Log entry mirroring in-mem log.
    LogEntry { entry: LogEntry },
}

/// Snapshot file entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum SnapEntry {
    /// Necessary slot indices to remember.
    SlotInfo {
        /// First entry at the start of file: number of log entries covered
        /// by this snapshot file == the start slot index of remaining log.
        start_slot: usize,
    },

    /// Set of key-value pairs to apply to the state.
    KVPairSet { pairs: HashMap<String, String> },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum PeerMsg {
    /// AppendEntries from leader to followers.
    AppendEntries {
        term: Term,
        prev_slot: usize,
        prev_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: usize,
        /// For conservative snapshotting purpose.
        last_snap: usize,
    },

    /// AppendEntries reply from follower to leader.
    AppendEntriesReply {
        term: Term,
        /// For correct tracking of which AppendEntries this reply is for.
        end_slot: usize,
        /// If success, `None`; otherwise, contains a pair of the conflicting
        /// entry's term and my first index for that term.
        conflict: Option<(Term, usize)>,
    },

    /// RequestVote from leader to followers.
    RequestVote {
        term: Term,
        last_slot: usize,
        last_term: Term,
    },

    /// RequestVote reply from follower to leader.
    RequestVoteReply { term: Term, granted: bool },

    /// Reconstruction read from new leader to followers.
    Reconstruct { slots: Vec<(usize, Term)> },

    /// Reconstruction read reply from follower to leader.
    ReconstructReply {
        /// Map from slot -> req batch shards data the follower has.
        slots_data: HashMap<usize, RSCodeword<ReqBatch>>,
    },
}

/// Replica role type.
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize,
)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

/// CRaft server replica module.
pub(crate) struct CRaftReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Majority quorum size.
    majority: u8,

    /// True if we are in full-copy replication fallback mode.
    // NOTE: works that follow CRaft proposed more gradual fallback mechanisms,
    //       but for the sake of CRaft, a boolean flag is enough.
    full_copy_mode: bool,

    /// Configuration parameters struct.
    config: ReplicaConfigCRaft,

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
    storage_hub: StorageHub<DurEntry>,

    /// StorageHub module for the snapshot file.
    snapshot_hub: StorageHub<SnapEntry>,

    /// TransportHub module.
    transport_hub: TransportHub<PeerMsg>,

    /// Which role am I in right now?
    role: Role,

    /// Who do I think is the effective leader of the cluster right now?
    leader: Option<ReplicaId>,

    /// Timer for hearing heartbeat from leader.
    hb_hear_timer: Timer,

    /// Interval for sending heartbeat to followers.
    hb_send_interval: Interval,

    /// Heartbeat reply counters for approximate detection of follower health.
    /// Tuple of (#hb_replied, #hb_replied seen at last send, repetition).
    hb_reply_cnts: HashMap<ReplicaId, (u64, u64, u8)>,

    /// Approximate health status tracking of peer replicas.
    peer_alive: Bitmap,

    /// Latest term seen.
    curr_term: Term,

    /// Candidate ID that I voted for in current term.
    voted_for: Option<ReplicaId>,

    /// Replica IDs that voted for me in current election.
    votes_granted: HashSet<ReplicaId>,

    /// In-memory log of entries. Slot 0 is a dummy entry to make indexing happy.
    log: Vec<LogEntry>,

    /// Start slot index of in-mem log after latest snapshot.
    start_slot: usize,

    /// Timer for taking a new autonomous snapshot.
    snapshot_interval: Interval,

    /// Slot index of highest log entry known to be committed.
    last_commit: usize,

    /// Slot index of highest log entry applied to state machine.
    last_exec: usize,

    /// Slot index of highest log entry for which a Reconstruct has been sent.
    /// This is a soft state and affects nothing about correctness.
    last_recon: usize,

    /// For each server, index of the next log entry to send.
    next_slot: HashMap<ReplicaId, usize>,

    /// For each server, index of the next log entry to try to send for an
    /// AppendEntries message. This is added due to the asynchronous nature
    /// of Summerset's implementation.
    /// It is always true that next_slot[r] <= try_next_slot[r]
    try_next_slot: HashMap<ReplicaId, usize>,

    /// For each server, index of the highest log entry known to be replicated.
    match_slot: HashMap<ReplicaId, usize>,

    /// Slot index up to which it is safe to take snapshot.
    /// NOTE: we are taking a conservative approach here that a snapshot
    /// covering an entry can be taken only when all servers have durably
    /// committed that entry.
    last_snap: usize,

    /// Current durable log file end offset.
    log_offset: usize,

    /// Current durable log end of offset of metadata.
    log_meta_end: usize,

    /// Current durable snapshot file offset.
    snap_offset: usize,

    /// Fixed Reed-Solomon coder.
    rs_coder: ReedSolomon,
}

// CRaftReplica common helpers
impl CRaftReplica {
    /// Compose LogActionId from (slot, end_slot) pair & entry type.
    /// Uses the `Role` enum type to represent differnet entry types.
    #[inline]
    fn make_log_action_id(
        slot: usize,
        slot_e: usize,
        entry_type: Role,
    ) -> LogActionId {
        let type_num = match entry_type {
            Role::Follower => 1,
            Role::Leader => 2,
            _ => panic!("unknown log entry type {:?}", entry_type),
        };
        ((slot << 33) | (slot_e << 2) | type_num) as LogActionId
    }

    /// Decompose LogActionId into (slot, end_slot) pair & entry type.
    #[inline]
    fn split_log_action_id(log_action_id: LogActionId) -> (usize, usize, Role) {
        let slot = (log_action_id >> 33) as usize;
        let slot_e = ((log_action_id & ((1 << 33) - 1)) >> 2) as usize;
        let type_num = log_action_id & ((1 << 2) - 1);
        let entry_type = match type_num {
            1 => Role::Follower,
            2 => Role::Leader,
            _ => panic!("unknown log entry type num {}", type_num),
        };
        (slot, slot_e, entry_type)
    }

    /// Compose CommandId from slot index & command index within.
    #[inline]
    fn make_command_id(slot: usize, cmd_idx: usize) -> CommandId {
        debug_assert!(slot <= (u32::MAX as usize));
        debug_assert!(cmd_idx <= (u32::MAX as usize));
        ((slot << 32) | cmd_idx) as CommandId
    }

    /// Decompose CommandId into slot index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let slot = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (slot, cmd_idx)
    }
}

#[async_trait]
impl GenericReplica for CRaftReplica {
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a server ID
        let mut control_hub = ControlHub::new_and_setup(manager).await?;
        let id = control_hub.me;
        let population = control_hub.population;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ReplicaConfigCRaft;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms, disable_hb_timer,
                                    snapshot_path, snapshot_interval_s,
                                    fault_tolerance, msg_chunk_size,
                                    sim_read_lease)?;
        if config.batch_interval_ms == 0 {
            return logged_err!(
                "invalid config.batch_interval_ms '{}'",
                config.batch_interval_ms
            );
        }
        if config.hb_hear_timeout_min < 100 {
            return logged_err!(
                "invalid config.hb_hear_timeout_min '{}'",
                config.hb_hear_timeout_min
            );
        }
        if config.hb_hear_timeout_max < config.hb_hear_timeout_min + 100 {
            return logged_err!(
                "invalid config.hb_hear_timeout_max '{}'",
                config.hb_hear_timeout_max
            );
        }
        if config.hb_send_interval_ms == 0 {
            return logged_err!(
                "invalid config.hb_send_interval_ms '{}'",
                config.hb_send_interval_ms
            );
        }
        if config.msg_chunk_size == 0 {
            return logged_err!(
                "invalid config.msg_chunk_size '{}'",
                config.msg_chunk_size
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
            protocol: SmrProtocol::CRaft,
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

        // create a Reed-Solomon coder with num_data_shards == quorum size and
        // num_parity shards == population - quorum
        let majority = (population / 2) + 1;
        if config.fault_tolerance > (population - majority) {
            return logged_err!(
                "invalid config.fault_tolerance '{}'",
                config.fault_tolerance
            );
        }
        let rs_coder = ReedSolomon::new(
            majority as usize,
            (population - majority) as usize,
        )?;

        // proactively connect to some peers, then wait for all population
        // have been connected with me
        for (peer, conn_addr) in to_peers {
            transport_hub.connect_to_peer(peer, conn_addr).await?;
        }
        transport_hub.wait_for_group(population).await?;

        // setup snapshot hub module
        let snapshot_hub =
            StorageHub::new_and_setup(id, Path::new(&config.snapshot_path))
                .await?;

        // setup external API module, ready to take in client requests
        let external_api = ExternalApi::new_and_setup(
            id,
            api_addr,
            Duration::from_millis(config.batch_interval_ms),
            config.max_batch_size,
        )
        .await?;

        let mut hb_send_interval =
            time::interval(Duration::from_millis(config.hb_send_interval_ms));
        hb_send_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut snapshot_interval = time::interval(Duration::from_secs(
            if config.snapshot_interval_s > 0 {
                config.snapshot_interval_s
            } else {
                60 // dummy non-zero value to make `time::interval` happy
            },
        ));
        snapshot_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let hb_reply_cnts = (0..population)
            .filter_map(|p| if p == id { None } else { Some((p, (1, 0, 0))) })
            .collect();

        Ok(CRaftReplica {
            id,
            population,
            majority,
            full_copy_mode: false,
            config,
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
            control_hub,
            external_api,
            state_machine,
            storage_hub,
            snapshot_hub,
            transport_hub,
            role: Role::Follower,
            leader: None,
            hb_hear_timer: Timer::new(),
            hb_send_interval,
            hb_reply_cnts,
            peer_alive: Bitmap::new(population, true),
            curr_term: 0,
            voted_for: None,
            votes_granted: HashSet::new(),
            log: vec![],
            start_slot: 0,
            snapshot_interval,
            last_commit: 0,
            last_exec: 0,
            last_recon: 0,
            next_slot: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 1)) })
                .collect(),
            try_next_slot: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 1)) })
                .collect(),
            match_slot: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 0)) })
                .collect(),
            last_snap: 0,
            log_offset: 0,
            log_meta_end: 0,
            snap_offset: 0,
            rs_coder,
        })
    }

    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
        // recover state from durable snapshot file
        self.recover_from_snapshot().await?;

        // recover the tail-piece memory log & state from remaining durable log
        self.recover_from_wal().await?;

        // kick off leader activity hearing timer
        self.kickoff_hb_hear_timer()?;

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
                    if let Err(e) = self.handle_msg_recv(peer, msg).await {
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

                // leader inactivity timeout
                _ = self.hb_hear_timer.timeout(), if !paused => {
                    if let Err(e) = self.become_a_candidate().await {
                        pf_error!("error becoming a candidate: {}", e);
                    }
                },

                // leader sending heartbeat
                _ = self.hb_send_interval.tick(), if !paused
                                                     && self.role == Role::Leader => {
                    if let Err(e) = self.bcast_heartbeats() {
                        pf_error!("error broadcasting heartbeats: {}", e);
                    }
                },

                // autonomous snapshot taking timeout
                _ = self.snapshot_interval.tick(), if !paused
                                                      && self.config.snapshot_interval_s > 0 => {
                    if let Err(e) = self.take_new_snapshot().await {
                        pf_error!("error taking a new snapshot: {}", e);
                    } else {
                        self.control_hub.send_ctrl(
                            CtrlMsg::SnapshotUpTo { new_start: self.start_slot }
                        )?;
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
pub struct ClientConfigCRaft {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigCRaft {
    fn default() -> Self {
        ClientConfigCRaft { init_server_id: 0 }
    }
}

/// CRaft client-side module.
pub(crate) struct CRaftClient {
    /// Client ID.
    id: ClientId,

    /// Configuration parameters struct.
    _config: ClientConfigCRaft,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Current server ID to talk to.
    server_id: ReplicaId,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stubs for communicating with servers.
    api_stubs: HashMap<ReplicaId, ClientApiStub>,
}

#[async_trait]
impl GenericEndpoint for CRaftClient {
    async fn new_and_setup(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub = ClientCtrlStub::new_by_connect(manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigCRaft;
                                    init_server_id)?;
        let init_server_id = config.init_server_id;

        Ok(CRaftClient {
            id,
            _config: config,
            servers: HashMap::new(),
            server_id: init_server_id,
            ctrl_stub,
            api_stubs: HashMap::new(),
        })
    }

    async fn connect(&mut self) -> Result<(), SummersetError> {
        // disallow reconnection without leaving
        if !self.api_stubs.is_empty() {
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
                // shift to a new server_id if current one not active
                debug_assert!(!servers_info.is_empty());
                while !servers_info.contains_key(&self.server_id)
                    || servers_info[&self.server_id].is_paused
                {
                    self.server_id = (self.server_id + 1) % population;
                }
                // establish connection to all servers
                self.servers = servers_info
                    .into_iter()
                    .map(|(id, info)| (id, info.api_addr))
                    .collect();
                for (&id, &server) in &self.servers {
                    pf_debug!("connecting to server {} '{}'...", id, server);
                    let api_stub =
                        ClientApiStub::new_by_connect(self.id, server).await?;
                    self.api_stubs.insert(id, api_stub);
                }
                Ok(())
            }
            _ => logged_err!("unexpected reply type received"),
        }
    }

    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        // send leave notification to all servers
        for (id, mut api_stub) in self.api_stubs.drain() {
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
        if self.api_stubs.contains_key(&self.server_id) {
            self.api_stubs
                .get_mut(&self.server_id)
                .unwrap()
                .send_req(req)
        } else {
            Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                self.server_id
            )))
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        if self.api_stubs.contains_key(&self.server_id) {
            let reply = self
                .api_stubs
                .get_mut(&self.server_id)
                .unwrap()
                .recv_reply()
                .await?;

            if let ApiReply::Reply {
                ref result,
                ref redirect,
                ..
            } = reply
            {
                // if the current server redirects me to a different server
                if result.is_none() && redirect.is_some() {
                    let redirect_id = redirect.unwrap();
                    debug_assert!(self.servers.contains_key(&redirect_id));
                    self.server_id = redirect_id;
                    pf_debug!(
                        "redirected to replica {} '{}'",
                        redirect_id,
                        self.servers[&redirect_id]
                    );
                }
            }

            Ok(reply)
        } else {
            Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                self.server_id
            )))
        }
    }

    fn id(&self) -> ClientId {
        self.id
    }

    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        &mut self.ctrl_stub
    }
}
