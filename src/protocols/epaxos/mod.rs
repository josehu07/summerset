//! Replication protocol: EPaxos.
//!
//! Leaderless Egalitarian Paxos that allows any replica to be command leader.
//! There's' no distinction between commit vs. execute latency now in Summerset
//! (see comments in 'execution.rs'). References:
//!   - <https://www.cs.cmu.edu/~dga/papers/epaxos-sosp2013.pdf>
//!   - <https://www.usenix.org/system/files/nsdi21-tollman.pdf>

mod control;
mod dependency;
mod durability;
mod execution;
mod heartbeat;
mod messages;
mod recovery;
mod request;
mod snapshot;

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::ops;
use std::path::Path;
use std::slice;

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, Command, CommandId, CommandResult, ControlHub,
    ExternalApi, GenericReplica, HeartbeatEvent, Heartbeater, LogActionId,
    ReplicaId, StateMachine, StorageHub, TransportHub,
};
use crate::utils::{Bitmap, SummersetError};

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::time::{self, Duration, Interval, MissedTickBehavior};

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigEPaxos {
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

    /// Interval of leader sending heartbeats to followers.
    pub hb_send_interval_ms: u64,

    /// Disable heartbeat timer (to force a deterministic leader during tests).
    pub disable_hb_timer: bool,

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Maximum chunk size (in slots) of any bulk messages.
    pub msg_chunk_size: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigEPaxos {
    fn default() -> Self {
        ReplicaConfigEPaxos {
            batch_interval_ms: 1,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.epaxos.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 1500,
            hb_hear_timeout_max: 2000,
            hb_send_interval_ms: 20,
            disable_hb_timer: false,
            snapshot_path: "/tmp/summerset.epaxos.snap".into(),
            snapshot_interval_s: 0,
            msg_chunk_size: 10,
        }
    }
}

/// Ballot number type. Use 0 as a null ballot number.
type Ballot = u64;

/// Sequence number type for PreAccepts. Use 0 as a dummy number.
type SeqNum = u64;

/// Dependency set type with the assumption that, since dependencies here are
/// naturally transitive, we just need to record the highest interfering column
/// index for each row.
#[derive(
    Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, GetSize,
)]
struct DepSet(Vec<Option<usize>>); // length always == population

/// Instance status enum.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    GetSize,
)]
enum Status {
    Null = 0,
    PreAccepting = 1,
    Accepting = 2,
    Committed = 3,
    Executed = 4,
}

/// Request batch type (i.e., the "value" in Paxos).
type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// Command leader-side bookkeeping info for each instance initiated.
#[derive(Debug, Clone)]
struct LeaderBookkeeping {
    /// Replicas from which I have received PreAccept confirmations.
    pre_accept_acks: Bitmap,

    /// The set of PreAccept replies received so far.
    pre_accept_replies: HashMap<ReplicaId, (SeqNum, DepSet)>,

    /// Replicas from which I have received Accept confirmations.
    accept_acks: Bitmap,

    /// Replicas from which I have received ExpPrepare confirmations.
    exp_prepare_acks: Bitmap,

    /// Max ballot among received ExpPrepare replies.
    exp_prepare_max_bal: Ballot,

    /// The set of ExpPrepare replies with the highest ballot number.
    exp_prepare_voteds: HashMap<ReplicaId, (Status, SeqNum, DepSet, ReqBatch)>,
}

/// Follower-side bookkeeping info for each instance received.
#[derive(Debug, Clone)]
struct ReplicaBookkeeping {
    /// Source command leader replica ID for replying to messages.
    source: ReplicaId,
}

/// Index into an instance slot in the 2D instance space.
#[derive(
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    GetSize,
)]
struct SlotIdx(ReplicaId, usize);

impl fmt::Display for SlotIdx {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.0, self.1)
    }
}

impl SlotIdx {
    /// Unpack row and column index as two usizes.
    #[inline]
    fn unpack(self) -> (usize, usize) {
        (self.0 as usize, self.1)
    }
}

/// In-memory instance containing a commands batch.
#[derive(Debug, Clone)]
struct Instance {
    /// Ballot number.
    bal: Ballot,

    /// Instance status.
    status: Status,

    /// Sequence number.
    seq: SeqNum,

    /// Dependency set.
    deps: DepSet,

    /// Batch of client requests.
    reqs: ReqBatch,

    /// Command leader-side bookkeeping info.
    leader_bk: Option<LeaderBookkeeping>,

    /// Follower-side bookkeeping info.
    replica_bk: Option<ReplicaBookkeeping>,

    /// True if from external client, else false.
    external: bool,

    /// True if explicitly avoiding fast path after the PreAccept phase.
    avoid_fast_path: bool,

    /// Offset of first durable WAL log entry related to this instance.
    wal_offset: usize,
}

/// Stable storage WAL log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
enum WalEntry {
    /// Records a newly initiated PreAccept phase instance.
    PreAcceptSlot {
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    },

    /// Records a slow-path Accept phase update to instance.
    AcceptSlot {
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    },

    /// Records an event of committing the instance at index.
    CommitSlot {
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    },
}

/// Snapshot file entry type.
//
// NOTE: the current implementation simply appends a squashed log at the
//       end of the snapshot file for simplicity. In production, the snapshot
//       file should be a bounded-sized backend, e.g., an LSM-tree.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
enum SnapEntry {
    /// Necessary slot indices to remember.
    SlotInfo {
        /// First entry at the start of file: number of log instance columns
        /// covered by this snapshot file == the start column index of in-mem
        /// instance space.
        start_col: usize,
    },

    /// Set of key-value pairs to apply to the state.
    KVPairSet { pairs: HashMap<String, String> },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
enum PeerMsg {
    /// PreAccept message from command leader to replicas.
    PreAccept {
        /// Slot index of the instance from peer to be accepted.
        slot: SlotIdx,
        ballot: Ballot,
        /// Sequence number.
        seq: SeqNum,
        /// Dependencies on sender.
        deps: DepSet,
        reqs: ReqBatch,
    },

    /// PreAccept reply from replica to command leader.
    PreAcceptReply {
        slot: SlotIdx,
        ballot: Ballot,
        /// Updated sequence number.
        seq: SeqNum,
        /// Updated dependencies with senders'.
        deps: DepSet,
    },

    /// Slow-path Accept message from command leader to replicas.
    Accept {
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    },

    /// Slow-path Accept reply from replica to command leader.
    AcceptReply { slot: SlotIdx, ballot: Ballot },

    /// Notification of commit from command leader to replicas.
    CommitNotice {
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    },

    /// ExpPrepare message from replica that suspects a failure to others.
    ExpPrepare { slot: SlotIdx, ballot: Ballot },

    /// ExpPrepare reply from replica to sender.
    ExpPrepareReply {
        slot: SlotIdx,
        ballot: Ballot,
        /// Highest ballot *accepted* before the one in ExpPrepare.
        voted_bal: Ballot,
        /// Last accepted in which phase.
        voted_status: Status,
        // My knowledge of the instance:
        voted_seq: SeqNum,
        voted_deps: DepSet,
        voted_reqs: ReqBatch,
    },

    /// Peer-to-peer periodic heartbeat.
    Heartbeat {
        ballot: Ballot,
        /// commit_bar of each row of the instance space; for notifying
        /// followers about safe-to-commit slots (in a bit conservative way).
        commit_bars: Vec<usize>, // length always == population
        /// exec_bar of each row of the instance space; for conservative
        /// snapshotting purpose.
        exec_bars: Vec<usize>, // length always == population
        /// For conservative snapshotting purpose.
        snap_bar: usize,
    },
}

/// EPaxos server replica module.
pub(crate) struct EPaxosReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Simple majority quorum size.
    simple_quorum_cnt: u8,

    /// Super majority (fast) quorum size.
    super_quorum_cnt: u8,

    /// Configuration parameters struct.
    config: ReplicaConfigEPaxos,

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

    /// StorageHub module for the snapshot file.
    snapshot_hub: StorageHub<SnapEntry>,

    /// TransportHub module.
    transport_hub: TransportHub<PeerMsg>,

    /// Heartbeater module.
    heartbeater: Heartbeater,

    /// In-memory 2D-array instance space, one row per replica.
    insts: Vec<Vec<Instance>>, // outer length always == population

    /// Start slot column index of in-mem instance space after latest snapshot.
    start_col: usize,

    /// Timer for taking a new autonomous snapshot.
    snapshot_interval: Interval,

    /// Column index of the first non-committed instance of each row.
    commit_bars: Vec<usize>, // length always == population

    /// Column index of the first non-executed instance of each row.
    /// It is always true that
    ///   exec_bar <= commit_bar <= start_slot + insts.len()
    exec_bars: Vec<usize>, // length always == population

    /// Map from peer ID -> its latest minimum exec_bar I know across its rows;
    /// this is for conservative snapshotting purpose.
    peer_exec_min: HashMap<ReplicaId, usize>,

    /// Column index before which it is safe to take snapshot.
    // NOTE: we are taking a conservative approach here that a snapshot up to
    //       covering a column can be taken only when all servers have durably
    //       committed (and executed) that column in all rows.
    snap_bar: usize,

    /// Map from key -> the highest column index in each row that (might)
    /// contain a write to that key. Used for faster cmd interference checking.
    // NOTE: there probably are better ways to do such bookkeeping, but this is
    //       good enough for now unless the key space is disgustingly huge
    highest_cols: HashMap<String, DepSet>,

    /// Current durable WAL log file offset.
    wal_offset: usize,

    /// Current durable snapshot file offset.
    snap_offset: usize,
}

// EPaxosReplica common helpers
impl EPaxosReplica {
    /// Create an empty null instance.
    #[inline]
    fn null_instance(&self) -> Instance {
        Instance {
            bal: 0,
            status: Status::Null,
            seq: 0,
            deps: DepSet::empty(self.population),
            reqs: ReqBatch::new(),
            leader_bk: None,
            replica_bk: None,
            external: false,
            avoid_fast_path: false,
            wal_offset: 0,
        }
    }

    /// Locate the first null slot or append a null instance if no holes exist
    /// in specified row.
    fn first_null_slot(&mut self, row: ReplicaId) -> SlotIdx {
        let row = row as usize;
        for c in self.exec_bars[row]..(self.start_col + self.insts[row].len()) {
            if self.insts[row][c - self.start_col].status == Status::Null {
                return SlotIdx(row as ReplicaId, c);
            }
        }

        let inst = self.null_instance();
        self.insts[row].push(inst);
        SlotIdx(row as ReplicaId, self.start_col + self.insts[row].len() - 1)
    }

    /// Compose a unique ballot number from base.
    #[inline]
    fn make_unique_ballot(id: ReplicaId, base: u64) -> Ballot {
        ((base << 8) | ((id + 1) as u64)) as Ballot
    }

    /// Compose a unique ballot number greater than the given one.
    #[inline]
    fn make_greater_ballot(id: ReplicaId, bal: Ballot) -> Ballot {
        Self::make_unique_ballot(id, (bal >> 8) + 1)
    }

    /// Returns the default ballot number for replica ID
    #[inline]
    fn make_default_ballot(id: ReplicaId) -> Ballot {
        Self::make_unique_ballot(id, 0)
    }

    /// Compose LogActionId from slot index & entry type.
    /// Uses the `Status` enum type to represent different entry types.
    #[inline]
    fn make_log_action_id(slot: SlotIdx, entry_type: Status) -> LogActionId {
        let (row, col) = slot.unpack();
        debug_assert!(row < (1 << 6));
        let type_num = match entry_type {
            Status::PreAccepting => 1,
            Status::Accepting => 2,
            Status::Committed => 3,
            _ => panic!("unknown log entry type {:?}", entry_type),
        };
        ((col << 8) | (row << 2) | type_num) as LogActionId
    }

    /// Decompose LogActionId into slot index & entry type.
    #[inline]
    fn split_log_action_id(log_action_id: LogActionId) -> (SlotIdx, Status) {
        let col = (log_action_id >> 10) as usize;
        let row = ((log_action_id >> 2) & ((1 << 6) - 1)) as ReplicaId;
        let type_num = log_action_id & ((1 << 2) - 1);
        let entry_type = match type_num {
            1 => Status::PreAccepting,
            2 => Status::Accepting,
            3 => Status::Committed,
            _ => panic!("unknown log entry type num {}", type_num),
        };
        (SlotIdx(row, col), entry_type)
    }

    /// Compose CommandId from slot index & command index within.
    #[inline]
    fn make_command_id(slot: SlotIdx, cmd_idx: usize) -> CommandId {
        let (row, col) = slot.unpack();
        debug_assert!(row < (1 << 6));
        debug_assert!(col < (1 << 29));
        debug_assert!(cmd_idx < (1 << 29));
        ((col << 35) | (row << 29) | cmd_idx) as CommandId
    }

    /// Decompose CommandId into slot index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (SlotIdx, usize) {
        let col = (command_id >> 35) as usize;
        let row = ((command_id >> 29) & ((1 << 6) - 1)) as ReplicaId;
        let cmd_idx = (command_id & ((1 << 29) - 1)) as usize;
        (SlotIdx(row, col), cmd_idx)
    }
}

#[async_trait]
impl GenericReplica for EPaxosReplica {
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
        let config = parsed_config!(config_str => ReplicaConfigEPaxos;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms, disable_hb_timer,
                                    snapshot_path, snapshot_interval_s,
                                    msg_chunk_size)?;
        if config.batch_interval_ms == 0 {
            return logged_err!(
                "invalid config.batch_interval_ms '{}'",
                config.batch_interval_ms
            );
        }
        if config.hb_hear_timeout_min == 0 {
            return logged_err!(
                "invalid config.hb_hear_timeout_min '{}'",
                config.hb_hear_timeout_min
            );
        }
        if config.hb_hear_timeout_max < config.hb_hear_timeout_min {
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

        // setup heartbeat management module
        let mut heartbeater = Heartbeater::new_and_setup(
            id,
            population,
            Duration::from_millis(config.hb_hear_timeout_min),
            Duration::from_millis(config.hb_hear_timeout_max),
            Duration::from_millis(config.hb_send_interval_ms),
        )?;
        heartbeater.set_sending(true); // doing all-to-all heartbeating

        // setup transport hub module
        let mut transport_hub = TransportHub::new_and_setup(
            id,
            population,
            p2p_addr,
            HashMap::new(), // no leases
        )
        .await?;

        // ask for the list of peers to proactively connect to. Do this after
        // transport hub has been set up, so that I will be able to accept
        // later peer connections
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::EPaxos,
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

        let mut snapshot_interval = time::interval(Duration::from_secs(
            if config.snapshot_interval_s > 0 {
                config.snapshot_interval_s
            } else {
                60 // dummy non-zero value to make `time::interval` happy
            },
        ));
        snapshot_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Ok(EPaxosReplica {
            id,
            population,
            simple_quorum_cnt: (population / 2) + 1,
            super_quorum_cnt: (population / 2) * 2, // FIXME: use optimized fast quorum
            config,
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
            control_hub,
            external_api,
            state_machine,
            storage_hub,
            snapshot_hub,
            transport_hub,
            heartbeater,
            insts: vec![vec![]; population as usize],
            start_col: 0,
            snapshot_interval,
            commit_bars: vec![0; population as usize],
            exec_bars: vec![0; population as usize],
            peer_exec_min: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 0)) })
                .collect(),
            snap_bar: 0,
            highest_cols: HashMap::new(),
            wal_offset: 0,
            snap_offset: 0,
        })
    }

    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
        // recover state from durable snapshot file
        self.recover_from_snapshot().await?;

        // recover the tail-piece memory log & state from durable WAL log
        self.recover_from_wal().await?;

        // kick off peer heartbeats hearing timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer()?;
        }

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
                    if let Err(e) = self.handle_req_batch(req_batch).await {
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
                    if let Err(e) = self.handle_log_result(action_id, log_result).await {
                        pf_error!("error handling log result {}: {}",
                                           action_id, e);
                    }
                },

                // message from peer
                msg = self.transport_hub.recv_msg(), if !paused => {
                    if let Err(_e) = msg {
                        // NOTE: commented out to prevent console lags
                        //       during benchmarking
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
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result).await {
                        pf_error!("error handling cmd result {}: {}", cmd_id, e);
                    }
                },

                // heartbeat-related event
                hb_event = self.heartbeater.get_event(), if !paused => {
                    match hb_event {
                        HeartbeatEvent::HearTimeout => {
                            if let Err(e) = self.heartbeat_timeout().await {
                                pf_error!("error taking care of hb_hear timeout: {}", e);
                            }
                        }
                        HeartbeatEvent::SendTicked => {
                            if let Err(e) = self.bcast_heartbeats().await {
                                pf_error!("error broadcasting heartbeats: {}", e);
                            }
                        }
                    }
                },

                // autonomous snapshot taking timeout
                _ = self.snapshot_interval.tick(), if !paused
                                                      && self.config.snapshot_interval_s > 0 => {
                    if let Err(e) = self.take_new_snapshot().await {
                        pf_error!("error taking a new snapshot: {}", e);
                    } else {
                        self.control_hub.send_ctrl(
                            CtrlMsg::SnapshotUpTo { new_start: self.start_col }
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

    fn population(&self) -> u8 {
        self.population
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigEPaxos {
    /// App-designated nearest server ID as its command leader.
    pub near_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigEPaxos {
    fn default() -> Self {
        ClientConfigEPaxos { near_server_id: 0 }
    }
}

/// EPaxos client-side module.
pub(crate) struct EPaxosClient {
    /// Client ID.
    id: ClientId,

    /// Number of servers in the cluster.
    population: u8,

    /// Configuration parameters struct.
    config: ClientConfigEPaxos,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// App-designated nearest server ID as its command leader. Could become
    /// different from `config.near_server_id` if the latter is deemed inactive.
    server_id: ReplicaId,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stubs for communicating with servers.
    api_stubs: HashMap<ReplicaId, ClientApiStub>,
}

#[async_trait]
impl GenericEndpoint for EPaxosClient {
    async fn new_and_setup(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub = ClientCtrlStub::new_by_connect(manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigEPaxos;
                                    near_server_id)?;
        let server_id = config.near_server_id;

        Ok(EPaxosClient {
            id,
            population: 0,
            config,
            servers: HashMap::new(),
            server_id,
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
                self.population = population;

                // shift to a new server_id if nearest one not active
                debug_assert!(!servers_info.is_empty());
                while !servers_info.contains_key(&self.server_id)
                    || servers_info[&self.server_id].is_paused
                {
                    self.server_id = (self.server_id + 1) % population;
                }
                if self.server_id != self.config.near_server_id {
                    pf_warn!(
                        "near server {} inactive, using {} instead...",
                        self.config.near_server_id,
                        self.server_id
                    );
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
            //       hanging upon leaving
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

    fn population(&self) -> u8 {
        self.population
    }

    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        &mut self.ctrl_stub
    }
}
