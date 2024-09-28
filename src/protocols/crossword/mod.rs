//! Replication protocol: Crossword.
//!
//! MultiPaxos with flexible Reed-Solomon erasure code sharding that supports
//! dynamically tunable shard assignment with the correct liveness constraints,
//! plus follower gossiping for actual usability.

mod adaptive;
mod control;
mod durability;
mod execution;
mod gossiping;
mod leadership;
mod messages;
mod recovery;
mod request;
mod snapshot;

use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::Path;
use std::time::SystemTime;

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, CommandId, ControlHub, ExternalApi, GenericReplica,
    LogActionId, ReplicaId, StateMachine, StorageHub, TransportHub,
};
use crate::utils::{
    Bitmap, LinearRegressor, PerfModel, QdiscInfo, RSCodeword, Stopwatch,
    SummersetError, Timer,
};

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::time::{self, Duration, Instant, Interval, MissedTickBehavior};

use reed_solomon_erasure::galois_8::ReedSolomon;

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigCrossword {
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

    /// Disable heartbeat timer (to force a deterministic leader during tests)?
    pub disable_hb_timer: bool,

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Min timeout of follower gossiping trigger in millisecs.
    pub gossip_timeout_min: u64,
    /// Max timeout of follower gossiping trigger in millisecs.
    pub gossip_timeout_max: u64,

    /// How many slots at the end should we ignore when gossiping is triggered.
    pub gossip_tail_ignores: usize,

    /// Batch size for Reconstruct messages; 0 means use `msg_chunk_size`.
    pub gossip_batch_size: usize,

    /// Disable gossiping timer (to force more deterministic perf behavior)?
    pub disable_gossip_timer: bool,

    /// Fault-tolerance level.
    pub fault_tolerance: u8,

    /// Maximum chunk size (in slots) of any bulk messages.
    pub msg_chunk_size: usize,

    /// Total number of possible shards in a codeword (i.e., codeword width).
    /// If zero, sets this to == population.
    pub rs_total_shards: u8,
    /// Number of data shards. If zero, sets this to == majority.
    pub rs_data_shards: u8,

    /// If non-empty, use this initial shards assignment policy.
    pub init_assignment: String,

    /// Update interval of linear regression perf monitoring model.
    pub linreg_interval_ms: u64,
    /// Window timespan of linear regression datapoints to keep.
    pub linreg_keep_ms: u64,

    /// Initial linear regression model slope.
    pub linreg_init_a: f64,
    /// Initial linear regression model intercept.
    pub linreg_init_b: f64,
    /// Ratio of outliers to exclude in each `calc_model()`.
    pub linreg_outlier_ratio: f32,

    /// Knob that controls choosing the best config with perf model values.
    pub b_to_d_threshold: f64,

    /// Recording performance breakdown statistics?
    pub record_breakdown: bool,

    /// Recording the latest committed value version of a key?
    /// Only effective if record_breakdown is set to true.
    pub record_value_ver: bool,

    /// Recording total payload size received from per peer?
    /// Only effective if record_breakdown is set to true.
    pub record_size_recv: bool,

    /// Simulate local read lease implementation?
    // TODO: actual read lease impl later? (won't affect anything about
    // evalutaion results though)
    pub sim_read_lease: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigCrossword {
    fn default() -> Self {
        ReplicaConfigCrossword {
            batch_interval_ms: 1,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.crossword.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 1500,
            hb_hear_timeout_max: 2000,
            hb_send_interval_ms: 20,
            disable_hb_timer: false,
            snapshot_path: "/tmp/summerset.crossword.snap".into(),
            snapshot_interval_s: 0,
            gossip_timeout_min: 10,
            gossip_timeout_max: 30,
            gossip_tail_ignores: 100,
            gossip_batch_size: 0,
            disable_gossip_timer: false,
            fault_tolerance: 0,
            msg_chunk_size: 10,
            rs_total_shards: 0,
            rs_data_shards: 0,
            init_assignment: "".into(),
            linreg_interval_ms: 200,
            linreg_keep_ms: 2000,
            linreg_init_a: 10.0,
            linreg_init_b: 10.0,
            linreg_outlier_ratio: 0.5,
            b_to_d_threshold: 0.0,
            record_breakdown: false,
            record_value_ver: false,
            record_size_recv: false,
            sim_read_lease: false,
        }
    }
}

/// Ballot number type. Use 0 as a null ballot number.
pub(crate) type Ballot = u64;

/// Instance status enum.
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize,
)]
pub(crate) enum Status {
    Null = 0,
    Preparing = 1,
    Accepting = 2,
    Committed = 3,
    Executed = 4,
}

/// Request batch type (i.e., the "value" in Paxos).
pub(crate) type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// Leader-side bookkeeping info for each instance initiated.
#[derive(Debug, Clone)]
pub(crate) struct LeaderBookkeeping {
    /// If in Preparing status, the trigger_slot of this Prepare phase.
    trigger_slot: usize,

    /// If in Preparing status, the endprep_slot of this Prepare phase.
    endprep_slot: usize,

    /// Replicas from which I have received Prepare confirmations.
    prepare_acks: Bitmap,

    /// Max ballot among received Prepare replies.
    prepare_max_bal: Ballot,

    /// Replicas and their assigned shards which the received Accept
    /// confirmations cover.
    accept_acks: HashMap<ReplicaId, Bitmap>,
}

/// Follower-side bookkeeping info for each instance received.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaBookkeeping {
    /// Source leader replica ID for replyiing to Prepares and Accepts.
    source: ReplicaId,

    /// If in Preparing status, the trigger_slot of this Prepare phase.
    trigger_slot: usize,

    /// If in Preparing status, the endprep_slot of this Prepare phase.
    endprep_slot: usize,
}

/// In-memory instance containing a (possibly partial) commands batch.
#[derive(Debug, Clone)]
pub(crate) struct Instance {
    /// Ballot number.
    bal: Ballot,

    /// Instance status.
    status: Status,

    /// Shards of a batch of client requests.
    reqs_cw: RSCodeword<ReqBatch>,

    /// Shards assignment map which the leader used.
    assignment: Vec<Bitmap>,

    /// Highest ballot and associated value I have accepted.
    voted: (Ballot, RSCodeword<ReqBatch>),

    /// Leader-side bookkeeping info.
    leader_bk: Option<LeaderBookkeeping>,

    /// Follower-side bookkeeping info.
    replica_bk: Option<ReplicaBookkeeping>,

    /// True if from external client, else false.
    external: bool,

    /// Offset of first durable WAL log entry related to this instance.
    wal_offset: usize,
}

/// Stable storage WAL log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum WalEntry {
    /// Records an update to the largest prepare ballot seen.
    PrepareBal { slot: usize, ballot: Ballot },

    /// Records a newly accepted request batch data shards at slot index.
    AcceptData {
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
        assignment: Vec<Bitmap>,
    },

    /// Records an event of committing the instance at index.
    CommitSlot { slot: usize },
}

/// Snapshot file entry type.
///
/// NOTE: the current implementation simply appends a squashed log at the
/// end of the snapshot file for simplicity. In production, the snapshot
/// file should be a bounded-sized backend, e.g., an LSM-tree.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum SnapEntry {
    /// Necessary slot indices to remember.
    SlotInfo {
        /// First entry at the start of file: number of log instances covered
        /// by this snapshot file == the start slot index of in-mem log.
        start_slot: usize,
    },

    /// Set of key-value pairs to apply to the state.
    KVPairSet { pairs: HashMap<String, String> },
}

/// Heartbeat messages monotonically incrementing ID.
pub(crate) type HeartbeatId = u64;

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum PeerMsg {
    /// Prepare message from leader to replicas.
    Prepare {
        /// Slot index in Prepare message is the triggering slot of this
        /// Prepare. Once prepared, it means that all slots in the range
        /// [slot, +infinity) are prepared under this ballot number.
        trigger_slot: usize,
        ballot: Ballot,
    },

    /// Prepare reply from replica to leader.
    PrepareReply {
        /// In our implementation, we choose to break the PrepareReply into
        /// slot-wise messages for simplicity.
        slot: usize,
        /// Also carry the trigger_slot information to make it easier for the
        /// leader to track reply progress.
        trigger_slot: usize,
        /// Due to the slot-wise design choice, we need a way to let leader
        /// know when have all PrepareReplies been received. We use the
        /// endprep_slot field to convey this: when all slots' PrepareReplies
        /// up to endprep_slot are received, the "wholesome" PrepareReply
        /// can be considered received.
        // NOTE: this currently assumes the "ordering" property of TCP.
        endprep_slot: usize,
        ballot: Ballot,
        /// The accepted ballot number for that instance and the corresponding
        /// request batch value shards known by replica.
        voted: Option<(Ballot, RSCodeword<ReqBatch>)>,
    },

    /// Accept message from leader to replicas.
    Accept {
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
        /// Shard-to-node assignment used for this instance.
        assignment: Vec<Bitmap>,
    },

    /// Accept reply from replica to leader.
    AcceptReply {
        slot: usize,
        ballot: Ballot,
        /// Data size in bytes that the corresponding Accept carried.
        size: usize,
        /// [for perf breakdown]
        reply_ts: Option<SystemTime>,
    },

    /// Reconstruction read from new leader to replicas.
    Reconstruct {
        /// List of slots and correspondingly the shards to exclude.
        slots_excl: Vec<(usize, Bitmap)>,
    },

    /// Reconstruction read reply from replica to leader.
    ReconstructReply {
        /// Map from slot -> (ballot, peer shards).
        slots_data: HashMap<usize, (Ballot, RSCodeword<ReqBatch>)>,
    },

    /// Leader activity heartbeat.
    Heartbeat {
        id: HeartbeatId,
        ballot: Ballot,
        /// For notifying followers about safe-to-commit slots (in a bit
        /// conservative way).
        commit_bar: usize,
        /// For leader step-up as well as conservative snapshotting purpose.
        exec_bar: usize,
        /// For conservative snapshotting purpose.
        snap_bar: usize,
    },
}

/// Crossword server replica module.
pub(crate) struct CrosswordReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Majority quorum size.
    majority: u8,

    /// Reed-Solomon total number of shards.
    rs_total_shards: u8,

    /// Reed-Solomon number of data shards.
    rs_data_shards: u8,

    /// Doing dynamically adaptive config choosing?
    // NOTE: currently, adaptability is only enabled when an initial assignment
    //       is not give.
    assignment_adaptive: bool,

    /// Using only balanced assignment policies?
    // NOTE: currently, adaptability is only enabled when using balanced
    //       assignment policies.
    assignment_balanced: bool,

    /// Initial assignment policy from config.
    init_assignment: Vec<Bitmap>,

    /// Pre-filled good balanced round-robin assignment policies for quicker
    /// access when peer_alive count is low.
    brr_assignments: HashMap<u8, Vec<Bitmap>>,

    /// Configuration parameters struct.
    config: ReplicaConfigCrossword,

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

    /// Who do I think is the effective leader of the cluster right now?
    leader: Option<ReplicaId>,

    /// Timer for hearing heartbeat from leader.
    hb_hear_timer: Timer,

    /// Interval for sending heartbeat to followers.
    hb_send_interval: Interval,

    /// Heartbeat reply counters for approximate detection of follower health.
    /// Tuple of (#hb_replied, #hb_replied seen at last send, repetition).
    hb_reply_cnts: HashMap<ReplicaId, (u64, u64, u64)>,

    /// Approximate health status tracking of peer replicas.
    peer_alive: Bitmap,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Start slot index of in-mem log after latest snapshot.
    start_slot: usize,

    /// Timer for taking a new autonomous snapshot.
    snapshot_interval: Interval,

    /// Timer for triggering linear regression model update.
    linreg_interval: Interval,

    /// Timer for triggering follower gossiping.
    gossip_timer: Timer,

    /// Largest ballot number that a leader has sent Prepare messages in.
    bal_prep_sent: Ballot,

    /// Largest ballot number that a leader knows has been safely prepared.
    bal_prepared: Ballot,

    /// Largest ballot number seen as acceptor.
    bal_max_seen: Ballot,

    /// Index of the first non-committed instance.
    commit_bar: usize,

    /// Index of the first instance starting from which gossiping might be needed.
    /// The "Gossiped" status is captured by this variable implicitly.
    gossip_bar: usize,

    /// Index of the first non-executed instance.
    /// The following is always true:
    ///   exec_bar <= gossip_bar <= commit_bar <= start_slot + insts.len()
    exec_bar: usize,

    /// Map from peer ID -> its latest exec_bar I know; this is for conservative
    /// snapshotting purpose.
    peer_exec_bar: HashMap<ReplicaId, usize>,

    /// Slot index before which it is safe to take snapshot.
    /// NOTE: we are taking a conservative approach here that a snapshot
    /// covering an entry can be taken only when all servers have durably
    /// committed (and executed) that entry.
    snap_bar: usize,

    /// Current durable WAL log file offset.
    wal_offset: usize,

    /// Current durable snapshot file offset.
    snap_offset: usize,

    /// Fixed Reed-Solomon coder.
    rs_coder: ReedSolomon,

    /// Map from peer ID -> list of pending Accepts (timestamp, slot) for
    /// perf monitoring.
    pending_accepts: HashMap<ReplicaId, VecDeque<(u128, usize)>>,

    /// Map from peer ID -> ;ist of pending heartbeats (timestamp, id) for
    /// perf monitoring.
    pending_heartbeats: HashMap<ReplicaId, VecDeque<(u128, HeartbeatId)>>,

    /// Monotonically incrementing ID for heartbeat messages.
    next_hb_id: HeartbeatId,

    /// Base time instant at startup, used as a reference zero timestamp.
    startup_time: Instant,

    /// Elapsed us of the last console printing of linreg models result.
    last_linreg_print: u128,

    /// Map from peer ID -> linear regressor for perf monitoring model.
    regressor: HashMap<ReplicaId, LinearRegressor>,

    /// Map from peer ID -> current saved linear regression perf model.
    linreg_model: HashMap<ReplicaId, PerfModel>,

    /// Queueing discipline information tracker.
    qdisc_info: Option<QdiscInfo>,

    /// Performance breakdown stopwatch if doing recording.
    bd_stopwatch: Option<Stopwatch>,

    /// Performance breakdown printing interval.
    bd_print_interval: Interval,

    /// Bandwidth utilization total bytes accumulators.
    bw_accumulators: HashMap<ReplicaId, usize>,
}

// CrosswordReplica common helpers
impl CrosswordReplica {
    /// Do I think I am the current effective leader?
    #[inline]
    fn is_leader(&self) -> bool {
        self.leader == Some(self.id)
    }

    /// Create an empty null instance.
    #[inline]
    fn null_instance(&self) -> Result<Instance, SummersetError> {
        Ok(Instance {
            bal: 0,
            status: Status::Null,
            reqs_cw: RSCodeword::<ReqBatch>::from_null(
                self.rs_data_shards,
                self.rs_total_shards - self.rs_data_shards,
            )?,
            assignment: vec![],
            voted: (
                0,
                RSCodeword::<ReqBatch>::from_null(
                    self.rs_data_shards,
                    self.rs_total_shards - self.rs_data_shards,
                )?,
            ),
            leader_bk: None,
            replica_bk: None,
            external: false,
            wal_offset: 0,
        })
    }

    /// Locate the first null slot or append a null instance if no holes exist.
    fn first_null_slot(&mut self) -> Result<usize, SummersetError> {
        for s in self.exec_bar..(self.start_slot + self.insts.len()) {
            if self.insts[s - self.start_slot].status == Status::Null {
                return Ok(s);
            }
        }
        self.insts.push(self.null_instance()?);
        Ok(self.start_slot + self.insts.len() - 1)
    }

    /// Compose a unique ballot number from base.
    #[inline]
    fn make_unique_ballot(&self, base: u64) -> Ballot {
        ((base << 8) | ((self.id + 1) as u64)) as Ballot
    }

    /// Compose a unique ballot number greater than the given one.
    #[inline]
    fn make_greater_ballot(&self, bal: Ballot) -> Ballot {
        self.make_unique_ballot((bal >> 8) + 1)
    }

    /// Compose LogActionId from slot index & entry type.
    /// Uses the `Status` enum type to represent differnet entry types.
    #[inline]
    fn make_log_action_id(slot: usize, entry_type: Status) -> LogActionId {
        let type_num = match entry_type {
            Status::Preparing => 1,
            Status::Accepting => 2,
            Status::Committed => 3,
            _ => panic!("unknown log entry type {:?}", entry_type),
        };
        ((slot << 2) | type_num) as LogActionId
    }

    /// Decompose LogActionId into slot index & entry type.
    #[inline]
    fn split_log_action_id(log_action_id: LogActionId) -> (usize, Status) {
        let slot = (log_action_id >> 2) as usize;
        let type_num = log_action_id & ((1 << 2) - 1);
        let entry_type = match type_num {
            1 => Status::Preparing,
            2 => Status::Accepting,
            3 => Status::Committed,
            _ => panic!("unknown log entry type num {}", type_num),
        };
        (slot, entry_type)
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
impl GenericReplica for CrosswordReplica {
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
        let config = parsed_config!(config_str => ReplicaConfigCrossword;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms, disable_hb_timer,
                                    snapshot_path, snapshot_interval_s,
                                    gossip_timeout_min, gossip_timeout_max,
                                    gossip_tail_ignores, disable_gossip_timer,
                                    gossip_batch_size, fault_tolerance,
                                    msg_chunk_size, rs_total_shards, rs_data_shards,
                                    init_assignment, linreg_interval_ms,
                                    linreg_keep_ms, linreg_outlier_ratio,
                                    linreg_init_a, linreg_init_b,
                                    b_to_d_threshold, record_breakdown,
                                    record_value_ver, record_size_recv,
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
        if config.gossip_timeout_max < config.gossip_timeout_min {
            return logged_err!(
                "invalid config.gossip_timeout_max '{}'",
                config.gossip_timeout_max
            );
        }
        if config.msg_chunk_size == 0 {
            return logged_err!(
                "invalid config.msg_chunk_size '{}'",
                config.msg_chunk_size
            );
        }
        if config.linreg_interval_ms == 0 {
            return logged_err!(
                "invalid config.linreg_interval_ms '{}'",
                config.linreg_interval_ms
            );
        }
        if config.linreg_keep_ms == 0 {
            return logged_err!(
                "invalid config.linreg_keep_ms '{}'",
                config.linreg_keep_ms
            );
        }
        if !(0.0..1.0).contains(&config.linreg_outlier_ratio) {
            return logged_err!(
                "invalid config.linreg_outlier_ratio '{}'",
                config.linreg_outlier_ratio
            );
        }
        if config.b_to_d_threshold < 0.0 {
            return logged_err!(
                "invalid config.b_to_d_threshold '{}'",
                config.b_to_d_threshold
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
            protocol: SmrProtocol::Crossword,
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

        // compute majority and set fault_tolerance level
        let majority = (population / 2) + 1;
        if config.fault_tolerance > (population - majority) {
            return logged_err!(
                "invalid config.fault_tolerance '{}'",
                config.fault_tolerance
            );
        }

        // parse Reed_Solomon coding scheme
        if config.rs_total_shards % population != 0 {
            return logged_err!(
                "invalid config.rs_total_shards '{}'",
                config.rs_total_shards
            );
        }
        let rs_total_shards = cmp::max(config.rs_total_shards, population);
        if (config.rs_total_shards != 0 && config.rs_data_shards == 0)
            || (config.rs_total_shards == 0 && config.rs_data_shards != 0)
            || (config.rs_data_shards % majority != 0)
            || (config.rs_data_shards != 0
                && config.rs_data_shards / majority
                    != rs_total_shards / population)
        {
            return logged_err!(
                "invalid config.rs_data_shards '{}'",
                config.rs_data_shards
            );
        }
        let rs_data_shards = cmp::max(config.rs_data_shards, majority);

        // create a Reed-Solomon coder
        let rs_coder = ReedSolomon::new(
            rs_data_shards as usize,
            (rs_total_shards - rs_data_shards) as usize,
        )?;

        // parse shards assignment policy config string if given
        let init_assignment = Self::parse_init_assignment(
            population,
            rs_total_shards,
            rs_data_shards,
            &config.init_assignment,
        )?;
        let mut max_coverage = Bitmap::new(rs_total_shards, false);
        let mut nums_assigned: HashSet<u8> = HashSet::new();
        for shards in &init_assignment {
            nums_assigned.insert(shards.count());
            for (shard, flag) in shards.iter() {
                if flag {
                    max_coverage.set(shard, true)?;
                }
            }
        }
        let assignment_adaptive = config.init_assignment.is_empty();
        let assignment_balanced = nums_assigned.len() == 1;
        if init_assignment.len() != population as usize
            || max_coverage.count() < rs_data_shards
        {
            return logged_err!(
                "invalid init assignment parsed: {:?}",
                init_assignment
            );
        }
        pf_debug!(
            "init asgmt {}",
            Self::assignment_to_string(&init_assignment)
        );

        // if restricted to balanced assignments only, pre-fill
        // `brr_assignments` with balanced round-robin policies
        let dj_spr = rs_total_shards / population;
        let brr_assignments = if assignment_balanced {
            (dj_spr..=rs_data_shards)
                .step_by(dj_spr as usize)
                .map(|spr| {
                    (
                        spr,
                        (0..population)
                            .map(|r| {
                                Bitmap::from(
                                    rs_total_shards,
                                    ((r * dj_spr)..(r * dj_spr + spr))
                                        .map(|i| i % rs_total_shards)
                                        .collect(),
                                )
                            })
                            .collect(),
                    )
                })
                .collect()
        } else {
            HashMap::new()
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

        let mut linreg_interval =
            time::interval(Duration::from_millis(config.linreg_interval_ms));
        linreg_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let hb_reply_cnts = (0..population)
            .filter_map(|p| if p == id { None } else { Some((p, (1, 0, 0))) })
            .collect();

        let bd_stopwatch = if config.record_breakdown {
            Some(Stopwatch::new())
        } else {
            None
        };
        let mut bd_print_interval = time::interval(Duration::from_secs(5));
        bd_print_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Ok(CrosswordReplica {
            id,
            population,
            majority,
            rs_total_shards,
            rs_data_shards,
            assignment_adaptive,
            assignment_balanced,
            init_assignment,
            brr_assignments,
            config: config.clone(),
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
            control_hub,
            external_api,
            state_machine,
            storage_hub,
            snapshot_hub,
            transport_hub,
            leader: None,
            hb_hear_timer: Timer::new(),
            hb_send_interval,
            hb_reply_cnts,
            peer_alive: Bitmap::new(population, true),
            insts: vec![],
            start_slot: 0,
            snapshot_interval,
            linreg_interval,
            gossip_timer: Timer::new(),
            bal_prep_sent: 0,
            bal_prepared: 0,
            bal_max_seen: 0,
            commit_bar: 0,
            gossip_bar: 0,
            exec_bar: 0,
            peer_exec_bar: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 0)) })
                .collect(),
            snap_bar: 0,
            wal_offset: 0,
            snap_offset: 0,
            rs_coder,
            pending_accepts: (0..population)
                .filter_map(|s| {
                    if s == id {
                        None
                    } else {
                        Some((s, VecDeque::new()))
                    }
                })
                .collect(),
            pending_heartbeats: (0..population)
                .filter_map(|s| {
                    if s == id {
                        None
                    } else {
                        Some((s, VecDeque::new()))
                    }
                })
                .collect(),
            next_hb_id: 0,
            startup_time: Instant::now(),
            last_linreg_print: 0,
            regressor: (0..population)
                .filter_map(|s| {
                    if s == id {
                        None
                    } else {
                        Some((s, LinearRegressor::new()))
                    }
                })
                .collect(),
            linreg_model: (0..population)
                .filter_map(|s| {
                    if s == id {
                        None
                    } else {
                        Some((
                            s,
                            PerfModel::new(
                                config.linreg_init_a,
                                config.linreg_init_b,
                                0.0,
                            ),
                        ))
                    }
                })
                .collect(),
            qdisc_info: if config.b_to_d_threshold > 0.0 {
                Some(QdiscInfo::new()?)
            } else {
                None
            },
            bd_stopwatch,
            bd_print_interval,
            bw_accumulators: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 0)) })
                .collect(),
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

        // kick off leader activity hearing timer
        self.kickoff_hb_hear_timer()?;

        // kick off follower gossiping trigger timer
        self.kickoff_gossip_timer()?;

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

                // leader inactivity timeout
                _ = self.hb_hear_timer.timeout(), if !paused => {
                    if let Err(e) = self.become_a_leader() {
                        pf_error!("error becoming a leader: {}", e);
                    }
                },

                // leader sending heartbeat
                _ = self.hb_send_interval.tick(), if !paused && self.is_leader() => {
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

                // linear regression model update trigger
                _ = self.linreg_interval.tick(), if !paused && self.is_leader() => {
                    if let Err(e) = self.update_linreg_model(self.config.linreg_keep_ms) {
                        pf_error!("error updating linear regression model: {}", e);
                    }
                    if let Err(e) = self.update_qdisc_info() {
                        pf_error!("error updating tc qdisc info: {}", e);
                    }
                },

                // follower gossiping trigger
                _ = self.gossip_timer.timeout(), if !paused && !self.is_leader() => {
                    if let Err(e) = self.trigger_gossiping() {
                        pf_error!("error triggering gossiping: {}", e);
                    }
                },

                // performance breakdown stats printing
                _ = self.bd_print_interval.tick(), if !paused && self.config.record_breakdown => {
                    if self.is_leader() {
                        if let Some(sw) = self.bd_stopwatch.as_mut() {
                            let (cnt, stats) = sw.summarize(5);
                            pf_info!("bd cnt {} comp {:.2} {:.2} ldur {:.2} {:.2} \
                                                         arep {:.2} {:.2} qrum {:.2} {:.2} \
                                                         exec {:.2} {:.2}",
                                              cnt, stats[0].0, stats[0].1, stats[1].0, stats[1].1,
                                                   stats[2].0, stats[2].1, stats[3].0, stats[3].1,
                                                   stats[4].0, stats[4].1);
                            sw.remove_all();
                        }
                    }
                    if self.config.record_value_ver {
                        if let Ok(Some((key, ver))) = self.val_ver_of_first_key() {
                            pf_info!("ver of {} @ {} ms is {}",
                                              key,
                                              Instant::now()
                                                .duration_since(self.startup_time)
                                                .as_millis(),
                                              ver);
                        }
                    }
                    if self.config.record_size_recv {
                        for (peer, recv) in &mut self.bw_accumulators {
                            pf_info!("bw period bytes recv <- {} : {}", peer, recv);
                            *recv = 0;
                        }
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
pub struct ClientConfigCrossword {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigCrossword {
    fn default() -> Self {
        ClientConfigCrossword { init_server_id: 0 }
    }
}

/// Crossword client-side module.
pub(crate) struct CrosswordClient {
    /// Client ID.
    id: ClientId,

    /// Configuration parameters struct.
    _config: ClientConfigCrossword,

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
impl GenericEndpoint for CrosswordClient {
    async fn new_and_setup(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub = ClientCtrlStub::new_by_connect(manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigCrossword;
                                    init_server_id)?;
        let init_server_id = config.init_server_id;

        Ok(CrosswordClient {
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
