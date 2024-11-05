//! Replication protocol: Quorum Leases.
//!
//! Built on top of MultiPaxos, allows marking an arbitrary subset of nodes as
//! read lease holders and letting them serve reads locally during quiescent
//! periods when no concurrent writes are happending. References:
//!   - <https://www.cs.cmu.edu/~imoraru/papers/qrl.pdf>
//!   - <https://www.pdl.cmu.edu/PDL-FTP/associated/CMU-PDL-14-105.pdf>

mod control;
mod durability;
mod execution;
mod leaderlease;
mod leadership;
mod messages;
mod quorumconf;
mod quorumlease;
mod recovery;
mod request;
mod snapshot;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::SystemTime;

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, Command, CommandId, CommandResult, ControlHub,
    ExternalApi, GenericReplica, HeartbeatEvent, Heartbeater, LeaseManager,
    LeaseMsg, LeaseNum, LeaserRoles, LogActionId, ReplicaId, RequestId,
    StateMachine, StorageHub, TransportHub,
};
use crate::utils::{Bitmap, Stopwatch, SummersetError};

use atomic_refcell::AtomicRefCell;

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::time::{self, Duration, Instant, Interval, MissedTickBehavior};

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfigQuorumLeases {
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

    /// Lease-related timeout duration in millisecs.
    pub lease_expire_ms: u64,

    /// Enable stable leader leases for leader local reads?
    pub enable_leader_leases: bool,

    /// Enable promptive CommitNotice sending for committed instances?
    pub urgent_commit_notice: bool,

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Maximum chunk size (in slots) of any bulk messages.
    pub msg_chunk_size: usize,

    /// Recording performance breakdown statistics?
    pub record_breakdown: bool,

    /// Recording the latest committed value version of a key?
    /// Only effective if record_breakdown is set to true.
    pub record_value_ver: bool,

    /// Recording total payload size received from per peer?
    /// Only effective if record_breakdown is set to true.
    pub record_size_recv: bool,

    // [for benchmarking purposes only]
    /// Simulate local read lease implementation?
    pub sim_read_lease: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigQuorumLeases {
    fn default() -> Self {
        ReplicaConfigQuorumLeases {
            batch_interval_ms: 1,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.quorum_leases.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 1500,
            hb_hear_timeout_max: 2000,
            hb_send_interval_ms: 20,
            disable_hb_timer: false,
            lease_expire_ms: 2000, // need proper hb settings if leasing
            enable_leader_leases: false,
            urgent_commit_notice: false,
            snapshot_path: "/tmp/summerset.quorum_leases.snap".into(),
            snapshot_interval_s: 0,
            msg_chunk_size: 10,
            record_breakdown: false,
            record_value_ver: false,
            record_size_recv: false,
            sim_read_lease: false,
        }
    }
}

/// Ballot number type. Use 0 as a null ballot number.
type Ballot = u64;

/// Instance status enum.
#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize,
)]
enum Status {
    Null = 0,
    Preparing = 1,
    Accepting = 2,
    Committed = 3,
    Executed = 4,
}

/// Request batch type (i.e., the "value" in Paxos).
type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// Leader-side bookkeeping info for each instance initiated.
#[derive(Debug, Clone)]
struct LeaderBookkeeping {
    /// If in Preparing status, the trigger_slot of this Prepare phase.
    trigger_slot: usize,

    /// If in Preparing status, the endprep_slot of this Prepare phase.
    endprep_slot: usize,

    /// Replicas from which I have received Prepare confirmations.
    /// This field is only tracked on the trigger_slot entry of the log.
    prepare_acks: Bitmap,

    /// Max ballot among received Prepare replies.
    prepare_max_bal: Ballot,

    /// Replicas from which I have received Accept confirmations.
    accept_acks: Bitmap,

    /// The grant_sets replied from grantors through their AcceptReplies (or
    /// updated through NoGrants message in cases of grantee unresponsiveness).
    /// Commit status cannot be reached until AcceptReplies from everyone in
    /// these sets have been received.
    accept_grant_sets: HashMap<ReplicaId, Bitmap>,
}

/// Follower-side bookkeeping info for each instance received.
#[derive(Debug, Clone)]
struct ReplicaBookkeeping {
    /// Source leader replica ID for replying to Prepares and Accepts.
    source: ReplicaId,

    /// If in Preparing status, the trigger_slot of this Prepare phase.
    trigger_slot: usize,

    /// If in Preparing status, the endprep_slot of this Prepare phase.
    endprep_slot: usize,
}

/// In-memory instance containing a commands batch.
#[derive(Debug, Clone)]
struct Instance {
    /// Ballot number.
    bal: Ballot,

    /// Instance status.
    status: Status,

    /// Batch of client requests.
    reqs: ReqBatch,

    /// Highest ballot and associated value I have accepted.
    voted: (Ballot, ReqBatch),

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
enum WalEntry {
    /// Records an update to the largest prepare ballot seen.
    PrepareBal { slot: usize, ballot: Ballot },

    /// Records a newly accepted request batch data at slot index.
    AcceptData {
        slot: usize,
        ballot: Ballot,
        reqs: ReqBatch,
    },

    /// Records an event of committing the instance at index.
    CommitSlot { slot: usize },
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
        /// First entry at the start of file: number of log instances covered
        /// by this snapshot file == the start slot index of in-mem log.
        start_slot: usize,
    },

    /// Set of key-value pairs to apply to the state.
    KVPairSet { pairs: HashMap<String, String> },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
enum PeerMsg {
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
        /// Map from slot index -> the accepted ballot number for that
        /// instance and the corresponding request batch value.
        voted: Option<(Ballot, ReqBatch)>,
        /// For stable majority-leased leader to determined when it is safe to
        /// start serving reads single-handedly.
        accept_bar: usize,
    },

    /// Accept message from leader to replicas.
    Accept {
        slot: usize,
        ballot: Ballot,
        reqs: ReqBatch,
    },

    /// Accept reply from replica to leader.
    AcceptReply {
        slot: usize,
        ballot: Ballot,
        /// Set of peers that possibly hold a lease granted from me during
        /// the Accept phase.
        grant_set: Bitmap,
        /// [for perf breakdown only]
        reply_ts: Option<SystemTime>,
    },

    /// Revocation confirmation from quorum lease grantor replica to leader,
    /// as a safety net against grantee unresponsiveness.
    NoGrants {
        ballot: Ballot,
        qlease_num: LeaseNum,
    },

    /// Peer-to-peer periodic heartbeat.
    Heartbeat {
        ballot: Ballot,
        /// For notifying followers about safe-to-commit slots (in a bit
        /// conservative way).
        commit_bar: usize,
        /// For leader step-up as well as conservative snapshotting purpose.
        exec_bar: usize,
        /// For conservative snapshotting purpose.
        snap_bar: usize,
    },

    /// Promptive notification of commits from leader to replicas.
    CommitNotice { ballot: Ballot, commit_bar: usize },
}

/// QuorumLeases server replica module.
pub(crate) struct QuorumLeasesReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Majority quorum size.
    quorum_cnt: u8,

    /// Configuration parameters struct.
    config: ReplicaConfigQuorumLeases,

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

    /// LeaseManager module for leader leases (i.e., the "default lease").
    llease_manager: LeaseManager,

    /// LeaseManager module for quorum leases.
    // NOTE: Technically, there should be multiple LeaseManager modules, each
    //       for a keyspace partition with a different leaser roles config.
    //       But this is non-essential for our evaluations for now and will be
    //       simulated by launching multiple consensus groups whenever needed.
    qlease_manager: LeaseManager,

    /// Who do I think is the effective leader of the cluster right now?
    leader: Option<ReplicaId>,

    /// The latest quorum lease number used in granting. It is always true that
    ///   commit_bar > qlease_num >= qlease_ver
    qlease_num: LeaseNum,

    /// The current quorum lease role assignment's version. Using committed
    /// instance slot number in which this config's committed as the "version"
    /// of this config.
    qlease_ver: usize,

    /// The current role assignment for quorum leases.
    qlease_cfg: LeaserRoles,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Start slot index of in-mem log after latest snapshot.
    start_slot: usize,

    /// Timer for taking a new autonomous snapshot.
    snapshot_interval: Interval,

    /// Largest ballot number that a leader has sent Prepare messages in.
    bal_prep_sent: Ballot,

    /// Largest ballot number that a leader knows has been safely prepared.
    bal_prepared: Ballot,

    /// Largest ballot number seen as acceptor.
    bal_max_seen: Ballot,

    /// Index of the first non-Accepted instance.
    accept_bar: usize,

    /// Map from peer ID (including my self) who replied to my Prepare -> its
    /// accept_bar then; this is for safe stable leader leases purpose.
    peer_accept_bar: HashMap<ReplicaId, usize>,

    /// Minimum of the max accept_bar among any majority set of peer_accept_bar;
    /// this is for safe stable leader leases purpose.
    peer_accept_max: usize,

    /// Index of the first non-committed instance.
    commit_bar: usize,

    /// Index of the first non-executed instance.
    /// It is always true that
    ///   exec_bar <= commit_bar <= accept_bar <= start_slot + insts.len()
    exec_bar: usize,

    /// Map from peer ID -> its latest exec_bar I know; this is for conservative
    /// snapshotting purpose.
    peer_exec_bar: HashMap<ReplicaId, usize>,

    /// Slot index before which it is safe to take snapshot.
    // NOTE: we are taking a conservative approach here that a snapshot
    //       covering an entry can be taken only when all servers have durably
    //       committed (and executed) that entry.
    snap_bar: usize,

    /// Map from key -> the highest slot number that (might) contain a write
    /// to that key. Useful for read optimizations.
    // NOTE: there probably are better ways to do such bookkeeping, but this is
    //       good enough for now unless the key space is disgustingly huge
    highest_slot: HashMap<String, usize>,

    /// Current durable WAL log file offset.
    wal_offset: usize,

    /// Current durable snapshot file offset.
    snap_offset: usize,

    /// Base time instant at startup, used as a reference zero timestamp.
    startup_time: Instant,

    /// Performance breakdown stopwatch if doing recording.
    bd_stopwatch: Option<Stopwatch>,

    /// Performance breakdown printing interval.
    bd_print_interval: Interval,

    /// Bandwidth utilization total bytes accumulators.
    bw_accumulators: HashMap<ReplicaId, usize>,
}

// QuorumLeasesReplica common helpers
impl QuorumLeasesReplica {
    /// Do I think I am the current effective leader?
    #[inline]
    fn is_leader(&self) -> bool {
        self.leader == Some(self.id)
    }

    /// Create an empty null instance.
    #[inline]
    fn null_instance(&self) -> Instance {
        Instance {
            bal: 0,
            status: Status::Null,
            reqs: Vec::new(),
            voted: (0, Vec::new()),
            leader_bk: None,
            replica_bk: None,
            external: false,
            wal_offset: 0,
        }
    }

    /// Locate the first null slot or append a null instance if no holes exist.
    fn first_null_slot(&mut self) -> usize {
        for s in self.exec_bar..(self.start_slot + self.insts.len()) {
            if self.insts[s - self.start_slot].status == Status::Null {
                return s;
            }
        }
        self.insts.push(self.null_instance());
        self.start_slot + self.insts.len() - 1
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
    /// Uses the `Status` enum type to represent different entry types.
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
        debug_assert!(cmd_idx <= (u32::MAX as usize) / 2);
        ((slot << 32) | cmd_idx) as CommandId
    }

    /// Decompose CommandId into slot index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let slot = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (slot, cmd_idx)
    }

    /// Special composition of a command ID used at read-only shortcuts.
    #[inline]
    fn make_ro_command_id(client: ClientId, req_id: RequestId) -> CommandId {
        debug_assert!(client <= (u32::MAX as ClientId));
        debug_assert!(req_id <= (u32::MAX as RequestId) / 2);
        ((client << 32) | 1 << 31 | req_id) as CommandId
    }
}

#[async_trait]
impl GenericReplica for QuorumLeasesReplica {
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
        let config = parsed_config!(config_str => ReplicaConfigQuorumLeases;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms, disable_hb_timer,
                                    lease_expire_ms, enable_leader_leases,
                                    urgent_commit_notice, snapshot_path,
                                    snapshot_interval_s, msg_chunk_size,
                                    record_breakdown, record_value_ver,
                                    record_size_recv, sim_read_lease)?;
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
        if config.lease_expire_ms < config.hb_hear_timeout_min {
            return logged_err!(
                "invalid config.lease_expire_ms '{}'",
                config.lease_expire_ms
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

        // setup leader leases management module
        let (llease_manager, tx_llease_msg) = LeaseManager::new_and_setup(
            id,
            population,
            Duration::from_millis(config.lease_expire_ms),
            Duration::from_millis(config.hb_send_interval_ms),
        )?;

        // setup leader leases management module
        let (qlease_manager, tx_qlease_msg) = LeaseManager::new_and_setup(
            id,
            population,
            Duration::from_millis(config.lease_expire_ms),
            Duration::from_millis(config.hb_send_interval_ms),
        )?;

        // setup transport hub module
        let mut transport_hub = TransportHub::new_and_setup(
            id,
            population,
            p2p_addr,
            HashMap::from([
                (
                    0, // gid 0 for leader leases
                    tx_llease_msg,
                ),
                (
                    1, // gid 1 for quorum leases
                    tx_qlease_msg,
                ),
            ]),
        )
        .await?;

        // ask for the list of peers to proactively connect to. Do this after
        // transport hub has been set up, so that I will be able to accept
        // later peer connections
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::QuorumLeases,
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

        // [for perf breakdown only]
        let bd_stopwatch = if config.record_breakdown {
            Some(Stopwatch::new())
        } else {
            None
        };
        let mut bd_print_interval = time::interval(Duration::from_secs(5));
        bd_print_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Ok(QuorumLeasesReplica {
            id,
            population,
            quorum_cnt: (population / 2) + 1,
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
            llease_manager,
            qlease_manager,
            leader: None,
            qlease_num: 0,
            qlease_ver: 0,
            qlease_cfg: LeaserRoles::empty(population),
            insts: vec![],
            start_slot: 0,
            snapshot_interval,
            bal_prep_sent: 0,
            bal_prepared: 0,
            bal_max_seen: 0,
            accept_bar: 0,
            peer_accept_bar: (0..population)
                .filter_map(
                    |s| if s == id { None } else { Some((s, usize::MAX)) },
                )
                .collect(),
            peer_accept_max: usize::MAX,
            commit_bar: 0,
            exec_bar: 0,
            peer_exec_bar: (0..population)
                .filter_map(|s| if s == id { None } else { Some((s, 0)) })
                .collect(),
            snap_bar: 0,
            highest_slot: HashMap::new(),
            wal_offset: 0,
            snap_offset: 0,
            startup_time: Instant::now(),
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
                            if let Err(e) = self.become_a_leader().await {
                                pf_error!("error becoming a leader: {}", e);
                            }
                        }
                        HeartbeatEvent::SendTicked => {
                            if let Err(e) = self.bcast_heartbeats().await {
                                pf_error!("error broadcasting heartbeats: {}", e);
                            }
                        }
                    }
                },

                // leader lease-related action
                lease_action = self.llease_manager.get_action(), if !paused
                                                                    && self.config.enable_leader_leases => {
                    if let Err(e) = lease_action {
                        pf_error!("error getting llease action: {}", e);
                        continue;
                    }
                    let (lease_num, lease_action) = lease_action.unwrap();
                    if let Err(e) = self.handle_llease_action(lease_num, lease_action).await {
                        pf_error!("error handling llease action @ {}: {}", lease_num, e);
                    }
                },

                // quorum lease-related action
                lease_action = self.qlease_manager.get_action(), if !paused => {
                    if let Err(e) = lease_action {
                        pf_error!("error getting qlease action: {}", e);
                        continue;
                    }
                    let (lease_num, lease_action) = lease_action.unwrap();
                    if let Err(e) = self.handle_qlease_action(lease_num, lease_action).await {
                        pf_error!("error handling qlease action @ {}: {}", lease_num, e);
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

                // performance breakdown stats printing
                _ = self.bd_print_interval.tick(), if !paused && self.config.record_breakdown => {
                    if self.is_leader() {
                        if let Some(sw) = self.bd_stopwatch.as_mut() {
                            let (cnt, stats) = sw.summarize(4);
                            pf_info!("bd cnt {} ldur {:.2} {:.2} arep {:.2} {:.2} \
                                                qrum {:.2} {:.2} exec {:.2} {:.2}",
                                      cnt, stats[0].0, stats[0].1, stats[1].0, stats[1].1,
                                           stats[2].0, stats[2].1, stats[3].0, stats[3].1);
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

    fn population(&self) -> u8 {
        self.population
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigQuorumLeases {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,

    /// App-designated nearest server ID for near read attempts.
    pub near_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigQuorumLeases {
    fn default() -> Self {
        ClientConfigQuorumLeases {
            init_server_id: 0,
            near_server_id: 0,
        }
    }
}

/// QuorumLeases client-side module.
pub(crate) struct QuorumLeasesClient {
    /// Client ID.
    id: ClientId,

    /// Number of servers in the cluster.
    population: u8,

    /// Configuration parameters struct.
    _config: ClientConfigQuorumLeases,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Current server ID to talk to for normal consensus commands.
    curr_server_id: ReplicaId,

    /// App-designated nearest server ID for near read attempts. Could become
    /// different from `config.near_server_id` if the latter is deemed inactive.
    near_server_id: ReplicaId,

    /// Last server ID I sent something to unsuccessfully; used here to help
    /// with retrying.
    last_server_id: Option<ReplicaId>,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stubs for communicating with servers.
    api_stubs: HashMap<ReplicaId, AtomicRefCell<ClientApiStub>>,
}

#[async_trait]
impl GenericEndpoint for QuorumLeasesClient {
    async fn new_and_setup(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub = ClientCtrlStub::new_by_connect(manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigQuorumLeases;
                                    init_server_id, near_server_id)?;
        let curr_server_id = config.init_server_id;
        let near_server_id = config.near_server_id;

        Ok(QuorumLeasesClient {
            id,
            population: 0,
            _config: config,
            servers: HashMap::new(),
            curr_server_id,
            near_server_id,
            last_server_id: None,
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

                // shift to a new server_id if current one not active
                debug_assert!(!servers_info.is_empty());
                while !servers_info.contains_key(&self.curr_server_id)
                    || servers_info[&self.curr_server_id].is_paused
                {
                    self.curr_server_id =
                        (self.curr_server_id + 1) % population;
                }
                if !servers_info.contains_key(&self.near_server_id)
                    || servers_info[&self.near_server_id].is_paused
                {
                    pf_warn!(
                        "near server {} inactive, using {} instead...",
                        self.near_server_id,
                        self.curr_server_id
                    );
                    self.near_server_id = self.curr_server_id;
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
                    self.api_stubs.insert(id, AtomicRefCell::new(api_stub));
                }
                Ok(())
            }
            _ => logged_err!("unexpected reply type received"),
        }
    }

    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        // send leave notification to all servers
        for (id, api_stub) in self.api_stubs.drain() {
            let mut sent =
                api_stub.borrow_mut().send_req(Some(&ApiRequest::Leave))?;
            while !sent {
                sent = api_stub.borrow_mut().send_req(None)?;
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
        let server_id = match req {
            None => {
                // last send needs retry
                if let Some(last_server_id) = self.last_server_id {
                    last_server_id
                } else {
                    return logged_err!("last_server_id not set when retrying");
                }
            }
            Some(req) if req.read_only() => {
                // read-only request and doing near quorum reads
                self.near_server_id
            }
            _ => {
                // normal consensus command
                self.curr_server_id
            }
        };

        if self.api_stubs.contains_key(&server_id) {
            let success = self
                .api_stubs
                .get_mut(&server_id)
                .unwrap()
                .borrow_mut()
                .send_req(req)?;
            self.last_server_id = if success { None } else { Some(server_id) };
            Ok(success)
        } else {
            Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                server_id
            )))
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        if !self.api_stubs.contains_key(&self.curr_server_id) {
            return Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                self.curr_server_id
            )));
        }
        if !self.api_stubs.contains_key(&self.near_server_id) {
            return Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                self.near_server_id
            )));
        }

        let mut reply = if self.curr_server_id == self.near_server_id {
            // curr is the same as near, so just wait on that stub
            self.api_stubs
                .get(&self.curr_server_id)
                .unwrap()
                .borrow_mut()
                .recv_reply()
                .await?
        } else {
            // don't know which one would have reply come in first, so need to do
            // a `tokio::select!` here
            let mut curr_stub = self
                .api_stubs
                .get(&self.curr_server_id)
                .unwrap()
                .borrow_mut();
            let mut near_stub = self
                .api_stubs
                .get(&self.near_server_id)
                .unwrap()
                .borrow_mut();
            tokio::select! {
                reply = curr_stub.recv_reply() => reply?,
                reply = near_stub.recv_reply() => reply?,
            }
        };

        if let ApiReply::Reply {
            id: req_id,
            ref result,
            ref redirect,
            ref mut rq_retry,
        } = reply
        {
            // if to retry a failed read-only optimization, just fallback to
            // sending the request to (believed) current leader and continue
            // the wait
            if let Some(read_cmd) = rq_retry.take() {
                if !read_cmd.read_only() {
                    return logged_err!(
                        "non-Get command found in reply rq_retry"
                    );
                }
                while self.last_server_id.is_some() {
                    self.send_req(None)?;
                }
                self.api_stubs
                    .get(&self.curr_server_id)
                    .unwrap()
                    .borrow_mut()
                    .send_req(Some(&ApiRequest::Req {
                        id: req_id,
                        cmd: read_cmd,
                    }))?;
                return self.recv_reply().await;
            }

            // if the current server redirects me to a different server
            if result.is_none() && redirect.is_some() {
                let redirect_id = redirect.unwrap();
                debug_assert!(self.servers.contains_key(&redirect_id));
                self.curr_server_id = redirect_id;
                pf_debug!(
                    "redirected to replica {} '{}'",
                    redirect_id,
                    self.servers[&redirect_id]
                );
            }
        }

        Ok(reply)
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
