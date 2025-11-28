//! Replication protocol: `Bodega`.
//!
//! Uses novel all-to-all background config leases to maintain global agreement
//! on the configuration of responder nodes, then applies optimal critical path
//! logic to allow almost-always local reads at responders.
//!
//! NOTE: only keys of format 'k<number>' are currently range-mappable

use bincode::{Decode, Encode};
mod conflease;
mod control;
mod durability;
mod execution;
mod heartbeat;
mod localread;
mod messages;
mod recovery;
mod request;
mod snapshot;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;

use async_trait::async_trait;
use atomic_refcell::AtomicRefCell;
use get_size::GetSize;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::time::{self, Duration, Interval, MissedTickBehavior};

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, Command, CommandId, CommandResult, ControlHub,
    ExternalApi, GenericReplica, HeartbeatEvent, Heartbeater, LeaseManager,
    LogActionId, ReplicaId, RequestId, StateMachine, StorageHub, TransportHub,
};
use crate::utils::{Bitmap, RespondersConf, SummersetError, Timer};

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ReplicaConfigBodega {
    /// Client request batching interval in millisecs.
    pub batch_interval_ms: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing log file.
    pub backer_path: String,

    /// Whether to call `fsync()`/`fdatasync()` on logger.
    pub logger_sync: bool,

    /// Min timeout of not hearing any heartbeat from peer in millisecs.
    pub hb_hear_timeout_min: u64,
    /// Max timeout of not hearing any heartbeat from peer in millisecs.
    // NOTE: technically Bodega does not need heartbeat time vairations for
    //       most peer-to-peer heartbeats, but we'll just keep this infra here
    pub hb_hear_timeout_max: u64,

    /// Interval of leader sending heartbeats to peers in millisecs.
    pub hb_send_interval_ms: u64,

    /// Disable heartbeat timer (to force a deterministic leader during tests).
    pub disable_hb_timer: bool,

    /// Disallow me to ever attempt stepping up as leader?
    pub disallow_step_up: bool,

    /// Lease-related timeout duration in millisecs.
    pub lease_expire_ms: u64,

    /// Enable promptive `CommitNotice` sending for committed instances?
    pub urgent_commit_notice: bool,

    /// Enable promptive `AcceptReply` broadcast among non-leader peers?
    pub urgent_accept_notice: bool,

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Maximum chunk size (in slots) of any bulk messages.
    pub msg_chunk_size: usize,

    // [for perf breakdown only]
    /// Recording performance breakdown statistics?
    pub record_breakdown: bool,

    // [for access cnt stats only]
    /// Recording critical-path server access statistics?
    /// Only effective if `record_breakdown` is set to true.
    pub record_node_cnts: bool,

    // [for benchmarking purposes only]
    /// Simulate local read lease implementation?
    pub sim_read_lease: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigBodega {
    fn default() -> Self {
        ReplicaConfigBodega {
            batch_interval_ms: 1,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.bodega.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 1200,
            hb_hear_timeout_max: 2000,
            hb_send_interval_ms: 20,
            disable_hb_timer: false,
            disallow_step_up: false,
            lease_expire_ms: 2000, // need proper hb settings if leasing
            urgent_commit_notice: false,
            urgent_accept_notice: false,
            snapshot_path: "/tmp/summerset.bodega.snap".into(),
            snapshot_interval_s: 0,
            msg_chunk_size: 10,
            record_breakdown: false,
            record_node_cnts: false,
            sim_read_lease: false,
        }
    }
}

/// Ballot number type. Use 0 as a null ballot number.
type Ballot = u64;

/// `Instance` status enum.
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
    Encode,
    Decode,
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
    /// If in Preparing status, the `trigger_slot` of this Prepare phase.
    trigger_slot: usize,

    /// If in Preparing status, the `endprep_slot` of this Prepare phase.
    endprep_slot: usize,

    /// Replicas from which I have received Prepare confirmations.
    /// This field is only tracked on the `trigger_slot` entry of the log.
    prepare_acks: Bitmap,

    /// Max ballot among received Prepare replies.
    prepare_max_bal: Ballot,

    /// Replicas from which I have received Accept confirmations.
    accept_acks: Bitmap,
}

/// Follower-side bookkeeping info for each instance received.
#[derive(Debug, Clone)]
struct ReplicaBookkeeping {
    /// Source leader replica ID for replying to Prepares and Accepts.
    source: ReplicaId,

    /// If in Preparing status, the `trigger_slot` of this Prepare phase.
    trigger_slot: usize,

    /// If in Preparing status, the `endprep_slot` of this Prepare phase.
    endprep_slot: usize,

    /// Follower-to-follower early `AcceptNotices` corresponding ballot.
    accept_notices_bal: Ballot,

    /// Follower-to-follower early `AcceptNotices` received.
    accept_notices: Bitmap,

    /// Follower local read client requests holding queue.
    holding_reads: ReqBatch,
}

/// In-memory instance containing a commands batch.
#[derive(Debug, Clone)]
struct Instance {
    /// Ballot number.
    bal: Ballot,

    /// `Instance` status.
    status: Status,

    /// Batch of client requests. This field is overwritten directly when
    /// receiving `PrepareReplies`; this is just a small engineering choice
    /// to avoid storing the full set of replies in `LeaderBookkeeping`.
    reqs: ReqBatch,

    /// Highest ballot and associated value I have accepted; this field is
    /// required to support correct Prepare phase replies.
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
#[derive(
    Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Encode, Decode, GetSize,
)]
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
#[derive(
    Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Encode, Decode, GetSize,
)]
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
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, GetSize)]
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
        /// In our implementation, we choose to break the `PrepareReply` into
        /// slot-wise messages for simplicity.
        slot: usize,
        /// Also carry the `trigger_slot` information to make it easier for the
        /// leader to track reply progress.
        trigger_slot: usize,
        /// Due to the slot-wise design choice, we need a way to let leader
        /// know when have all `PrepareReplies` been received. We use the
        /// `endprep_slot` field to convey this: when all slots' `PrepareReplies`
        /// up to `endprep_slot` are received, the "wholesome" `PrepareReply`
        /// can be considered received.
        // NOTE: this currently assumes the "ordering" property of TCP.
        endprep_slot: usize,
        ballot: Ballot,
        /// Map from slot index -> the accepted ballot number for that
        /// instance and the corresponding request batch value.
        voted: Option<(Ballot, ReqBatch)>,
    },

    /// Accept message from leader to replicas.
    Accept {
        slot: usize,
        ballot: Ballot,
        reqs: ReqBatch,
    },

    /// Accept reply from replica to leader.
    AcceptReply { slot: usize, ballot: Ballot },

    /// Peer-to-peer periodic heartbeat.
    Heartbeat {
        ballot: Ballot,
        conf: RespondersConf<()>,
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

    /// Early Accept notice from replica to peer follower.
    AcceptNotice { slot: usize, ballot: Ballot },
}

/// `Bodega` server replica module.
pub(crate) struct BodegaReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Majority quorum size.
    quorum_cnt: u8,

    /// Configuration parameters struct.
    // NOTE: this is not the "config" of leader & responders; see `bodega_conf`.
    config: ReplicaConfigBodega,

    /// Address string for client requests API.
    _api_addr: SocketAddr,

    /// Address string for internal peer-peer communication.
    _p2p_addr: SocketAddr,

    /// `ControlHub` module.
    control_hub: ControlHub,

    /// `ExternalApi` module.
    external_api: ExternalApi,

    /// `StateMachine` module.
    state_machine: StateMachine,

    /// `StorageHub` module.
    storage_hub: StorageHub<WalEntry>,

    /// `StorageHub` module for the snapshot file.
    snapshot_hub: StorageHub<SnapEntry>,

    /// `TransportHub` module.
    transport_hub: TransportHub<PeerMsg>,

    /// Heartbeater module.
    heartbeater: Heartbeater,

    /// Special timer for auto leader step-up in cases where all nodes are
    /// alive and heartbeating with each other but the current configuration
    /// has no leader.
    volunteer_timer: Timer,

    /// `LeaseManager` module.
    lease_manager: LeaseManager,

    /// Most up-to-date cluster roles config that I know of. A config includes:
    ///   - who is supposed to be the stable leader
    ///   - for each key range: who are the responders serving reads locally
    bodega_conf: RespondersConf<()>,

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
    /// `accept_bar` then; this is for safe stable leader leases purpose.
    peer_accept_bar: HashMap<ReplicaId, usize>,

    /// Minimum of the max `accept_bar` among any majority set of `peer_accept_bar`;
    /// this is for safe stable leader leases purpose.
    peer_accept_max: usize,

    /// Index of the first non-committed instance.
    commit_bar: usize,

    /// Index of the first non-executed instance.
    /// It is always true that
    ///   `exec_bar` <= `commit_bar` <= `start_slot` + `insts.len()`
    exec_bar: usize,

    /// Map from peer ID -> its latest `exec_bar` I know; this is for conservative
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

    // [for perf breakdown only]
    /// Performance breakdown printing interval.
    bd_print_interval: Interval,

    // [for access cnt stats only]
    /// Critical-path server acccess counts statistics.
    node_cnts_stats: HashMap<ReplicaId, u64>,
}

// BodegaReplica common helpers
impl BodegaReplica {
    /// Do I think I am the current effective leader?
    #[inline]
    fn is_leader(&self) -> bool {
        self.bodega_conf.is_leader(self.id)
    }

    /// Create an empty null instance.
    #[inline]
    #[allow(clippy::unused_self)]
    fn null_instance(&self) -> Instance {
        Instance {
            bal: 0,
            status: Status::Null,
            reqs: ReqBatch::new(),
            voted: (0, ReqBatch::new()),
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
        ((base << 8) | u64::from(self.id + 1)) as Ballot
    }

    /// Compose a unique ballot number greater than the given one.
    #[inline]
    fn make_greater_ballot(&self, bal: Ballot) -> Ballot {
        self.make_unique_ballot((bal >> 8) + 1)
    }

    /// Compose `LogActionId` from slot index & entry type.
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

    /// Decompose `LogActionId` into slot index & entry type.
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

    /// Compose `CommandId` from slot index & command index within.
    #[inline]
    fn make_command_id(slot: usize, cmd_idx: usize) -> CommandId {
        debug_assert!(u32::try_from(slot).is_ok());
        debug_assert!(cmd_idx <= (u32::MAX as usize) / 2);
        ((slot << 32) | cmd_idx) as CommandId
    }

    /// Decompose `CommandId` into slot index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let slot = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (slot, cmd_idx)
    }

    /// Special composition of a command ID used at read-only shortcuts.
    #[inline]
    fn make_ro_command_id(client: ClientId, req_id: RequestId) -> CommandId {
        debug_assert!(client <= ClientId::from(u32::MAX));
        debug_assert!(req_id <= RequestId::from(u32::MAX) / 2);
        ((client << 32) | (1 << 31) | req_id) as CommandId
    }
}

#[async_trait]
impl GenericReplica for BodegaReplica {
    #[allow(clippy::too_many_lines)]
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
        let config = parsed_config!(config_str => ReplicaConfigBodega;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms, disable_hb_timer,
                                    disallow_step_up, lease_expire_ms,
                                    urgent_commit_notice, urgent_accept_notice,
                                    snapshot_path, snapshot_interval_s,
                                    msg_chunk_size, record_breakdown,
                                    record_node_cnts, sim_read_lease)?;
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

        // setup lease management module
        let (lease_manager, tx_lease_msg) = LeaseManager::new_and_setup(
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
            HashMap::from([(
                0, // only one lease purpose exists in Bodega
                tx_lease_msg,
            )]),
        )
        .await?;

        // ask for the list of peers to proactively connect to. Do this after
        // transport hub has been set up, so that I will be able to accept
        // later peer connections
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::Bodega,
            api_addr,
            p2p_addr,
        })?;
        let CtrlMsg::ConnectToPeers { to_peers, .. } =
            control_hub.recv_ctrl().await?
        else {
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
        let mut bd_print_interval = time::interval(Duration::from_secs(5));
        bd_print_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Ok(BodegaReplica {
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
            volunteer_timer: Timer::new::<fn()>(true, None, false),
            lease_manager,
            bodega_conf: RespondersConf::empty(population),
            insts: vec![],
            start_slot: 0,
            snapshot_interval,
            bal_prep_sent: 0,
            bal_prepared: 0,
            bal_max_seen: 0,
            accept_bar: 0,
            peer_accept_bar: (0..population).map(|s| (s, usize::MAX)).collect(),
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
            bd_print_interval,
            node_cnts_stats: (0..population).map(|s| (s, 0)).collect(),
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
        // recover state from durable snapshot file
        self.recover_from_snapshot().await?;

        // recover the tail-piece memory log & state from durable WAL log
        self.recover_from_wal().await?;

        // kick off peer heartbeats hearing timer
        self.refresh_heartbeat_timer(None)?;
        self.refresh_volunteer_timer()?;

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
                    if let Err(e) = hb_event {
                        pf_error!("error getting heartbeat event: {}", e);
                        continue;
                    }
                    match hb_event.unwrap() {
                        HeartbeatEvent::HearTimeout { peer } => {
                            if let Err(e) = self.heartbeat_timeout(peer).await {
                                pf_error!("error handling heartbeat timeout: {}", e);
                            }
                        }
                        HeartbeatEvent::SendTicked => {
                            if let Err(e) = self.bcast_heartbeats().await {
                                pf_error!("error broadcasting heartbeats: {}", e);
                            }
                        }
                    }
                },

                // volunteer timer timeout
                () = self.volunteer_timer.timeout(), if !paused => {
                    if let Err(e) = self.heartbeat_timeout(self.id).await {
                        pf_error!("error volunteering to be a leader: {}", e);
                    }
                }

                // lease-related action
                lease_action = self.lease_manager.get_action(), if !paused => {
                    if let Err(e) = lease_action {
                        pf_error!("error getting lease action: {}", e);
                        continue;
                    }
                    let (lease_num, lease_action) = lease_action.unwrap();
                    if let Err(e) = self.handle_lease_action(lease_num, lease_action).await {
                        pf_error!("error handling lease action @ {}: {}", lease_num, e);
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

                // [for perf breakdown only]
                // performance breakdown stats printing
                _ = self.bd_print_interval.tick(), if !paused && self.config.record_breakdown => {
                    if self.config.record_node_cnts {
                        pf_info!("node cnts stats {}",
                                 self.node_cnts_stats
                                     .iter()
                                     .map(|(peer, cnt)| format!("{}:{}", peer, cnt))
                                     .collect::<Vec<String>>()
                                     .join(" "));
                        for cnt in self.node_cnts_stats.values_mut() {
                            *cnt = 0;
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
pub struct ClientConfigBodega {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,

    /// App-designated nearest server ID for near read attempts.
    /// Any number that's larger than cluster size will be treated as `None`,
    /// i.e., no near server preference.
    pub near_server_id: ReplicaId,

    /// Timeout for a local read from which I will try to directly contact
    /// the current leader for the read. A value of `0` means don't do this.
    pub local_read_unhold_ms: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigBodega {
    fn default() -> Self {
        ClientConfigBodega {
            init_server_id: 0,
            near_server_id: ReplicaId::MAX,
            local_read_unhold_ms: 200,
        }
    }
}

/// `Bodega` client-side module.
pub(crate) struct BodegaClient {
    /// Client ID.
    id: ClientId,

    /// Number of servers in the cluster.
    population: u8,

    /// Configuration parameters struct.
    config: ClientConfigBodega,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Current server ID to talk to for normal consensus commands.
    curr_server_id: ReplicaId,

    /// App-designated nearest server ID for near read attempts. If `None`,
    /// means no near server preference is specified; otherwise, still could
    /// become different from `config.near_server_id` if the latter is deemed
    /// inactive.
    near_server_id: Option<ReplicaId>,

    /// Last server ID I sent something to unsuccessfully; used here to help
    /// with retrying.
    last_server_id: Option<ReplicaId>,

    /// Map from all pending read commands' request IDs -> their command key
    /// and their corresponding try-leader timer.
    pending_reads: HashMap<RequestId, (String, Timer)>,

    /// Sender side of try-leader timeout events, to be cloned into timers.
    tx_try_leader: mpsc::UnboundedSender<RequestId>,

    /// Receiver side of try-leader timeout events.
    rx_try_leader: mpsc::UnboundedReceiver<RequestId>,

    /// Count of contiguous try-leader events triggered. If too high, should
    /// deem the preferred near server as failed.
    try_leader_streak: u8,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stubs for communicating with servers.
    api_stubs: HashMap<ReplicaId, AtomicRefCell<ClientApiStub>>,
}

#[async_trait]
impl GenericEndpoint for BodegaClient {
    async fn new_and_setup(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_debug!("connecting to manager '{}'...", manager);
        let ctrl_stub = ClientCtrlStub::new_by_connect(manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigBodega;
                                      init_server_id, near_server_id,
                                      local_read_unhold_ms)?;
        let curr_server_id = config.init_server_id;

        let (tx_try_leader, rx_try_leader) = mpsc::unbounded_channel();

        Ok(BodegaClient {
            id,
            population: 0,
            config,
            servers: HashMap::new(),
            curr_server_id,
            near_server_id: None,
            last_server_id: None,
            pending_reads: HashMap::new(),
            tx_try_leader,
            rx_try_leader,
            try_leader_streak: 0,
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
        if let CtrlReply::QueryInfo {
            population,
            servers_info,
        } = reply
        {
            self.population = population;

            // shift to a new server_id if current one not active
            debug_assert!(!servers_info.is_empty());
            while !servers_info.contains_key(&self.curr_server_id)
                || servers_info[&self.curr_server_id].is_paused
            {
                self.curr_server_id = (self.curr_server_id + 1) % population;
            }
            if self.config.near_server_id < population {
                let mut near_server_id = self.config.near_server_id;
                if !servers_info.contains_key(&near_server_id)
                    || servers_info[&near_server_id].is_paused
                {
                    pf_warn!(
                        "near server {} inactive, using {} instead...",
                        near_server_id,
                        self.curr_server_id
                    );
                    near_server_id = self.curr_server_id;
                }
                self.near_server_id = Some(near_server_id);
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
        } else {
            logged_err!("unexpected reply type received")
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
                // last send needs retry (not a read retry, but just a msg
                // send retry)
                if let Some(last_server_id) = self.last_server_id {
                    last_server_id
                } else {
                    return logged_err!("last_server_id not set when retrying");
                }
            }
            Some(req)
                if req.read_only().is_some()
                    && self.near_server_id.is_some() =>
            {
                // read-only request and doing near quorum reads
                self.near_server_id.unwrap()
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

            // if successfully sent a read only request, kick off a try-leader
            // timer for it and bookkeep it
            if success
                && self.config.local_read_unhold_ms > 0
                && let Some(ApiRequest::Req {
                    id: req_id,
                    cmd: Command::Get { key },
                }) = req
            {
                // NOTE: for closed-loop clients we can make the timer once
                //       and reuse it for requests, but we are here creating
                //       a new one every time to support open-loop clients
                //       as well; overhead should be negligible
                // NOTE: for open-loop clients, assumes that replies are
                //       harvested promptly, otherwise unnecessary false
                //       negatives will fire
                let req_id = *req_id;
                let tx_try_leader_ref = self.tx_try_leader.clone();
                let timer = Timer::new(
                    false,
                    Some(move || {
                        tx_try_leader_ref.send(req_id).expect(
                            "sending to tx_try_leader_ref should succeed",
                        );
                    }),
                    false,
                );
                timer.kickoff(Duration::from_millis(
                    self.config.local_read_unhold_ms,
                ))?;
                self.pending_reads.insert(req_id, (key.clone(), timer));
            }

            Ok(success)
        } else {
            Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                server_id
            )))
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        if !self.api_stubs.contains_key(&self.curr_server_id) {
            return Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                self.curr_server_id
            )));
        }
        if let Some(near_server_id) = self.near_server_id
            && near_server_id != self.curr_server_id
            && !self.api_stubs.contains_key(&near_server_id)
        {
            return Err(SummersetError::msg(format!(
                "server_id {} not in api_stubs",
                near_server_id
            )));
        }

        let mut reply = if self
            .near_server_id
            .as_ref()
            .is_none_or(|&s| s == self.curr_server_id)
        {
            // curr is the same as near, so just wait on that stub
            let reply = self
                .api_stubs
                .get(&self.curr_server_id)
                .unwrap()
                .borrow_mut()
                .recv_reply()
                .await?;

            self.try_leader_streak = 0;
            reply
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
                .get(&self.near_server_id.unwrap())
                .unwrap()
                .borrow_mut();

            loop {
                tokio::select! {
                    // recv from currently believed leader server
                    reply = curr_stub.recv_reply() => {
                        break reply?;
                    },

                    // recv from preferred near server
                    reply = near_stub.recv_reply() => {
                        self.try_leader_streak = 0;
                        break reply?;
                    },

                    // checking for try-leader timeout events
                    req_id = self.rx_try_leader.recv() => {
                        let req_id = req_id.unwrap();
                        if let Some((key, _)) =
                            self.pending_reads.get(&req_id)
                        {
                            if self.last_server_id.is_some() {
                                // during msg send retry, just put this event
                                // back and will handle it later
                                self.tx_try_leader.send(req_id)?;
                            } else {
                                self.try_leader_streak += 1;
                                if self.try_leader_streak >= 10 { // hardcoded
                                    // too many try-leader timeouts in a row;
                                    // return a "failure" reply
                                    break ApiReply::Reply {
                                        id: req_id,
                                        result: None,
                                        redirect: None,
                                        rq_retry: None,
                                    }
                                }

                                curr_stub.send_req(Some(&ApiRequest::Req {
                                    id: req_id,
                                    cmd: Command::Get { key: key.clone() }
                                }))?;

                            }
                        }
                    }
                }
            }
        };

        if let ApiReply::Reply {
            id: req_id,
            ref result,
            ref redirect,
            ref mut rq_retry,
        } = reply
        {
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

            // if this request is in pending_reads, this is the first reply we
            // got for it from whichever source, so remove from pending_reads;
            // this should also drop the associated timer and thus its timeout
            // event won't fire
            self.pending_reads.remove(&req_id);

            // if to retry a failed read-only optimization, just fallback to
            // sending the request to (believed) current leader and continue
            // the wait
            if let Some(read_cmd) = rq_retry.take() {
                if read_cmd.read_only().is_none() {
                    return logged_err!(
                        "non-Get command found in reply rq_retry"
                    );
                }
                while self.last_server_id.is_some() {
                    self.send_req(None)?;
                    if self.last_server_id.is_some() {
                        time::sleep(Duration::from_millis(5)).await; // slack
                    }
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
