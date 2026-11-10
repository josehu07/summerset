//! Replication protocol: `CRaft` (Coded-`Raft`).
//!
//! `Raft` with erasure coding and fall-back mechanism. References:
//!   - <https://www.usenix.org/conference/fast20/presentation/wang-zizhong>

use wincode::{SchemaRead, SchemaWrite};
mod control;
mod durability;
mod execution;
mod leadership;
mod messages;
mod recovery;
mod request;
mod snapshot;

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;

use async_trait::async_trait;
use get_size::GetSize;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::{self, Duration, Instant, Interval, MissedTickBehavior};

use crate::client::{ClientApiStub, ClientCtrlStub, ClientId, GenericEndpoint};
use crate::manager::{CtrlMsg, CtrlReply, CtrlRequest};
use crate::protocols::SmrProtocol;
use crate::server::{
    ApiReply, ApiRequest, CommandId, ControlHub, ExternalApi, GenericReplica, HeartbeatEvent,
    Heartbeater, LogAction, LogResult, ReplicaId, Session, StateMachine, StorageHub, TransportHub,
};
use crate::utils::{Bitmap, RSCodeword, SummersetError};

/// Configuration parameters struct.
#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
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

    /// Interval of leader sending `AppendEntries` heartbeats to followers.
    pub hb_send_interval_ms: u64,

    /// Disable heartbeat timer (to force a deterministic leader during tests).
    pub disable_hb_timer: bool,

    /// Disallow me to ever attempt stepping up as leader?
    pub disallow_step_up: bool,

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Fault-tolerance level.
    pub fault_tolerance: u8,

    /// Maximum chunk size of any bulk of messages.
    pub msg_chunk_size: usize,

    // [for benchmarking purposes only]
    /// Simulate local read lease implementation?
    pub sim_read_lease: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigCRaft {
    fn default() -> Self {
        ReplicaConfigCRaft {
            batch_interval_ms: 1,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.craft.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 1200,
            hb_hear_timeout_max: 2000,
            hb_send_interval_ms: 20,
            disable_hb_timer: false,
            disallow_step_up: false,
            snapshot_path: "/tmp/summerset.craft.snap".into(),
            snapshot_interval_s: 0,
            fault_tolerance: 0,
            msg_chunk_size: 10,
            sim_read_lease: false,
        }
    }
}

/// Term number type, defined for better code readability.
type Term = u64;

/// Request batch type (i.e., the "command" in an entry).
//
// NOTE: the originally presented Raft algorithm does not explicitly mention
//       batching, but instead hides it with the heartbeats: every AppendEntries
//       RPC from the leader basically batches all commands it has received
//       since the last sent heartbeat. Here, to make this implementation more
//       comparable to MultiPaxos, we trigger batching also explicitly.
type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// In-mem + persistent entry of log, containing a term and a (possibly
/// partial) commands batch.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
struct LogEntry {
    /// Term number.
    term: Term,

    /// Shards of a batch of client requests.
    reqs_cw: RSCodeword<ReqBatch>,

    /// True if replicated as full copies (all data shards to each replica),
    /// else as one shard per replica; set by the leader appending (or
    /// upgrading) it.
    full_copy: bool,

    /// True if from external client, else false.
    external: bool,

    /// Offset in durable log file of this entry. This field is not maintained
    /// in durable storage itself, where it is typically `None`. It is
    /// maintained only in the in-memory log.
    log_offset: Option<usize>,
}

/// Stable storage log entry type.
//
// NOTE: Raft makes the persistent log exactly mirror the in-memory log, so
//       the backer file is not a WAL log in runtime operation; it might get
//       overwritten, etc.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
enum DurEntry {
    /// Durable metadata.
    Metadata {
        curr_term: Term,
        /// Not using `Option<ReplicaId>` here to ensure it serdes to fixed size.
        /// `None` is mapped to `ReplicaId::MAX`.
        voted_for: ReplicaId,
    },

    /// Log entry mirroring in-mem log.
    LogEntry { entry: LogEntry },
}

impl DurEntry {
    fn pack_meta(curr_term: Term, voted_for: Option<ReplicaId>) -> Self {
        DurEntry::Metadata {
            curr_term,
            voted_for: voted_for.unwrap_or(ReplicaId::MAX),
        }
    }

    fn unpack_meta(self) -> Result<(Term, Option<ReplicaId>), SummersetError> {
        if let DurEntry::Metadata {
            curr_term,
            voted_for,
        } = self
        {
            Ok((
                curr_term,
                if voted_for == ReplicaId::MAX {
                    None
                } else {
                    Some(voted_for)
                },
            ))
        } else {
            logged_err!("unpacking non Metadata entry")
        }
    }
}

/// Context of a durable logging action, given back with its result to tell
/// what the logged entry was about.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum LogCtx {
    /// Leader appending its own new entry at slot.
    LeaderAppend { slot: usize, term: Term },
    /// Follower appending the entry at slot out of an `AppendEntries` in term,
    /// whose entries end at `end_slot`.
    FollowerAppend {
        slot: usize,
        end_slot: usize,
        term: Term,
    },
}

impl LogCtx {
    /// Slot index of the logged entry.
    fn slot(self) -> usize {
        match self {
            Self::LeaderAppend { slot, .. } | Self::FollowerAppend { slot, .. } => slot,
        }
    }
}

/// Snapshot file entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
enum SnapEntry {
    /// Necessary slot indices to remember.
    SlotInfo {
        /// First entry at the start of file: number of log entries covered
        /// by this snapshot file == the start slot index of remaining log.
        start_slot: usize,
    },

    /// Set of key-value pairs to apply to the state, and client sessions as
    /// of then.
    KVPairSet {
        pairs: HashMap<String, String>,
        sessions: Vec<(ClientId, Session)>,
    },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead, GetSize)]
enum PeerMsg {
    /// `AppendEntries` from leader to followers.
    AppendEntries {
        term: Term,
        prev_slot: usize,
        prev_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: usize,
        /// For conservative snapshotting purpose.
        last_snap: usize,
    },

    /// `AppendEntries` reply from follower to leader.
    AppendEntriesReply {
        term: Term,
        /// For correct tracking of which `AppendEntries` this reply is for.
        end_slot: usize,
        /// If success, `None`; otherwise, contains a pair of the conflicting
        /// entry's term and my first index for that term.
        conflict: Option<(Term, usize)>,
        /// Whether my entry at `end_slot` is a full copy, telling apart acks
        /// from before the entry got upgraded to full-copy replication.
        full_copy: bool,
    },

    /// `RequestVote` from leader to followers.
    RequestVote {
        term: Term,
        last_slot: usize,
        last_term: Term,
    },

    /// `RequestVote` reply from follower to leader.
    RequestVoteReply { term: Term, granted: bool },

    /// Reconstruction read from the leader or a follower to peers.
    Reconstruct {
        /// List of slots, their entries' terms, and the shards to exclude.
        slots: Vec<(usize, Term, Bitmap)>,
    },

    /// Reconstruction read reply from a peer.
    ReconstructReply {
        /// Map from slot -> term and req batch shards data the peer has.
        slots_data: HashMap<usize, (Term, RSCodeword<ReqBatch>)>,
    },
}

/// Replica role type.
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
    SchemaWrite,
    SchemaRead,
)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// `CRaft` server replica module.
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

    /// `ControlHub` module.
    control_hub: ControlHub,

    /// `ExternalApi` module.
    external_api: ExternalApi,

    /// `StateMachine` module.
    state_machine: StateMachine,

    /// `StorageHub` module.
    storage_hub: StorageHub<DurEntry, LogCtx>,

    /// `StorageHub` module for the snapshot file.
    snapshot_hub: StorageHub<SnapEntry>,

    /// `TransportHub` module.
    transport_hub: TransportHub<PeerMsg>,

    /// Heartbeater module.
    heartbeater: Heartbeater,

    /// Which role am I in right now?
    role: Role,

    /// Who do I think is the effective leader of the cluster right now?
    leader: Option<ReplicaId>,

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
    commit_known: usize,

    /// Slot index of highest committed log entry submitted for execution,
    /// which waits for entries to have enough shards.
    last_commit: usize,

    /// Slot index of highest log entry applied to state machine.
    last_exec: usize,

    /// Slot index of highest log entry for which a Reconstruct has been sent.
    /// This is a soft state and affects nothing about correctness.
    last_recon: usize,

    /// When reconstruction reads were last re-sent.
    recon_retry: Instant,

    /// For each server, index of the next log entry to send.
    next_slot: HashMap<ReplicaId, usize>,

    /// For each server, index of the next log entry to try to send for an
    /// `AppendEntries` message. This is added due to the asynchronous nature
    /// of Summerset's implementation.
    /// It is always true that `next_slot`[r] <= `try_next_slot`[r]
    try_next_slot: HashMap<ReplicaId, usize>,

    /// For each server, index of the highest log entry known to be replicated.
    match_slot: HashMap<ReplicaId, usize>,

    /// Slot index up to which it is safe to take snapshot.
    // NOTE: we are taking a conservative approach here that a snapshot
    //       covering an entry can be taken only when all servers have durably
    //       committed (and executed) that entry.
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
    /// Compose `CommandId` from slot index & command index within.
    #[inline]
    fn make_command_id(slot: usize, cmd_idx: usize) -> CommandId {
        debug_assert!(u32::try_from(slot).is_ok());
        debug_assert!(u32::try_from(cmd_idx).is_ok());
        ((slot << 32) | cmd_idx) as CommandId
    }

    /// Decompose `CommandId` into slot index & command index within.
    #[inline]
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let slot = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (slot, cmd_idx)
    }

    /// Slot of the last durable entry. All entries up to it are durable, as
    /// appends of new entries at the log end complete in order.
    fn last_durable_slot(&self) -> usize {
        let pending = self
            .log
            .iter()
            .rev()
            .take_while(|e| e.log_offset.is_none())
            .count();
        self.start_slot + self.log.len() - 1 - pending
    }

    /// The copy of an entry that replica `peer` keeps, according to how the
    /// entry is replicated.
    fn entry_for(&self, entry: &LogEntry, peer: ReplicaId) -> Result<LogEntry, SummersetError> {
        let shards = if entry.full_copy {
            Bitmap::from((self.population, 0..self.majority))
        } else {
            Bitmap::from((self.population, vec![peer]))
        };
        Ok(LogEntry {
            term: entry.term,
            reqs_cw: entry.reqs_cw.subset_copy(&shards, false)?,
            full_copy: entry.full_copy,
            external: false,
            log_offset: entry.log_offset,
        })
    }

    /// Highest slot up to which entries can be committed: it is of my term,
    /// and every entry up to it has acks from enough replicas for how it is
    /// replicated (a majority for full copies, more for single shards).
    fn commit_point(&self) -> usize {
        let mut commit = self.commit_known;
        for slot in (self.commit_known + 1)..(self.start_slot + self.log.len()) {
            let entry = &self.log[slot - self.start_slot];
            let match_cnt = 1 + self.match_slot.values().filter(|&&s| s >= slot).count();
            let required = if entry.full_copy {
                self.majority
            } else {
                self.majority + self.config.fault_tolerance
            };
            if match_cnt < usize::from(required) {
                break;
            }
            if entry.term == self.curr_term {
                commit = slot;
            }
        }
        commit
    }

    /// Appends a new entry of my term to my log as the leader, submitting the
    /// logger action to make my copy of it durable.
    fn append_new_entry(&mut self, reqs: ReqBatch, external: bool) -> Result<(), SummersetError> {
        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw =
            RSCodeword::from_data(reqs, self.majority, self.population - self.majority)?;
        reqs_cw.compute_parity(Some(&self.rs_coder))?;
        let entry = LogEntry {
            term: self.curr_term,
            reqs_cw,
            full_copy: self.full_copy_mode,
            external,
            log_offset: None,
        };

        let slot = self.start_slot + self.log.len();
        self.storage_hub.submit_action(
            LogCtx::LeaderAppend {
                slot,
                term: self.curr_term,
            },
            LogAction::Append {
                entry: DurEntry::LogEntry {
                    entry: self.entry_for(&entry, self.id)?,
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!("submitted leader append log action for slot {}", slot);
        self.log.push(entry);
        Ok(())
    }

    /// Sends `AppendEntries` with the peer's copies of my entries from `from`
    /// to `to` (inclusive) to it, in chunks to keep it heartbeated.
    fn send_append_entries(
        &mut self,
        peer: ReplicaId,
        from: usize,
        to: usize,
    ) -> Result<(), SummersetError> {
        for chunk_from in (from..=to).step_by(self.config.msg_chunk_size) {
            let chunk_to = cmp::min(chunk_from + self.config.msg_chunk_size - 1, to);
            let entries = self.log[(chunk_from - self.start_slot)..=(chunk_to - self.start_slot)]
                .iter()
                .map(|e| self.entry_for(e, peer))
                .collect::<Result<_, _>>()?;
            let prev_slot = chunk_from - 1;
            self.transport_hub.send_msg(
                PeerMsg::AppendEntries {
                    term: self.curr_term,
                    prev_slot,
                    prev_term: self.log[prev_slot - self.start_slot].term,
                    entries,
                    leader_commit: self.commit_known,
                    last_snap: self.last_snap,
                },
                peer,
            )?;
            pf_trace!(
                "sent AppendEntries -> {} with slots {} - {}",
                peer,
                chunk_from,
                chunk_to
            );
        }
        Ok(())
    }

    /// Re-persists my copies of the entries from slot `from` on, as the
    /// durable log mirrors the in-mem one (e.g., after entries there got
    /// upgraded to full copies).
    async fn rewrite_log_from(&mut self, from: usize) -> Result<(), SummersetError> {
        // wait for in-flight appends, to learn where the entry at `from` starts
        let hb_peer = self.leader.unwrap_or(self.id);
        for (old_ctx, old_result) in self.storage_hub.barrier().await? {
            self.handle_log_result(old_ctx, old_result).await?;
            self.heard_heartbeat(hb_peer, self.curr_term).await?;
        }
        let Some(cut_offset) = self.log[from - self.start_slot].log_offset else {
            return logged_err!("log offset of slot {} unknown", from);
        };
        let (_, result) = self
            .storage_hub
            .do_sync_action(LogAction::Truncate { offset: cut_offset })
            .await?;
        if !matches!(
            result,
            LogResult::Truncate {
                offset_ok: true,
                ..
            }
        ) {
            return logged_err!("unexpected log result type or failed truncate");
        }
        self.log_offset = cut_offset;

        for slot in from..(self.start_slot + self.log.len()) {
            let entry = self.entry_for(&self.log[slot - self.start_slot], self.id)?;
            let (_, result) = self
                .storage_hub
                .do_sync_action(LogAction::Append {
                    entry: DurEntry::LogEntry { entry },
                    sync: self.config.logger_sync,
                })
                .await?;
            let LogResult::Append { offset, now_size } = result else {
                return logged_err!("unexpected log result type");
            };
            self.log[slot - self.start_slot].log_offset = Some(offset);
            self.log_offset = now_size;
        }
        pf_debug!("re-persisted log from slot {}", from);
        Ok(())
    }

    /// Learns that entries up to `commit` are committed, and submits those
    /// with enough shards for execution in order.
    fn commit_up_to(&mut self, commit: usize) -> Result<(), SummersetError> {
        self.commit_known = cmp::max(self.commit_known, commit);
        while self.last_commit < self.commit_known {
            let slot = self.last_commit + 1;
            let entry = &mut self.log[slot - self.start_slot];
            if entry.reqs_cw.avail_shards() < self.majority {
                // can't execute if I don't have the complete request batch
                pf_trace!("postponing execution for slot {}", slot);
                break;
            } else if entry.reqs_cw.avail_data_shards() < self.majority {
                // have enough shards but need reconstruction
                entry.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
            }
            let reqs = entry.reqs_cw.get_data()?;
            for (cmd_idx, (client, req)) in reqs.iter().enumerate() {
                if let ApiRequest::Req { id: req_id, cmd } = req {
                    self.state_machine.submit_cmd(
                        Self::make_command_id(slot, cmd_idx),
                        cmd.clone(),
                        Some((*client, *req_id)),
                    )?;
                }
            }
            pf_trace!("submitted {} exec commands for slot {}", reqs.len(), slot);
            self.last_commit = slot;
        }
        self.advance_last_exec()
    }

    /// Sends reconstruction reads for committed entries that still lack
    /// enough shards to execute: right away for new ones (where a follower
    /// reads each shard from the other follower holding it, sparing the
    /// leader), and again to everyone once per hearing timeout, as earlier
    /// reads may have got lost.
    fn retrigger_reconstructs(&mut self) -> Result<(), SummersetError> {
        let overdue =
            self.recon_retry.elapsed() >= Duration::from_millis(self.config.hb_hear_timeout_min);
        let from_slot = if overdue {
            self.recon_retry = Instant::now();
            self.last_commit + 1
        } else {
            cmp::max(self.last_recon, self.last_commit) + 1
        };

        let mut recon_slots: HashMap<ReplicaId, Vec<(usize, Term, Bitmap)>> = HashMap::new();
        for slot in from_slot..=self.commit_known {
            let entry = &self.log[slot - self.start_slot];
            let avail = entry.reqs_cw.avail_shards_map();
            if avail.count() >= self.majority {
                continue;
            }
            let mut needed = self.majority - avail.count();
            for peer in ((self.id + 1)..(self.id + self.population)).map(|p| p % self.population) {
                if overdue || self.role == Role::Leader {
                    recon_slots
                        .entry(peer)
                        .or_default()
                        .push((slot, entry.term, avail.clone()));
                } else if needed > 0 && self.leader != Some(peer) && !avail.get(peer)? {
                    let mut excl = Bitmap::new(self.population, true);
                    excl.set(peer, false)?;
                    recon_slots
                        .entry(peer)
                        .or_default()
                        .push((slot, entry.term, excl));
                    needed -= 1;
                }
            }
        }
        self.last_recon = cmp::max(self.last_recon, self.commit_known);

        for (peer, slots) in recon_slots {
            for chunk in slots.chunks(self.config.msg_chunk_size) {
                self.transport_hub.send_msg(
                    PeerMsg::Reconstruct {
                        slots: chunk.to_vec(),
                    },
                    peer,
                )?;
            }
            pf_debug!(
                "sent Reconstruct messages -> {} for {} slots",
                peer,
                slots.len()
            );
        }
        Ok(())
    }

    /// Advances `last_exec` past committed entries with nothing to execute,
    /// such as the no-ops of new leaders.
    fn advance_last_exec(&mut self) -> Result<(), SummersetError> {
        while self.last_exec < self.last_commit {
            let entry = &mut self.log[self.last_exec + 1 - self.start_slot];
            if !entry.reqs_cw.get_data()?.is_empty() {
                break;
            }
            self.last_exec += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl GenericReplica for CRaftReplica {
    #[allow(clippy::too_many_lines)]
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        manager: SocketAddr,
        id: Option<ReplicaId>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a server ID
        let mut control_hub = ControlHub::new_and_setup(manager, id).await?;
        let id = control_hub.me;
        let population = control_hub.population;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ReplicaConfigCRaft;
                                    batch_interval_ms, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms, disable_hb_timer,
                                    disallow_step_up, snapshot_path,
                                    snapshot_interval_s, fault_tolerance,
                                    msg_chunk_size, sim_read_lease)?;
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
            return logged_err!("invalid config.msg_chunk_size '{}'", config.msg_chunk_size);
        }

        // setup state machine module
        let state_machine = StateMachine::new_and_setup(id).await?;

        // setup storage hub module
        let storage_hub = StorageHub::new_and_setup(id, Path::new(&config.backer_path)).await?;

        // setup heartbeat management module
        let heartbeater = Heartbeater::new_and_setup(
            id,
            population,
            Duration::from_millis(config.hb_hear_timeout_min),
            Duration::from_millis(config.hb_hear_timeout_max),
            Duration::from_millis(config.hb_send_interval_ms),
        )?;

        // setup transport hub module
        let mut transport_hub =
            TransportHub::new_and_setup(id, population, p2p_addr, HashMap::new()).await?;

        // ask for the list of peers to proactively connect to. Do this after
        // transport hub has been set up, so that I will be able to accept
        // later peer connections
        control_hub.send_ctrl(CtrlMsg::NewServerJoin {
            id,
            protocol: SmrProtocol::CRaft,
            api_addr,
            p2p_addr,
        })?;
        let CtrlMsg::ConnectToPeers { to_peers, .. } = control_hub.recv_ctrl().await? else {
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
        let rs_coder = ReedSolomon::new(majority as usize, (population - majority) as usize)?;

        // proactively connect to some peers, then wait for all population
        // have been connected with me
        for (peer, conn_addr) in to_peers {
            transport_hub.connect_to_peer(peer, conn_addr).await?;
        }
        transport_hub.wait_for_group(population).await?;

        // setup snapshot hub module
        let snapshot_hub = StorageHub::new_and_setup(id, Path::new(&config.snapshot_path)).await?;

        // setup external API module, ready to take in client requests
        let external_api = ExternalApi::new_and_setup(
            id,
            api_addr,
            Duration::from_millis(config.batch_interval_ms),
            config.max_batch_size,
        )
        .await?;

        let mut snapshot_interval =
            time::interval(Duration::from_secs(if config.snapshot_interval_s > 0 {
                config.snapshot_interval_s
            } else {
                60 // dummy non-zero value to make `time::interval` happy
            }));
        snapshot_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

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
            heartbeater,
            role: Role::Follower,
            leader: None,
            curr_term: 0,
            voted_for: None,
            votes_granted: HashSet::new(),
            log: vec![],
            start_slot: 0,
            snapshot_interval,
            commit_known: 0,
            last_commit: 0,
            last_exec: 0,
            last_recon: 0,
            recon_retry: Instant::now(),
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

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, mut rx_term: watch::Receiver<bool>) -> Result<bool, SummersetError> {
        // recover state from durable snapshot file
        self.recover_from_snapshot().await?;

        // recover the tail-piece memory log & state from remaining durable log
        self.recover_from_wal().await?;

        // kick off leader activity hearing timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer(None)?;
        }

        // main event loop; getting an event fails only if the task behind it
        // is gone, in which case exit
        let mut paused = false;
        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.get_req_batch(), if !paused => {
                    let req_batch = req_batch?;
                    if let Err(e) = self.handle_req_batch(req_batch).await {
                        pf_error!("error handling req batch: {}", e);
                    }
                },

                // durable logging result
                log_result = self.storage_hub.get_result(), if !paused => {
                    let (log_ctx, log_result) = log_result?;
                    if let Err(e) = self.handle_log_result(log_ctx, log_result).await {
                        pf_error!("error handling log result {:?}: {}", log_ctx, e);
                    }
                },

                // message from peer
                msg = self.transport_hub.recv_msg(), if !paused => {
                    let (peer, msg) = msg?;
                    if let Err(e) = self.handle_msg_recv(peer, msg).await {
                        pf_error!("error handling msg recv <- {}: {}", peer, e);
                    }
                },

                // state machine execution result
                cmd_result = self.state_machine.get_result(), if !paused => {
                    let (cmd_id, cmd_result) = cmd_result?;
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result).await {
                        pf_error!("error handling cmd result {}: {}", cmd_id, e);
                    }
                },

                // heartbeat-related event
                hb_event = self.heartbeater.get_event(), if !paused => {
                    match hb_event? {
                        HeartbeatEvent::HearTimeout { peer } => {
                            if let Err(e) = self.become_a_candidate(peer).await {
                                pf_error!("error becoming a candidate: {}", e);
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
                            CtrlMsg::SnapshotUpTo { new_start: self.start_slot }
                        )?;
                    }
                },

                // manager control message
                ctrl_msg = self.control_hub.recv_ctrl() => {
                    // fails only if the manager connection is lost, in which
                    // case exit as there is no way to rejoin
                    let ctrl_msg = ctrl_msg?;
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

/// `CRaft` client-side module.
pub(crate) struct CRaftClient {
    /// Client ID.
    id: ClientId,

    /// Number of servers in the cluster.
    population: u8,

    /// Configuration parameters struct.
    _config: ClientConfigCRaft,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Current server ID to talk to.
    server_id: ReplicaId,

    /// Last server ID I sent something to unsuccessfully; used here to help
    /// with retrying.
    last_server_id: Option<ReplicaId>,

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
            population: 0,
            _config: config,
            servers: HashMap::new(),
            server_id: init_server_id,
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
        self.ctrl_stub
            .send_req_insist(&CtrlRequest::QueryInfo)
            .await?;

        let reply = self.ctrl_stub.recv_reply().await?;
        if let CtrlReply::QueryInfo {
            population,
            servers_info,
        } = reply
        {
            self.population = population;

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
                let api_stub = ClientApiStub::new_by_connect(self.id, server).await?;
                self.api_stubs.insert(id, api_stub);
            }
            Ok(())
        } else {
            logged_err!("unexpected reply type received")
        }
    }

    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        // send leave notification to all servers
        for (id, mut api_stub) in self.api_stubs.drain() {
            api_stub.send_req_insist(&ApiRequest::Leave).await?;

            // NOTE: commented out the following wait to avoid accidental
            //       hanging upon leaving
            // while api_stub.recv_reply().await? != ApiReply::Leave {}
            pf_debug!("left server connection {}", id);
        }

        // if permanently leaving, send leave notification to the manager
        if permanent {
            self.ctrl_stub.send_req_insist(&CtrlRequest::Leave).await?;

            while self.ctrl_stub.recv_reply().await? != CtrlReply::Leave {}
            pf_debug!("left manager connection");
        }

        Ok(())
    }

    fn send_req(&mut self, req: Option<&ApiRequest>) -> Result<bool, SummersetError> {
        // a retry goes to where the last send failed, even if since redirected
        let server_id = if req.is_some() {
            self.server_id
        } else if let Some(last_server_id) = self.last_server_id {
            last_server_id
        } else {
            return Ok(true); // nothing blocked left to retry
        };

        if let Some(api_stub) = self.api_stubs.get_mut(&server_id) {
            let success = api_stub.send_req(req)?;
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
