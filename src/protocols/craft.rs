//! Replication protocol: CRaft (Coded-Raft).
//!
//! Raft with erasure coding and fall-back mechanism. References:
//!   - <https://www.usenix.org/conference/fast20/presentation/wang-zizhong>

use std::cmp;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::{SummersetError, Bitmap, Timer, RSCodeword};
use crate::manager::{CtrlMsg, CtrlRequest, CtrlReply};
use crate::server::{
    ReplicaId, ControlHub, StateMachine, Command, CommandResult, CommandId,
    ExternalApi, ApiRequest, ApiReply, StorageHub, LogAction, LogResult,
    LogActionId, TransportHub, GenericReplica,
};
use crate::client::{ClientId, ClientApiStub, ClientCtrlStub, GenericEndpoint};
use crate::protocols::SmrProtocol;

use rand::prelude::*;

use async_trait::async_trait;

use get_size::GetSize;

use serde::{Serialize, Deserialize};

use tokio::time::{self, Duration, Interval, MissedTickBehavior};
use tokio::sync::watch;

use reed_solomon_erasure::galois_8::ReedSolomon;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
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

    /// Path to snapshot file.
    pub snapshot_path: String,

    /// Snapshot self-triggering interval in secs. 0 means never trigger
    /// snapshotting autonomously.
    pub snapshot_interval_s: u64,

    /// Fault-tolerance level.
    pub fault_tolerance: u8,

    /// Maximum chunk size of a ReconstructRead message.
    pub recon_chunk_size: usize,

    // Performance simulation params (all zeros means no perf simulation):
    pub perf_storage_a: u64,
    pub perf_storage_b: u64,
    pub perf_network_a: u64,
    pub perf_network_b: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigCRaft {
    fn default() -> Self {
        ReplicaConfigCRaft {
            batch_interval_ms: 10,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.craft.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 600,
            hb_hear_timeout_max: 900,
            hb_send_interval_ms: 50,
            snapshot_path: "/tmp/summerset.craft.snap".into(),
            snapshot_interval_s: 0,
            fault_tolerance: 0,
            recon_chunk_size: 100,
            perf_storage_a: 0,
            perf_storage_b: 0,
            perf_network_a: 0,
            perf_network_b: 0,
        }
    }
}

/// Term number type, defined for better code readability.
type Term = u64;

/// Request batch type (i.e., the "command" in an entry).
///
/// NOTE: the originally presented Raft algorithm does not explicitly mention
/// batching, but instead hides it with the heartbeats: every AppendEntries RPC
/// from the leader basically batches all commands it has received since the
/// last sent heartbeat. Here, to make this implementation more comparable to
/// MultiPaxos, we trigger batching also explicitly.
type ReqBatch = Vec<(ClientId, ApiRequest)>;

/// In-mem + persistent entry of log, containing a term and a (possibly
/// partial) commands batch.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
struct LogEntry {
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
enum DurEntry {
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
enum SnapEntry {
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
enum PeerMsg {
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
enum Role {
    Follower,
    Candidate,
    Leader,
}

/// CRaft server replica module.
pub struct CRaftReplica {
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

    /// Check if the given term is larger than mine. If so, convert my role
    /// back to follower. Returns true if my role was not follower but now
    /// converted to follower, and false otherwise.
    #[inline]
    async fn check_term(
        &mut self,
        peer: ReplicaId,
        term: Term,
    ) -> Result<bool, SummersetError> {
        if term > self.curr_term {
            self.curr_term = term;
            self.voted_for = None;
            self.votes_granted.clear();

            // also make the two critical fields durable, synchronously
            self.storage_hub.submit_action(
                0,
                LogAction::Write {
                    entry: DurEntry::Metadata {
                        curr_term: self.curr_term,
                        voted_for: self.voted_for,
                    },
                    offset: 0,
                    sync: self.config.logger_sync,
                },
            )?;
            loop {
                let (action_id, log_result) =
                    self.storage_hub.get_result().await?;
                if action_id != 0 {
                    // normal log action previously in queue; process it
                    self.handle_log_result(action_id, log_result)?;
                } else {
                    if let LogResult::Write {
                        offset_ok: true, ..
                    } = log_result
                    {
                    } else {
                        return logged_err!(self.id; "unexpected log result type or failed write");
                    }
                    break;
                }
            }

            // refresh heartbeat hearing timer
            self.heard_heartbeat(peer, term)?;

            if self.role != Role::Follower {
                self.role = Role::Follower;
                self.control_hub
                    .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
                pf_trace!(self.id; "converted back to follower");
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Switch between normal "1 shard per replica" mode and full-copy mode.
    /// If falling back to full-copy, also re-persist and re-send all shards
    /// in my current log.
    fn switch_assignment_mode(
        &mut self,
        to_full_copy: bool,
    ) -> Result<(), SummersetError> {
        if self.full_copy_mode == to_full_copy {
            return Ok(()); // invalid this mode, ignore
        }
        pf_info!(self.id; "switching assignment config to: {}",
                          if to_full_copy { "full-copy" } else { "1-shard" });
        self.full_copy_mode = to_full_copy;

        if to_full_copy {
            // you might already notice that such fallback mechanism does not
            // guarantee to guard against extra failures during this period

            // TODO: should re-persist all data shards, but not really that
            // important for evaluation; skipped in current implementation

            // re-send AppendEntries covering all entries, containing all
            // data shards of each entry, to followers
            let entries = self
                .log
                .iter()
                .skip(1)
                .map(|e| LogEntry {
                    term: e.term,
                    reqs_cw: e
                        .reqs_cw
                        .subset_copy(
                            Bitmap::from(
                                self.population,
                                (0..self.majority).collect(),
                            ),
                            false,
                        )
                        .unwrap(),
                    external: false,
                    log_offset: e.log_offset,
                })
                .collect();
            self.transport_hub.bcast_msg(
                PeerMsg::AppendEntries {
                    term: self.curr_term,
                    prev_slot: self.start_slot,
                    prev_term: self.log[0].term,
                    entries,
                    leader_commit: self.last_commit,
                    last_snap: self.last_snap,
                },
                None,
            )?;
        }

        Ok(())
    }
}

// CRaftReplica client requests entrance
impl CRaftReplica {
    /// Handler of client request batch chan recv.
    fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        debug_assert!(batch_size > 0);
        pf_debug!(self.id; "got request batch of size {}", batch_size);

        // if I'm not a leader, ignore client requests
        if self.role != Role::Leader {
            for (client, req) in req_batch {
                if let ApiRequest::Req { id: req_id, .. } = req {
                    // tell the client to try on known leader or just the
                    // next ID replica
                    let target = if let Some(peer) = self.leader {
                        peer
                    } else {
                        (self.id + 1) % self.population
                    };
                    self.external_api.send_reply(
                        ApiReply::Reply {
                            id: req_id,
                            result: None,
                            redirect: Some(target),
                        },
                        client,
                    )?;
                    pf_trace!(self.id; "redirected client {} to replica {}",
                                       client, target);
                }
            }
            return Ok(());
        }

        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw = RSCodeword::from_data(
            req_batch,
            self.majority,
            self.population - self.majority,
        )?;
        reqs_cw.compute_parity(Some(&self.rs_coder))?;

        // submit logger action to make this log entry durable
        let slot = self.start_slot + self.log.len();
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, slot, Role::Leader),
            LogAction::Append {
                entry: DurEntry::LogEntry {
                    entry: LogEntry {
                        term: self.curr_term,
                        reqs_cw: if self.full_copy_mode {
                            reqs_cw.subset_copy(
                                Bitmap::from(
                                    self.population,
                                    (0..self.majority).collect(),
                                ),
                                false,
                            )?
                        } else {
                            reqs_cw.subset_copy(
                                Bitmap::from(self.population, vec![self.id]),
                                false,
                            )?
                        },
                        external: true,
                        log_offset: 0,
                    },
                },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(self.id; "submitted leader append log action for slot {}", slot);

        // append an entry to in-memory log
        self.log.push(LogEntry {
            term: self.curr_term,
            reqs_cw,
            external: true,
            log_offset: 0,
        });

        Ok(())
    }
}

// CRaftReplica durable logging
impl CRaftReplica {
    /// Handler of leader append logging result chan recv.
    fn handle_logged_leader_append(
        &mut self,
        slot: usize,
        slot_e: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot || self.role != Role::Leader {
            return Ok(()); // ignore if outdated
        }
        pf_trace!(self.id; "finished leader append logging for slot {} <= {}",
                           slot, slot_e);
        debug_assert_eq!(slot, slot_e);

        // broadcast AppendEntries messages to followers, each containing just
        // the one shard of each entry for that follower
        for peer in 0..self.population {
            if peer == self.id || self.try_next_slot[&peer] < 1 {
                continue;
            }

            let prev_slot = self.try_next_slot[&peer] - 1;
            if prev_slot < self.start_slot {
                return logged_err!(self.id; "snapshotted slot {} queried", prev_slot);
            }
            if prev_slot >= self.start_slot + self.log.len() {
                continue;
            }
            let prev_term = self.log[prev_slot - self.start_slot].term;
            let entries = self
                .log
                .iter()
                .take(slot + 1 - self.start_slot)
                .skip(self.try_next_slot[&peer] - self.start_slot)
                .map(|e| {
                    if self.full_copy_mode {
                        e.clone()
                    } else {
                        LogEntry {
                            term: e.term,
                            reqs_cw: e
                                .reqs_cw
                                .subset_copy(
                                    Bitmap::from(self.population, vec![peer]),
                                    false,
                                )
                                .unwrap(),
                            external: false,
                            log_offset: e.log_offset,
                        }
                    }
                })
                .collect();

            if slot >= self.try_next_slot[&peer] {
                self.transport_hub.send_msg(
                    PeerMsg::AppendEntries {
                        term: self.curr_term,
                        prev_slot,
                        prev_term,
                        entries,
                        leader_commit: self.last_commit,
                        last_snap: self.last_snap,
                    },
                    peer,
                )?;
                pf_trace!(self.id; "sent AppendEntries -> {} with slots {} - {}",
                                   peer, self.try_next_slot[&peer], slot);

                // update try_next_slot to avoid blindly sending the same
                // entries again on future triggers
                *self.try_next_slot.get_mut(&peer).unwrap() = slot + 1;
            }
        }

        // I also heard my own heartbeat
        self.heard_heartbeat(self.id, self.curr_term)?;

        Ok(())
    }

    /// Handler of follower append logging result chan recv.
    fn handle_logged_follower_append(
        &mut self,
        slot: usize,
        slot_e: usize,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot || self.role != Role::Follower {
            return Ok(()); // ignore if outdated
        }
        pf_trace!(self.id; "finished follower append logging for slot {} <= {}",
                           slot, slot_e);
        debug_assert!(slot <= slot_e);

        // if all consecutive entries are made durable, reply AppendEntries
        // success back to leader
        if slot == slot_e {
            if let Some(leader) = self.leader {
                self.transport_hub.send_msg(
                    PeerMsg::AppendEntriesReply {
                        term: self.curr_term,
                        end_slot: slot_e,
                        conflict: None,
                    },
                    leader,
                )?;
                pf_trace!(self.id; "sent AppendEntriesReply -> {} up to slot {}",
                                   leader, slot_e);
            }
        }

        Ok(())
    }

    /// Synthesized handler of durable logging result chan recv.
    fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<DurEntry>,
    ) -> Result<(), SummersetError> {
        let (slot, slot_e, entry_type) = Self::split_log_action_id(action_id);
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot_e < self.start_slot + self.log.len());

        if let LogResult::Append { now_size } = log_result {
            let entry = &mut self.log[slot - self.start_slot];
            if entry.log_offset != self.log_offset {
                // entry has incorrect log_offset bookkept; update it
                entry.log_offset = self.log_offset;
            }
            debug_assert!(now_size > self.log_offset);
            self.log_offset = now_size;
        } else {
            return logged_err!(self.id; "unexpected log result type: {:?}", log_result);
        }

        match entry_type {
            Role::Follower => self.handle_logged_follower_append(slot, slot_e),
            Role::Leader => self.handle_logged_leader_append(slot, slot_e),
            _ => {
                logged_err!(self.id; "unexpected log entry type: {:?}", entry_type)
            }
        }
    }
}

// CRaftReplica peer-peer messages handling
impl CRaftReplica {
    /// Handler of AppendEntries message from leader.
    #[allow(clippy::too_many_arguments)]
    async fn handle_msg_append_entries(
        &mut self,
        leader: ReplicaId,
        term: Term,
        prev_slot: usize,
        prev_term: Term,
        mut entries: Vec<LogEntry>,
        leader_commit: usize,
        last_snap: usize,
    ) -> Result<(), SummersetError> {
        if !entries.is_empty() {
            pf_trace!(self.id; "received AcceptEntries <- {} for slots {} - {} term {}",
                               leader, prev_slot + 1, prev_slot + entries.len(), term);
        }
        if self.check_term(leader, term).await? || self.role != Role::Follower {
            return Ok(());
        }

        // reply false if term smaller than mine, or if my log does not
        // contain an entry at prev_slot matching prev_term
        if !entries.is_empty()
            && (term < self.curr_term
                || prev_slot < self.start_slot
                || prev_slot >= self.start_slot + self.log.len()
                || self.log[prev_slot - self.start_slot].term != prev_term)
        {
            // figure out the conflict info to send back
            let conflict_term = if prev_slot >= self.start_slot
                && prev_slot < self.start_slot + self.log.len()
            {
                self.log[prev_slot - self.start_slot].term
            } else {
                0
            };
            let mut conflict_slot = prev_slot;
            while conflict_term > 0 && conflict_slot > self.start_slot {
                if self.log[conflict_slot - 1 - self.start_slot].term
                    == conflict_term
                {
                    conflict_slot -= 1;
                } else {
                    break;
                }
            }

            self.transport_hub.send_msg(
                PeerMsg::AppendEntriesReply {
                    term: self.curr_term,
                    end_slot: prev_slot + entries.len(),
                    conflict: Some((conflict_term, conflict_slot)),
                },
                leader,
            )?;
            pf_trace!(self.id; "sent AcceptEntriesReply -> {} term {} end_slot {} fail",
                               leader, self.curr_term, prev_slot);

            if term >= self.curr_term {
                // also refresh heartbeat timer here since the "decrementing"
                // procedure for a lagging follower might take long
                self.heard_heartbeat(leader, term)?;
            }
            return Ok(());
        }

        // update my knowledge of who's the current leader, and reset election
        // timeout timer
        self.leader = Some(leader);
        self.heard_heartbeat(leader, term)?;

        // check if any existing entry conflicts with a new one in `entries`.
        // If so, truncate everything at and after that entry
        let mut first_new = prev_slot + 1;
        for (slot, new_entry) in entries
            .iter()
            .enumerate()
            .map(|(s, e)| (s + prev_slot + 1, e))
        {
            if slot >= self.start_slot + self.log.len() {
                first_new = slot;
                break;
            } else if self.log[slot - self.start_slot].term != new_entry.term {
                let cut_offset = self.log[slot - self.start_slot].log_offset;
                // do this truncation in-place for simplicity
                self.storage_hub.submit_action(
                    0,
                    LogAction::Truncate { offset: cut_offset },
                )?;
                loop {
                    let (action_id, log_result) =
                        self.storage_hub.get_result().await?;
                    if action_id != 0 {
                        // normal log action previously in queue; process it
                        self.handle_log_result(action_id, log_result)?;
                    } else {
                        if let LogResult::Truncate {
                            offset_ok: true,
                            now_size,
                        } = log_result
                        {
                            debug_assert_eq!(now_size, cut_offset);
                            self.log_offset = cut_offset;
                        } else {
                            return logged_err!(
                                self.id;
                                "unexpected log result type or failed truncate"
                            );
                        }
                        break;
                    }
                }
                // truncate in-mem log as well
                self.log.truncate(slot - self.start_slot);
                first_new = slot;
                break;
            } else {
                // no conflict, then absorb this sent entry's shards
                if self.log[slot - self.start_slot].reqs_cw.avail_data_shards()
                    < self.majority
                    && self.log[slot - self.start_slot].reqs_cw.data_len()
                        == new_entry.reqs_cw.data_len()
                    && self.log[slot - self.start_slot]
                        .reqs_cw
                        .avail_shards_vec()
                        != new_entry.reqs_cw.avail_shards_vec()
                {
                    self.log[slot - self.start_slot]
                        .reqs_cw
                        .absorb_other(new_entry.reqs_cw.clone())?;
                }
            }
        }

        // append new entries into my log, and submit logger actions to make
        // new entries durable
        let (num_entries, mut num_appended) = (entries.len(), 0);
        for (slot, mut entry) in entries
            .drain((first_new - prev_slot - 1)..entries.len())
            .enumerate()
            .map(|(s, e)| (s + first_new, e))
        {
            entry.log_offset = 0;

            self.log.push(entry.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(
                    slot,
                    prev_slot + num_entries,
                    Role::Follower,
                ),
                LogAction::Append {
                    entry: DurEntry::LogEntry { entry },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted follower append log action for slot {}", slot);

            num_appended += 1;
        }

        // even if no entries appended, also send back AppendEntriesReply
        // as a follower-to-leader reverse heardbeat for peer health
        // tracking purposes
        if num_appended == 0 {
            self.transport_hub.send_msg(
                PeerMsg::AppendEntriesReply {
                    term: self.curr_term,
                    end_slot: first_new - 1,
                    conflict: None,
                },
                leader,
            )?;
        }

        // if leader_commit is larger than my last_commit, update last_commit
        if leader_commit > self.last_commit {
            let mut new_commit =
                cmp::min(leader_commit, prev_slot + entries.len());
            new_commit =
                cmp::min(new_commit, self.start_slot + self.log.len() - 1);

            // submit newly committed entries for state machine execution
            for slot in (self.last_commit + 1)..=new_commit {
                let entry = &mut self.log[slot - self.start_slot];

                if entry.reqs_cw.avail_shards() < self.majority {
                    // can't execute if I don't have the complete request batch
                    if !entries.is_empty() {
                        pf_debug!(self.id; "postponing execution for slot {} (shards {}/{})",
                                           slot, entry.reqs_cw.avail_shards(), self.majority);
                    }
                    break;
                } else if entry.reqs_cw.avail_data_shards() < self.majority {
                    // have enough shards but need reconstruction
                    entry.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }
                let reqs = entry.reqs_cw.get_data()?;

                for (cmd_idx, (_, req)) in reqs.iter().enumerate() {
                    if let ApiRequest::Req { cmd, .. } = req {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(slot, cmd_idx),
                            cmd.clone(),
                        )?;
                    } else {
                        continue; // ignore other types of requests
                    }
                }
                pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                   reqs.len(), slot);

                // last_commit update stops at the last slot successfully
                // submitted for execution
                self.last_commit = slot;
            }
        }

        // if last_snap is larger than mine, update last_snap
        if last_snap > self.last_snap {
            self.last_snap = last_snap;
        }

        Ok(())
    }

    /// Handler of AppendEntries reply from follower.
    async fn handle_msg_append_entries_reply(
        &mut self,
        peer: ReplicaId,
        term: Term,
        end_slot: usize,
        conflict: Option<(Term, usize)>,
    ) -> Result<(), SummersetError> {
        if conflict.is_some() || self.match_slot[&peer] != end_slot {
            pf_trace!(self.id; "received AcceptEntriesReply <- {} term {} end_slot {} {}",
                               peer, term, end_slot,
                               if conflict.is_none() { "ok" } else { "fail" });
        }
        if self.check_term(peer, term).await? || self.role != Role::Leader {
            return Ok(());
        }
        self.heard_heartbeat(peer, term)?;

        if conflict.is_none() {
            // success: update next_slot and match_slot for follower
            debug_assert!(self.next_slot[&peer] <= end_slot + 1);
            *self.next_slot.get_mut(&peer).unwrap() = end_slot + 1;
            if self.try_next_slot[&peer] < end_slot + 1 {
                *self.try_next_slot.get_mut(&peer).unwrap() = end_slot + 1;
            }
            *self.match_slot.get_mut(&peer).unwrap() = end_slot;

            // since we updated some match_slot here, check if any additional
            // entries are now considered committed
            let mut new_commit = self.last_commit;
            for slot in
                (self.last_commit + 1)..(self.start_slot + self.log.len())
            {
                let entry = &self.log[slot - self.start_slot];
                if entry.term != self.curr_term {
                    continue; // cannot decide commit using non-latest term
                }

                // if quorum size reached AND enough number of shards are
                // remembered, mark this instance as committed; in CRaft, this
                // means match_cnt >= self.majority + fault_tolerance
                let match_cnt = 1 + self
                    .match_slot
                    .values()
                    .filter(|&&s| s >= slot)
                    .count() as u8;
                if match_cnt >= self.majority + self.config.fault_tolerance {
                    // quorum size reached, set new_commit to here
                    new_commit = slot;
                }
            }

            // submit newly committed commands, if any, for execution
            let mut recon_slots = Vec::new();
            for slot in (self.last_commit + 1)..=new_commit {
                let entry = &mut self.log[slot - self.start_slot];

                if entry.reqs_cw.avail_shards() < self.majority {
                    // can't execute because I don't have the complete request
                    // batch. Because I am the leader, this means I need to
                    // issue reconstruction reads to followers to recover this
                    // log entry
                    recon_slots.push((slot, entry.term));
                } else if entry.reqs_cw.avail_data_shards() < self.majority {
                    // have enough shards but need reconstruction
                    entry.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }

                if !recon_slots.is_empty() {
                    // have encountered insufficient shards; cannot do any
                    // further execution right now
                    continue;
                }
                let reqs = entry.reqs_cw.get_data()?;

                for (cmd_idx, (_, req)) in reqs.iter().enumerate() {
                    if let ApiRequest::Req { cmd, .. } = req {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(slot, cmd_idx),
                            cmd.clone(),
                        )?;
                    } else {
                        continue; // ignore other types of requests
                    }
                }
                pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                   reqs.len(), slot);

                // last_commit update stops at the last slot successfully
                // submitted for execution
                self.last_commit = slot;
            }

            // send reconstruction read messages in chunks
            for chunk in recon_slots.chunks(self.config.recon_chunk_size) {
                let slots = chunk.to_vec();
                let num_slots = slots.len();
                self.transport_hub
                    .bcast_msg(PeerMsg::Reconstruct { slots }, None)?;
                pf_trace!(self.id; "broadcast Reconstruct messages for {} slots",
                                   num_slots);
            }

            // also check if any additional entries are safe to snapshot
            for slot in (self.last_snap + 1)..=end_slot {
                let match_cnt = 1 + self
                    .match_slot
                    .values()
                    .filter(|&&s| s >= slot)
                    .count() as u8;
                if match_cnt == self.population {
                    // all servers have durably stored this entry
                    self.last_snap = slot;
                }
            }
        } else {
            // failed: decrement next_slot for follower and retry
            debug_assert!(self.next_slot[&peer] >= 1);
            if self.next_slot[&peer] == 1 {
                *self.try_next_slot.get_mut(&peer).unwrap() = 1;
                return Ok(()); // cannot move backward any more
            }

            *self.next_slot.get_mut(&peer).unwrap() -= 1;
            if let Some((conflict_term, conflict_slot)) = conflict {
                while self.next_slot[&peer] > self.start_slot
                    && self.log[self.next_slot[&peer] - self.start_slot].term
                        == conflict_term
                    && self.next_slot[&peer] >= conflict_slot
                    && self.next_slot[&peer] > 1
                {
                    // bypass all conflicting entries in the conflicting term
                    *self.next_slot.get_mut(&peer).unwrap() -= 1;
                }
            }
            *self.try_next_slot.get_mut(&peer).unwrap() = self.next_slot[&peer];
            debug_assert!(end_slot >= self.next_slot[&peer]);

            let prev_slot = self.next_slot[&peer] - 1;
            if prev_slot < self.start_slot {
                return logged_err!(self.id; "snapshotted slot {} queried", prev_slot);
            }
            if prev_slot >= self.start_slot + self.log.len() {
                return Ok(());
            }
            let prev_term = self.log[prev_slot - self.start_slot].term;
            let entries = self
                .log
                .iter()
                .take(end_slot + 1 - self.start_slot)
                .skip(self.next_slot[&peer] - self.start_slot)
                .map(|e| {
                    if self.full_copy_mode {
                        e.clone()
                    } else {
                        LogEntry {
                            term: e.term,
                            reqs_cw: e
                                .reqs_cw
                                .subset_copy(
                                    Bitmap::from(self.population, vec![peer]),
                                    false,
                                )
                                .unwrap(),
                            external: false,
                            log_offset: e.log_offset,
                        }
                    }
                })
                .collect();

            self.transport_hub.send_msg(
                PeerMsg::AppendEntries {
                    term: self.curr_term,
                    prev_slot,
                    prev_term,
                    entries,
                    leader_commit: self.last_commit,
                    last_snap: self.last_snap,
                },
                peer,
            )?;
            pf_trace!(self.id; "sent AppendEntries -> {} with slots {} - {}",
                               peer, self.next_slot[&peer], end_slot);

            // update try_next_slot to avoid blindly sending the same
            // entries again on future triggers
            *self.try_next_slot.get_mut(&peer).unwrap() = end_slot + 1;
        }

        Ok(())
    }

    /// Handler of RequestVote message from candidate.
    async fn handle_msg_request_vote(
        &mut self,
        candidate: ReplicaId,
        term: Term,
        last_slot: usize,
        last_term: Term,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received RequestVote <- {} with term {} last {} term {}",
                           candidate, term, last_slot, last_term);
        self.check_term(candidate, term).await?;

        // if the given term is smaller than mine, reply false
        if term < self.curr_term {
            self.transport_hub.send_msg(
                PeerMsg::RequestVoteReply {
                    term: self.curr_term,
                    granted: false,
                },
                candidate,
            )?;
            pf_trace!(self.id; "sent RequestVoteReply -> {} term {} false",
                               candidate, self.curr_term);
            return Ok(());
        }

        // if I did not vote for anyone else in my current term and that the
        // candidate's log is as up-to-date as mine, grant vote
        #[allow(clippy::collapsible_if)]
        if self.voted_for.is_none() || (self.voted_for.unwrap() == candidate) {
            if last_term >= self.log.last().unwrap().term
                || (last_term == self.curr_term
                    && last_slot + 1 >= self.start_slot + self.log.len())
            {
                self.transport_hub.send_msg(
                    PeerMsg::RequestVoteReply {
                        term: self.curr_term,
                        granted: true,
                    },
                    candidate,
                )?;
                pf_trace!(self.id; "sent RequestVoteReply -> {} term {} granted",
                                   candidate, self.curr_term);

                // hear a heartbeat here to prevent me from starting an
                // election soon
                self.heard_heartbeat(candidate, term)?;

                // update voted_for and make the field durable, synchronously
                self.voted_for = Some(candidate);
                self.storage_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: DurEntry::Metadata {
                            curr_term: self.curr_term,
                            voted_for: self.voted_for,
                        },
                        offset: 0,
                        sync: self.config.logger_sync,
                    },
                )?;
                loop {
                    let (action_id, log_result) =
                        self.storage_hub.get_result().await?;
                    if action_id != 0 {
                        // normal log action previously in queue; process it
                        self.handle_log_result(action_id, log_result)?;
                    } else {
                        if let LogResult::Write {
                            offset_ok: true, ..
                        } = log_result
                        {
                        } else {
                            return logged_err!(self.id; "unexpected log result type or failed write");
                        }
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Handler of RequestVote reply from peer.
    async fn handle_msg_request_vote_reply(
        &mut self,
        peer: ReplicaId,
        term: Term,
        granted: bool,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received RequestVoteReply <- {} with term {} {}",
                           peer, term, if granted { "granted" } else { "false" });
        if self.check_term(peer, term).await? || self.role != Role::Candidate {
            return Ok(());
        }

        // bookkeep this vote
        self.votes_granted.insert(peer);

        // if a majority of servers have voted for me, become the leader
        if self.votes_granted.len() as u8 >= self.majority {
            self.become_the_leader()?;
        }

        Ok(())
    }

    /// Handler of Reconstruct message from leader.
    fn handle_msg_reconstruct(
        &mut self,
        peer: ReplicaId,
        slots: Vec<(usize, Term)>,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Reconstruct <- {} for slots {:?}", peer, slots);
        let mut slots_data = HashMap::new();

        for (slot, term) in slots {
            if slot < self.start_slot
                || slot >= self.start_slot + self.log.len()
                || term != self.log[slot - self.start_slot].term
            {
                // NOTE: this has one caveat: a new leader trying to do
                // reconstruction reads might find that all other peers have
                // snapshotted that slot. Proper InstallSnapshot-style messages
                // will be needed to deal with this; but since this scenario is
                // just too rare, it is not implemented yet
                continue;
            }

            // term match, send back my available shards to requester
            slots_data
                .insert(slot, self.log[slot - self.start_slot].reqs_cw.clone());
        }

        if !slots_data.is_empty() {
            let num_slots = slots_data.len();
            self.transport_hub
                .send_msg(PeerMsg::ReconstructReply { slots_data }, peer)?;
            pf_trace!(self.id; "sent ReconstructReply -> {} for {} slots",
                               peer, num_slots);
        }
        Ok(())
    }

    /// Handler of Reconstruct reply from replica.
    fn handle_msg_reconstruct_reply(
        &mut self,
        peer: ReplicaId,
        slots_data: HashMap<usize, RSCodeword<ReqBatch>>,
    ) -> Result<(), SummersetError> {
        // shadow_last_commit is the position of where last_commit should be
        // at if without sharding issues
        let shadow_last_commit = {
            let mut match_slots: Vec<usize> =
                self.match_slot.values().copied().collect();
            match_slots.sort();
            match_slots.reverse();
            match_slots
                [(self.majority + self.config.fault_tolerance - 2) as usize]
        };

        for (slot, reqs_cw) in slots_data {
            if slot < self.start_slot {
                continue; // ignore if slot index outdated
            }
            pf_trace!(self.id; "in ReconstructReply <- {} for slot {} shards {:?}",
                               peer, slot, reqs_cw.avail_shards_map());
            debug_assert!(slot < self.start_slot + self.log.len());
            let entry = &mut self.log[slot - self.start_slot];

            // absorb the shards from this replica
            entry.reqs_cw.absorb_other(reqs_cw)?;

            // if enough shards have been gathered, can push execution forward
            if slot == self.last_commit + 1 {
                while self.last_commit < shadow_last_commit {
                    let entry =
                        &mut self.log[self.last_commit + 1 - self.start_slot];
                    if entry.reqs_cw.avail_shards() < self.majority {
                        break;
                    }

                    if entry.reqs_cw.avail_data_shards() < self.majority {
                        // have enough shards but need reconstruction
                        entry.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                    }
                    let reqs = entry.reqs_cw.get_data()?;

                    // submit commands in committed instance to the state machine
                    // for execution
                    for (cmd_idx, (_, req)) in reqs.iter().enumerate() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            self.state_machine.submit_cmd(
                                Self::make_command_id(
                                    self.last_commit + 1,
                                    cmd_idx,
                                ),
                                cmd.clone(),
                            )?;
                        } else {
                            continue; // ignore other types of requests
                        }
                    }
                    pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                       reqs.len(), self.last_commit + 1);

                    self.last_commit += 1;
                }
            }
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    async fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::AppendEntries {
                term,
                prev_slot,
                prev_term,
                entries,
                leader_commit,
                last_snap,
            } => {
                self.handle_msg_append_entries(
                    peer,
                    term,
                    prev_slot,
                    prev_term,
                    entries,
                    leader_commit,
                    last_snap,
                )
                .await
            }
            PeerMsg::AppendEntriesReply {
                term,
                end_slot,
                conflict,
            } => {
                self.handle_msg_append_entries_reply(
                    peer, term, end_slot, conflict,
                )
                .await
            }
            PeerMsg::RequestVote {
                term,
                last_slot,
                last_term,
            } => {
                self.handle_msg_request_vote(peer, term, last_slot, last_term)
                    .await
            }
            PeerMsg::RequestVoteReply { term, granted } => {
                self.handle_msg_request_vote_reply(peer, term, granted)
                    .await
            }
            PeerMsg::Reconstruct { slots } => {
                self.handle_msg_reconstruct(peer, slots)
            }
            PeerMsg::ReconstructReply { slots_data } => {
                self.handle_msg_reconstruct_reply(peer, slots_data)
            }
        }
    }
}

// CRaftReplica state machine execution
impl CRaftReplica {
    /// Handler of state machine exec result chan recv.
    fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot < self.start_slot + self.log.len());
        pf_trace!(self.id; "executed cmd in entry at slot {} idx {}",
                           slot, cmd_idx);

        let entry = &mut self.log[slot - self.start_slot];
        let reqs = entry.reqs_cw.get_data()?;
        debug_assert!(cmd_idx < reqs.len());
        let (client, ref req) = reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if entry.external && self.external_api.has_client(client) {
                self.external_api.send_reply(
                    ApiReply::Reply {
                        id: *req_id,
                        result: Some(cmd_result),
                        redirect: None,
                    },
                    client,
                )?;
                pf_trace!(self.id; "replied -> client {} for slot {} idx {}",
                                   client, slot, cmd_idx);
            }
        } else {
            return logged_err!(self.id; "unexpected API request type");
        }

        // if all commands in this entry have been executed, update last_exec
        if cmd_idx == reqs.len() - 1 {
            pf_debug!(self.id; "executed all cmds in entry at slot {}", slot);
            self.last_exec = slot;
        }

        Ok(())
    }
}

// CRaftReplica leader election timeout logic
impl CRaftReplica {
    /// Becomes a candidate and starts the election procedure.
    async fn become_a_candidate(&mut self) -> Result<(), SummersetError> {
        if self.role != Role::Follower {
            return Ok(());
        }

        self.role = Role::Candidate;

        // increment current term and vote for myself
        self.curr_term += 1;
        self.voted_for = Some(self.id);
        self.votes_granted = HashSet::from([self.id]);
        pf_info!(self.id; "starting election with term {}...", self.curr_term);

        // reset election timeout timer
        self.heard_heartbeat(self.id, self.curr_term)?;

        // send RequestVote messages to all other peers
        let last_slot = self.start_slot + self.log.len() - 1;
        debug_assert!(last_slot >= self.start_slot);
        let last_term = self.log[last_slot - self.start_slot].term;
        self.transport_hub.bcast_msg(
            PeerMsg::RequestVote {
                term: self.curr_term,
                last_slot,
                last_term,
            },
            None,
        )?;
        pf_trace!(self.id; "broadcast RequestVote with term {} last {} term {}",
                           self.curr_term, last_slot, last_term);

        // also make the two critical fields durable, synchronously
        self.storage_hub.submit_action(
            0,
            LogAction::Write {
                entry: DurEntry::Metadata {
                    curr_term: self.curr_term,
                    voted_for: self.voted_for,
                },
                offset: 0,
                sync: self.config.logger_sync,
            },
        )?;
        loop {
            let (action_id, log_result) = self.storage_hub.get_result().await?;
            if action_id != 0 {
                // normal log action previously in queue; process it
                self.handle_log_result(action_id, log_result)?;
            } else {
                if let LogResult::Write {
                    offset_ok: true, ..
                } = log_result
                {
                } else {
                    return logged_err!(self.id; "unexpected log result type or failed write");
                }
                break;
            }
        }

        Ok(())
    }

    /// Becomes the leader after enough votes granted for me.
    fn become_the_leader(&mut self) -> Result<(), SummersetError> {
        pf_info!(self.id; "elected to be leader with term {}", self.curr_term);
        self.role = Role::Leader;
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;

        // clear peers' heartbeat reply counters, and broadcast a heartbeat now
        for cnts in self.hb_reply_cnts.values_mut() {
            *cnts = (1, 0, 0);
        }
        self.bcast_heartbeats()?;

        // re-initialize next_slot and match_slot information
        for slot in self.next_slot.values_mut() {
            *slot = self.start_slot + self.log.len();
        }
        for slot in self.try_next_slot.values_mut() {
            *slot = self.start_slot + self.log.len();
        }
        for slot in self.match_slot.values_mut() {
            *slot = 0;
        }

        Ok(())
    }

    /// Broadcasts empty AppendEntries messages as heartbeats to all peers.
    fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        let prev_slot = self.start_slot + self.log.len() - 1;
        debug_assert!(prev_slot >= self.start_slot);
        let prev_term = self.log[prev_slot - self.start_slot].term;
        self.transport_hub.bcast_msg(
            PeerMsg::AppendEntries {
                term: self.curr_term,
                prev_slot,
                prev_term,
                entries: vec![],
                leader_commit: self.last_commit,
                last_snap: self.last_snap,
            },
            None,
        )?;

        // update max heartbeat reply counters and their repetitions seen
        for (&peer, cnts) in self.hb_reply_cnts.iter_mut() {
            if cnts.0 > cnts.1 {
                // more hb replies have been received from this peer; it is
                // probably alive
                cnts.1 = cnts.0;
                cnts.2 = 0;
            } else {
                // did not receive hb reply from this peer at least for the
                // last sent hb from me; increment repetition count
                cnts.2 += 1;
                let repeat_threshold = (self.config.hb_hear_timeout_min
                    / self.config.hb_send_interval_ms)
                    as u8;
                if cnts.2 > repeat_threshold {
                    // did not receive hb reply from this peer for too many
                    // past hbs sent from me; this peer is probably dead
                    if self.peer_alive.get(peer)? {
                        self.peer_alive.set(peer, false)?;
                        pf_debug!(self.id; "peer_alive updated: {:?}", self.peer_alive);
                    }
                    cnts.2 = 0;
                }
            }
        }

        // I also heard this heartbeat from myself
        self.heard_heartbeat(self.id, self.curr_term)?;

        // check if we need to fall back to full-copy replication
        if self.population - self.peer_alive.count()
            >= self.config.fault_tolerance
        {
            self.switch_assignment_mode(true)?;
        }

        // pf_trace!(self.id; "broadcast heartbeats term {}", self.curr_term);
        Ok(())
    }

    /// Chooses a random hb_hear_timeout from the min-max range and kicks off
    /// the hb_hear_timer.
    fn kickoff_hb_hear_timer(&mut self) -> Result<(), SummersetError> {
        self.hb_hear_timer.cancel()?;

        let timeout_ms = thread_rng().gen_range(
            self.config.hb_hear_timeout_min..=self.config.hb_hear_timeout_max,
        );

        // pf_trace!(self.id; "kickoff hb_hear_timer @ {} ms", timeout_ms);
        self.hb_hear_timer
            .kickoff(Duration::from_millis(timeout_ms))?;
        Ok(())
    }

    /// Heard a heartbeat from some other replica. Resets election timer.
    fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        _term: Term,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
            self.hb_reply_cnts.get_mut(&peer).unwrap().0 += 1;
            if !self.peer_alive.get(peer)? {
                self.peer_alive.set(peer, true)?;
                pf_debug!(self.id; "peer_alive updated: {:?}", self.peer_alive);
                // check if we can move back to 1-shard replication
                if self.population - self.peer_alive.count()
                    < self.config.fault_tolerance
                {
                    self.switch_assignment_mode(false)?;
                }
            }
        }

        // reset hearing timer
        self.kickoff_hb_hear_timer()?;

        // pf_trace!(self.id; "heard heartbeat <- {} term {}", peer, term);
        Ok(())
    }
}

// CRaftReplica control messages handling
impl CRaftReplica {
    /// Handler of ResetState control message.
    async fn handle_ctrl_reset_state(
        &mut self,
        durable: bool,
    ) -> Result<(), SummersetError> {
        pf_warn!(self.id; "server got restart req");

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

        // promptly redirect all connected clients to another server
        let target = (self.id + 1) % self.population;
        self.external_api.bcast_reply(ApiReply::Reply {
            id: 0,
            result: None,
            redirect: Some(target),
        })?;
        pf_trace!(self.id; "redirected all clients to replica {}", target);

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

        // reset leader heartbeat timer
        self.kickoff_hb_hear_timer()?;

        *paused = false;
        self.control_hub.send_ctrl(CtrlMsg::ResumeReply)?;
        Ok(())
    }

    /// Handler of TakeSnapshot control message.
    async fn handle_ctrl_take_snapshot(
        &mut self,
    ) -> Result<(), SummersetError> {
        pf_warn!(self.id; "server told to take snapshot");
        self.take_new_snapshot().await?;

        self.control_hub.send_ctrl(CtrlMsg::SnapshotUpTo {
            new_start: self.start_slot,
        })?;
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

            CtrlMsg::TakeSnapshot => {
                self.handle_ctrl_take_snapshot().await?;
                Ok(None)
            }

            _ => Ok(None), // ignore all other types
        }
    }
}

// CRaftReplica recovery from durable log
impl CRaftReplica {
    /// Recover state from durable storage log.
    async fn recover_from_log(&mut self) -> Result<(), SummersetError> {
        debug_assert_eq!(self.log_offset, 0);

        // first, try to read the first several bytes, which should record
        // necessary durable metadata
        self.storage_hub
            .submit_action(0, LogAction::Read { offset: 0 })?;
        let (_, log_result) = self.storage_hub.get_result().await?;

        match log_result {
            LogResult::Read {
                entry:
                    Some(DurEntry::Metadata {
                        curr_term,
                        voted_for,
                    }),
                end_offset,
            } => {
                self.log_offset = end_offset;
                self.log_meta_end = end_offset;

                // recover necessary metadata info
                self.curr_term = curr_term;
                self.voted_for = voted_for;

                // read out and push all log entries into memory log
                loop {
                    // using 0 as a special log action ID
                    self.storage_hub.submit_action(
                        0,
                        LogAction::Read {
                            offset: self.log_offset,
                        },
                    )?;
                    let (_, log_result) = self.storage_hub.get_result().await?;

                    match log_result {
                        LogResult::Read {
                            entry: Some(DurEntry::LogEntry { mut entry }),
                            end_offset,
                        } => {
                            entry.log_offset = self.log_offset;
                            entry.external = false; // no re-replying to clients
                            self.log.push(entry);
                            self.log_offset = end_offset; // update log offset
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
            }

            LogResult::Read { entry: None, .. } => {
                // log file is empty, write initial metadata
                self.storage_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: DurEntry::Metadata {
                            curr_term: 0,
                            voted_for: None,
                        },
                        offset: 0,
                        sync: self.config.logger_sync,
                    },
                )?;
                let (_, log_result) = self.storage_hub.get_result().await?;
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = log_result
                {
                    self.log_offset = now_size;
                    self.log_meta_end = now_size;
                } else {
                    return logged_err!(self.id; "unexpected log result type or failed write");
                }
                // ... and push a 0-th dummy entry into in-mem log
                let null_entry = LogEntry {
                    term: 0,
                    reqs_cw: RSCodeword::from_null(
                        self.majority,
                        self.population - self.majority,
                    )?,
                    external: false,
                    log_offset: 0,
                };
                self.log.push(null_entry.clone());
                // ... and write the 0-th dummy entry durably
                self.storage_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: DurEntry::LogEntry { entry: null_entry },
                        offset: self.log_offset,
                        sync: self.config.logger_sync,
                    },
                )?;
                let (_, log_result) = self.storage_hub.get_result().await?;
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = log_result
                {
                    self.log[0].log_offset = self.log_offset;
                    self.log_offset = now_size;
                } else {
                    return logged_err!(self.id; "unexpected log result type or failed write");
                }
            }

            _ => return logged_err!(self.id; "unexpected log result type"),
        }

        // do an extra Truncate to remove paritial entry at the end if any
        debug_assert!(self.log_offset >= self.log_meta_end);
        self.storage_hub.submit_action(
            0,
            LogAction::Truncate {
                offset: self.log_offset,
            },
        )?;
        let (_, log_result) = self.storage_hub.get_result().await?;
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = log_result
        {
            if self.log_offset > self.log_meta_end {
                pf_info!(self.id; "recovered from wal log: term {} voted {:?} |log| {}",
                                  self.curr_term, self.voted_for, self.log.len());
            }
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type or failed truncate")
        }
    }
}

// CRaftReplica snapshotting & GC logic
impl CRaftReplica {
    /// Dump new key-value pairs to snapshot file.
    async fn snapshot_dump_kv_pairs(
        &mut self,
        new_start_slot: usize,
    ) -> Result<(), SummersetError> {
        // collect all key-value pairs put up to exec_bar
        let mut pairs = HashMap::new();
        for slot in self.start_slot..new_start_slot {
            let entry = &mut self.log[slot - self.start_slot];
            // do nothing for dummy entry at slot 0
            if entry.term > 0 {
                debug_assert!(
                    entry.reqs_cw.avail_data_shards() >= self.majority
                );
                for (_, req) in entry.reqs_cw.get_data()?.clone() {
                    if let ApiRequest::Req {
                        cmd: Command::Put { key, value },
                        ..
                    } = req
                    {
                        pairs.insert(key, value);
                    }
                }
            }
        }

        // write the collection to snapshot file
        self.snapshot_hub.submit_action(
            0, // using 0 as dummy log action ID
            LogAction::Append {
                entry: SnapEntry::KVPairSet { pairs },
                sync: self.config.logger_sync,
            },
        )?;
        let (_, log_result) = self.snapshot_hub.get_result().await?;
        if let LogResult::Append { now_size } = log_result {
            self.snap_offset = now_size;
            Ok(())
        } else {
            logged_err!(
                self.id;
                "unexpected log result type"
            )
        }
    }

    /// Discard everything lower than start_slot in durable log.
    async fn snapshot_discard_log(&mut self) -> Result<(), SummersetError> {
        // drain things currently in storage_hub's recv chan if head of log's
        // durable file offset has not been set yet
        debug_assert!(!self.log.is_empty());
        while self.log[0].log_offset == 0 {
            let (action_id, log_result) = self.storage_hub.get_result().await?;
            self.handle_log_result(action_id, log_result)?;
        }
        let cut_offset = self.log[0].log_offset;

        // discard the log after meta_end and before cut_offset
        if cut_offset > 0 {
            debug_assert!(self.log_meta_end > 0);
            debug_assert!(self.log_meta_end <= cut_offset);
            self.storage_hub.submit_action(
                0,
                LogAction::Discard {
                    offset: cut_offset,
                    keep: self.log_meta_end,
                },
            )?;
            loop {
                let (action_id, log_result) =
                    self.storage_hub.get_result().await?;
                if action_id != 0 {
                    // normal log action previously in queue; process it
                    self.handle_log_result(action_id, log_result)?;
                } else {
                    if let LogResult::Discard {
                        offset_ok: true,
                        now_size,
                    } = log_result
                    {
                        debug_assert_eq!(
                            self.log_offset - cut_offset + self.log_meta_end,
                            now_size
                        );
                        self.log_offset = now_size;
                    } else {
                        return logged_err!(
                            self.id;
                            "unexpected log result type or failed discard"
                        );
                    }
                    break;
                }
            }
        }

        // update entry.log_offset for all remaining in-mem entries
        for entry in &mut self.log {
            if entry.log_offset > 0 {
                debug_assert!(entry.log_offset >= cut_offset);
                entry.log_offset -= cut_offset - self.log_meta_end;
            }
        }

        Ok(())
    }

    /// Take a snapshot up to current last_exec, then discard the in-mem log up
    /// to that index as well as their data in the durable log file.
    ///
    /// NOTE: the current implementation does not guard against crashes in the
    /// middle of taking a snapshot. Production quality implementations should
    /// make the snapshotting action "atomic".
    ///
    /// NOTE: the current implementation does not take care of InstallSnapshot
    /// messages (which is needed when some lagging follower has some slot
    /// which all other peers have snapshotted); we take the conservative
    /// approach that a snapshot is only taken when data has been durably
    /// committed on all servers.
    async fn take_new_snapshot(&mut self) -> Result<(), SummersetError> {
        pf_debug!(self.id; "taking new snapshot: start {} exec {} snap {}",
                           self.start_slot, self.last_exec, self.last_snap);
        debug_assert!(self.last_exec + 1 >= self.start_slot);

        // always keep at least one entry in log to make indexing happy
        let new_start_slot = cmp::min(self.last_snap, self.last_exec);
        debug_assert!(new_start_slot < self.start_slot + self.log.len());
        if new_start_slot < self.start_slot + 1 {
            return Ok(());
        }

        // collect and dump all Puts in executed entries
        if self.role == Role::Leader {
            // NOTE: broadcast heartbeats here to appease followers
            self.bcast_heartbeats()?;
        }
        self.snapshot_dump_kv_pairs(new_start_slot).await?;

        // write new slot info entry to the head of snapshot
        self.snapshot_hub.submit_action(
            0,
            LogAction::Write {
                entry: SnapEntry::SlotInfo {
                    start_slot: new_start_slot,
                },
                offset: 0,
                sync: self.config.logger_sync,
            },
        )?;
        let (_, log_result) = self.snapshot_hub.get_result().await?;
        match log_result {
            LogResult::Write {
                offset_ok: true, ..
            } => {}
            _ => {
                return logged_err!(self.id; "unexpected log result type or failed write");
            }
        }

        // update start_slot and discard all in-mem log entries up to
        // new_start_slot
        self.log.drain(0..(new_start_slot - self.start_slot));
        self.start_slot = new_start_slot;

        // discarding everything lower than start_slot in durable log
        if self.role == Role::Leader {
            // NOTE: broadcast heartbeats here to appease followers
            self.bcast_heartbeats()?;
        }
        self.snapshot_discard_log().await?;

        // reset the leader heartbeat hear timer
        self.kickoff_hb_hear_timer()?;

        pf_info!(self.id; "took snapshot up to: start {}", self.start_slot);
        Ok(())
    }

    /// Recover initial state from durable storage snapshot file.
    async fn recover_from_snapshot(&mut self) -> Result<(), SummersetError> {
        debug_assert_eq!(self.snap_offset, 0);

        // first, try to read the first several bytes, which should record the
        // start_slot index
        self.snapshot_hub
            .submit_action(0, LogAction::Read { offset: 0 })?;
        let (_, log_result) = self.snapshot_hub.get_result().await?;

        match log_result {
            LogResult::Read {
                entry: Some(SnapEntry::SlotInfo { start_slot }),
                end_offset,
            } => {
                self.snap_offset = end_offset;

                // recover start_slot info
                self.start_slot = start_slot;
                if start_slot > 0 {
                    self.last_commit = start_slot - 1;
                    self.last_exec = start_slot - 1;
                    self.last_snap = start_slot - 1;
                }

                // repeatedly apply key-value pairs
                loop {
                    self.snapshot_hub.submit_action(
                        0,
                        LogAction::Read {
                            offset: self.snap_offset,
                        },
                    )?;
                    let (_, log_result) =
                        self.snapshot_hub.get_result().await?;

                    match log_result {
                        LogResult::Read {
                            entry: Some(SnapEntry::KVPairSet { pairs }),
                            end_offset,
                        } => {
                            // execute Put commands on state machine
                            for (key, value) in pairs {
                                self.state_machine.submit_cmd(
                                    0,
                                    Command::Put { key, value },
                                )?;
                                let _ = self.state_machine.get_result().await?;
                            }
                            // update snapshot file offset
                            self.snap_offset = end_offset;
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

                // tell manager about my start_slot index
                self.control_hub.send_ctrl(CtrlMsg::SnapshotUpTo {
                    new_start: self.start_slot,
                })?;

                if self.start_slot > 0 {
                    pf_info!(self.id; "recovered from snapshot: start {}",
                                      self.start_slot);
                }
                Ok(())
            }

            LogResult::Read { entry: None, .. } => {
                // snapshot file is empty. Write a 0 as start_slot and return
                self.snapshot_hub.submit_action(
                    0,
                    LogAction::Write {
                        entry: SnapEntry::SlotInfo { start_slot: 0 },
                        offset: 0,
                        sync: self.config.logger_sync,
                    },
                )?;
                let (_, log_result) = self.snapshot_hub.get_result().await?;
                if let LogResult::Write {
                    offset_ok: true,
                    now_size,
                } = log_result
                {
                    self.snap_offset = now_size;
                    Ok(())
                } else {
                    logged_err!(self.id; "unexpected log result type or failed write")
                }
            }

            _ => {
                logged_err!(self.id; "unexpected log result type")
            }
        }
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
                                    hb_send_interval_ms,
                                    snapshot_path, snapshot_interval_s,
                                    fault_tolerance, recon_chunk_size,
                                    perf_storage_a, perf_storage_b,
                                    perf_network_a, perf_network_b)?;
        if config.batch_interval_ms == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_ms '{}'",
                config.batch_interval_ms
            );
        }
        if config.hb_hear_timeout_min < 100 {
            return logged_err!(
                id;
                "invalid config.hb_hear_timeout_min '{}'",
                config.hb_hear_timeout_min
            );
        }
        if config.hb_hear_timeout_max < config.hb_hear_timeout_min + 100 {
            return logged_err!(
                id;
                "invalid config.hb_hear_timeout_max '{}'",
                config.hb_hear_timeout_max
            );
        }
        if config.hb_send_interval_ms == 0 {
            return logged_err!(
                id;
                "invalid config.hb_send_interval_ms '{}'",
                config.hb_send_interval_ms
            );
        }
        if config.recon_chunk_size == 0 {
            return logged_err!(
                id;
                "invalid config.recon_chunk_size '{}'",
                config.recon_chunk_size
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
            protocol: SmrProtocol::CRaft,
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

        // create a Reed-Solomon coder with num_data_shards == quorum size and
        // num_parity shards == population - quorum
        let majority = (population / 2) + 1;
        if config.fault_tolerance > (population - majority) {
            return logged_err!(id; "invalid config.fault_tolerance '{}'",
                                   config.fault_tolerance);
        }
        let rs_coder = ReedSolomon::new(
            majority as usize,
            (population - majority) as usize,
        )?;

        // proactively connect to some peers, then wait for all population
        // have been connected with me
        for (peer, addr) in to_peers {
            transport_hub.connect_to_peer(peer, addr).await?;
        }
        transport_hub.wait_for_group(population).await?;

        // setup snapshot hub module
        let snapshot_hub = StorageHub::new_and_setup(
            id,
            Path::new(&config.snapshot_path),
            None,
        )
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
        self.recover_from_log().await?;

        // kick off leader activity hearing timer
        self.kickoff_hb_hear_timer()?;

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
                        pf_error!(self.id; "error handling log result {}: {}",
                                           action_id, e);
                    }
                },

                // message from peer
                msg = self.transport_hub.recv_msg(), if !paused => {
                    if let Err(e) = msg {
                        pf_error!(self.id; "error receiving peer msg: {}", e);
                        continue;
                    }
                    let (peer, msg) = msg.unwrap();
                    if let Err(e) = self.handle_msg_recv(peer, msg).await {
                        pf_error!(self.id; "error handling msg recv <- {}: {}", peer, e);
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

                // leader inactivity timeout
                _ = self.hb_hear_timer.timeout(), if !paused => {
                    if let Err(e) = self.become_a_candidate().await {
                        pf_error!(self.id; "error becoming a candidate: {}", e);
                    }
                },

                // leader sending heartbeat
                _ = self.hb_send_interval.tick(), if !paused
                                                     && self.role == Role::Leader => {
                    if let Err(e) = self.bcast_heartbeats() {
                        pf_error!(self.id; "error broadcasting heartbeats: {}", e);
                    }
                },

                // autonomous snapshot taking timeout
                _ = self.snapshot_interval.tick(), if !paused
                                                      && self.config.snapshot_interval_s > 0 => {
                    if let Err(e) = self.take_new_snapshot().await {
                        pf_error!(self.id; "error taking a new snapshot: {}", e);
                    } else {
                        self.control_hub.send_ctrl(
                            CtrlMsg::SnapshotUpTo { new_start: self.start_slot }
                        )?;
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
pub struct CRaftClient {
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
        pf_info!("c"; "connecting to manager '{}'...", manager);
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
                // shift to a new server_id if current one not active
                debug_assert!(!servers_info.is_empty());
                while !servers_info.contains_key(&self.server_id) {
                    self.server_id = (self.server_id + 1) % population;
                }
                // establish connection to all servers
                self.servers = servers_info
                    .into_iter()
                    .map(|(id, info)| (id, info.api_addr))
                    .collect();
                for (&id, &server) in &self.servers {
                    pf_info!(self.id; "connecting to server {} '{}'...", id, server);
                    let api_stub =
                        ClientApiStub::new_by_connect(self.id, server).await?;
                    self.api_stubs.insert(id, api_stub);
                }
                Ok(())
            }
            _ => logged_err!(self.id; "unexpected reply type received"),
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
            pf_info!(self.id; "left server connection {}", id);
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
            pf_info!(self.id; "left manager connection");
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
            Err(SummersetError(format!(
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
                    pf_debug!(self.id; "redirected to replica {} '{}'",
                                       redirect_id, self.servers[&redirect_id]);
                }
            }

            Ok(reply)
        } else {
            Err(SummersetError(format!(
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
