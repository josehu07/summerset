//! Replication protocol: Crossword.
//!
//! MultiPaxos with flexible Reed-Solomon erasure coding that supports tunable
//! shard groups and asymmetric shard assignment.

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::{SummersetError, Bitmap, Timer, RSCodeword};
use crate::manager::{CtrlMsg, CtrlRequest, CtrlReply};
use crate::server::{
    ReplicaId, ControlHub, StateMachine, CommandResult, CommandId, ExternalApi,
    ApiRequest, ApiReply, StorageHub, LogAction, LogResult, LogActionId,
    TransportHub, GenericReplica,
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
pub struct ReplicaConfigCrossword {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing file.
    pub backer_path: String,

    /// Whether to call `fsync()`/`fdatasync()` on logger.
    pub logger_sync: bool,

    /// Min timeout of not hearing any heartbeat from leader in millisecs.
    pub hb_hear_timeout_min: u64,

    /// Max timeout of not hearing any heartbeat from leader in millisecs.
    pub hb_hear_timeout_max: u64,

    /// Interval of leader sending heartbeats to followers.
    pub hb_send_interval_ms: u64,

    /// Fault-tolerance level.
    pub fault_tolerance: u8,

    /// Number of shards to assign to each replica.
    // TODO: proper config options.
    pub shards_per_replica: u8,

    // Performance simulation params (all zeros means no perf simulation):
    pub perf_storage_a: u64,
    pub perf_storage_b: u64,
    pub perf_network_a: u64,
    pub perf_network_b: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigCrossword {
    fn default() -> Self {
        ReplicaConfigCrossword {
            batch_interval_us: 1000,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.rs_paxos.wal".into(),
            logger_sync: false,
            hb_hear_timeout_min: 300,
            hb_hear_timeout_max: 600,
            hb_send_interval_ms: 50,
            fault_tolerance: 0,
            shards_per_replica: 1,
            perf_storage_a: 0,
            perf_storage_b: 0,
            perf_network_a: 0,
            perf_network_b: 0,
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
struct ReplicaBookkeeping {
    /// Source leader replica ID for replyiing to Prepares and Accepts.
    source: ReplicaId,
}

/// In-memory instance containing a (possibly partial) commands batch.
#[derive(Debug, Clone)]
struct Instance {
    /// Ballot number.
    bal: Ballot,

    /// Instance status.
    status: Status,

    /// Shards of a batch of client requests.
    reqs_cw: RSCodeword<ReqBatch>,

    /// Highest ballot and associated value I have accepted.
    voted: (Ballot, RSCodeword<ReqBatch>),

    /// Leader-side bookkeeping info.
    leader_bk: Option<LeaderBookkeeping>,

    /// Follower-side bookkeeping info.
    replica_bk: Option<ReplicaBookkeeping>,

    /// True if from external client, else false.
    external: bool,
}

/// Stable storage log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
enum LogEntry {
    /// Records an update to the largest prepare ballot seen.
    PrepareBal { slot: usize, ballot: Ballot },

    /// Records a newly accepted request batch data shards at slot index.
    AcceptData {
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
    },

    /// Records an event of committing the instance at index.
    CommitSlot { slot: usize },
}

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize, GetSize)]
enum PeerMsg {
    /// Prepare message from leader to replicas.
    Prepare { slot: usize, ballot: Ballot },

    /// Prepare reply from replica to leader.
    PrepareReply {
        slot: usize,
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
    },

    /// Accept reply from replica to leader.
    AcceptReply { slot: usize, ballot: Ballot },

    /// Commit notification from leader to replicas.
    Commit { slot: usize },

    /// Recovery read from new leader to replicas.
    Recover { slot: usize },

    /// Recovery read reply from replica to leader.
    RecoverReply {
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
    },

    /// Leader activity heartbeat.
    Heartbeat { ballot: Ballot },
}

/// Crossword server replica module.
pub struct CrosswordReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Majority quorum size.
    quorum_cnt: u8,

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
    storage_hub: StorageHub<LogEntry>,

    /// TransportHub module.
    transport_hub: TransportHub<PeerMsg>,

    /// Timer for hearing heartbeat from leader.
    hb_hear_timer: Timer,

    /// Interval for sending heartbeat to followers.
    hb_send_interval: Interval,

    /// Do I think I am the leader?
    is_leader: bool,

    /// In-memory log of instances.
    insts: Vec<Instance>,

    /// Largest ballot number that a leader has sent Prepare messages in.
    bal_prep_sent: Ballot,

    /// Largest ballot number that a leader knows has been safely prepared.
    bal_prepared: Ballot,

    /// Largest ballot number seen as acceptor.
    bal_max_seen: Ballot,

    /// Index of the first non-committed instance.
    commit_bar: usize,

    /// Index of the first non-executed instance.
    /// It is always true that exec_bar <= commit_bar <= insts.len()
    exec_bar: usize,

    /// Current durable log file offset.
    log_offset: usize,

    /// Fixed Reed-Solomon coder.
    rs_coder: ReedSolomon,
}

impl CrosswordReplica {
    /// Create an empty null instance.
    fn null_instance(&self) -> Result<Instance, SummersetError> {
        Ok(Instance {
            bal: 0,
            status: Status::Null,
            reqs_cw: RSCodeword::<ReqBatch>::from_null(
                self.quorum_cnt,
                self.population - self.quorum_cnt,
            )?,
            voted: (
                0,
                RSCodeword::<ReqBatch>::from_null(
                    self.quorum_cnt,
                    self.population - self.quorum_cnt,
                )?,
            ),
            leader_bk: None,
            replica_bk: None,
            external: false,
        })
    }

    /// Compose a unique ballot number from base.
    fn make_unique_ballot(&self, base: u64) -> Ballot {
        ((base << 8) | ((self.id + 1) as u64)) as Ballot
    }

    /// Compose a unique ballot number greater than the given one.
    fn make_greater_ballot(&self, bal: Ballot) -> Ballot {
        self.make_unique_ballot((bal >> 8) + 1)
    }

    /// Compose LogActionId from slot index & entry type.
    /// Uses the `Status` enum type to represent differnet entry types.
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
    fn make_command_id(slot: usize, cmd_idx: usize) -> CommandId {
        assert!(slot <= (u32::MAX as usize));
        assert!(cmd_idx <= (u32::MAX as usize));
        ((slot << 32) | cmd_idx) as CommandId
    }

    /// Decompose CommandId into slot index & command index within.
    fn split_command_id(command_id: CommandId) -> (usize, usize) {
        let slot = (command_id >> 32) as usize;
        let cmd_idx = (command_id & ((1 << 32) - 1)) as usize;
        (slot, cmd_idx)
    }

    /// TODO: maybe remove this.
    fn shards_for_replica(
        id: ReplicaId,
        population: u8,
        num_shards: u8,
    ) -> Vec<u8> {
        (id..(id + num_shards)).map(|i| (i % population)).collect()
    }

    /// TODO: make better impl of this.
    fn coverage_under_faults(
        population: u8,
        acks: &HashMap<ReplicaId, Bitmap>,
        fault_tolerance: u8,
    ) -> u8 {
        if acks.len() <= fault_tolerance as usize {
            return 0;
        }

        // enumerate all subsets of acks excluding fault number of replicas
        let cnt = (acks.len() - fault_tolerance as usize) as u32;
        let servers: Vec<ReplicaId> = acks.keys().cloned().collect();
        let mut min_coverage = population;

        for n in (0..2usize.pow(servers.len() as u32))
            .filter(|n| n.count_ones() == cnt)
        {
            let mut coverage = Bitmap::new(population, false);
            for (_, server) in servers
                .iter()
                .enumerate()
                .filter(|&(i, _)| (n >> i) % 2 == 1)
            {
                for shard in acks[server].iter().filter_map(|(s, flag)| {
                    if flag {
                        Some(s)
                    } else {
                        None
                    }
                }) {
                    coverage.set(shard, true).expect("impossible shard index");
                }
            }

            if coverage.count() < min_coverage {
                min_coverage = coverage.count();
            }
        }

        min_coverage
    }

    /// Handler of client request batch chan recv.
    fn handle_req_batch(
        &mut self,
        req_batch: ReqBatch,
    ) -> Result<(), SummersetError> {
        let batch_size = req_batch.len();
        assert!(batch_size > 0);
        pf_debug!(self.id; "got request batch of size {}", batch_size);

        // if I'm not a leader, ignore client requests
        if !self.is_leader {
            for (client, req) in req_batch {
                if let ApiRequest::Req { id: req_id, .. } = req {
                    // tell the client to try on the next replica
                    let next_replica = (self.id + 1) % self.population;
                    self.external_api.send_reply(
                        ApiReply::Reply {
                            id: req_id,
                            result: None,
                            redirect: Some(next_replica),
                        },
                        client,
                    )?;
                    pf_trace!(self.id; "redirected client {} to replica {}",
                                       client, next_replica);
                }
            }
            return Ok(());
        }

        // compute the complete Reed-Solomon codeword for the batch data
        let mut reqs_cw = RSCodeword::from_data(
            req_batch,
            self.quorum_cnt,
            self.population - self.quorum_cnt,
        )?;
        reqs_cw.compute_parity(Some(&self.rs_coder))?;

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist)
        let mut slot = self.insts.len();
        for s in self.commit_bar..self.insts.len() {
            if self.insts[s].status == Status::Null {
                slot = s;
                break;
            }
        }
        if slot < self.insts.len() {
            let old_inst = &mut self.insts[slot];
            assert_eq!(old_inst.status, Status::Null);
            old_inst.reqs_cw = reqs_cw;
            old_inst.leader_bk = Some(LeaderBookkeeping {
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                accept_acks: HashMap::new(),
            });
        } else {
            let mut new_inst = self.null_instance()?;
            new_inst.reqs_cw = reqs_cw;
            new_inst.leader_bk = Some(LeaderBookkeeping {
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                accept_acks: HashMap::new(),
            });
            new_inst.external = true;
            self.insts.push(new_inst);
        }

        // decide whether we can enter fast path for this instance
        if self.bal_prepared == 0 {
            // slow case: Prepare phase not done yet. Initiate a Prepare round
            // if none is on the fly, or just wait for some Prepare reply to
            // trigger my Accept phase
            if self.bal_prep_sent == 0 {
                self.bal_prep_sent =
                    self.make_greater_ballot(self.bal_max_seen);
                self.bal_max_seen = self.bal_prep_sent;
            }

            let inst = &mut self.insts[slot];
            inst.bal = self.bal_prep_sent;
            inst.status = Status::Preparing;
            pf_debug!(self.id; "enter Prepare phase for slot {} bal {}",
                               slot, inst.bal);

            // record update to largest prepare ballot
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Preparing),
                LogAction::Append {
                    entry: LogEntry::PrepareBal {
                        slot,
                        ballot: self.bal_prep_sent,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                               slot, inst.bal);

            // send Prepare messages to all peers
            self.transport_hub.bcast_msg(
                PeerMsg::Prepare {
                    slot,
                    ballot: self.bal_prep_sent,
                },
                None,
            )?;
            pf_trace!(self.id; "broadcast Prepare messages for slot {} bal {}",
                               slot, inst.bal);
        } else {
            // normal case: Prepare phase covered, only do the Accept phase
            let inst = &mut self.insts[slot];
            inst.bal = self.bal_prepared;
            inst.status = Status::Accepting;
            pf_debug!(self.id; "enter Accept phase for slot {} bal {}",
                               slot, inst.bal);

            // record update to largest accepted ballot and corresponding data
            let subset_copy = inst.reqs_cw.subset_copy(
                Bitmap::from(
                    self.population,
                    Self::shards_for_replica(
                        self.id,
                        self.population,
                        self.config.shards_per_replica,
                    ),
                ),
                false,
            )?;
            inst.voted = (inst.bal, subset_copy.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Accepting),
                LogAction::Append {
                    entry: LogEntry::AcceptData {
                        slot,
                        ballot: inst.bal,
                        // persist only some shards on myself
                        reqs_cw: subset_copy,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                               slot, inst.bal);

            // send Accept messages to all peers, each getting its subset of
            // shards of data
            for peer in 0..self.population {
                if peer == self.id {
                    continue;
                }
                self.transport_hub.send_msg(
                    PeerMsg::Accept {
                        slot,
                        ballot: inst.bal,
                        reqs_cw: inst.reqs_cw.subset_copy(
                            Bitmap::from(
                                self.population,
                                Self::shards_for_replica(
                                    peer,
                                    self.population,
                                    self.config.shards_per_replica,
                                ),
                            ),
                            false,
                        )?,
                    },
                    peer,
                )?;
            }
            pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                               slot, inst.bal);
        }

        Ok(())
    }

    /// Handler of PrepareBal logging result chan recv.
    fn handle_logged_prepare_bal(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "finished PrepareBal logging for slot {} bal {}",
                           slot, self.insts[slot].bal);
        let inst = &self.insts[slot];
        let voted = if inst.voted.0 > 0 {
            Some(inst.voted.clone())
        } else {
            None
        };

        if self.is_leader {
            // on leader, finishing the logging of a PrepareBal entry
            // is equivalent to receiving a Prepare reply from myself
            // (as an acceptor role)
            self.handle_msg_prepare_reply(self.id, slot, inst.bal, voted)?;
        } else {
            // on follower replica, finishing the logging of a
            // PrepareBal entry leads to sending back a Prepare reply
            assert!(inst.replica_bk.is_some());
            let source = inst.replica_bk.as_ref().unwrap().source;
            self.transport_hub.send_msg(
                PeerMsg::PrepareReply {
                    slot,
                    ballot: inst.bal,
                    voted,
                },
                source,
            )?;
            pf_trace!(self.id; "sent PrepareReply -> {} for slot {} bal {}",
                               source, slot, inst.bal);
        }

        Ok(())
    }

    /// Handler of AcceptData logging result chan recv.
    fn handle_logged_accept_data(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "finished AcceptData logging for slot {} bal {}",
                           slot, self.insts[slot].bal);
        let inst = &self.insts[slot];

        if self.is_leader {
            // on leader, finishing the logging of an AcceptData entry
            // is equivalent to receiving an Accept reply from myself
            // (as an acceptor role)
            self.handle_msg_accept_reply(self.id, slot, inst.bal)?;
        } else {
            // on follower replica, finishing the logging of an
            // AcceptData entry leads to sending back an Accept reply
            assert!(inst.replica_bk.is_some());
            let source = inst.replica_bk.as_ref().unwrap().source;
            self.transport_hub.send_msg(
                PeerMsg::AcceptReply {
                    slot,
                    ballot: inst.bal,
                },
                source,
            )?;
            pf_trace!(self.id; "sent AcceptReply -> {} for slot {} bal {}",
                               source, slot, inst.bal);
        }

        Ok(())
    }

    /// Handler of CommitSlot logging result chan recv.
    fn handle_logged_commit_slot(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "finished CommitSlot logging for slot {} bal {}",
                                   slot, self.insts[slot].bal);
        assert!(self.insts[slot].status >= Status::Committed);

        // update index of the first non-committed instance
        if slot == self.commit_bar {
            while self.commit_bar < self.insts.len() {
                let inst = &mut self.insts[self.commit_bar];
                if inst.status < Status::Committed {
                    break;
                }

                if inst.reqs_cw.avail_shards() < self.quorum_cnt {
                    // can't execute if I don't have the complete request batch
                    pf_debug!(self.id; "postponing execution for slot {} (shards {}/{})",
                                       slot, inst.reqs_cw.avail_shards(), self.quorum_cnt);
                    break;
                } else if inst.reqs_cw.avail_data_shards() < self.quorum_cnt {
                    // have enough shards but need reconstruction
                    inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }
                let reqs = inst.reqs_cw.get_data()?;

                // submit commands in committed instance to the state machine
                // for execution
                if reqs.is_empty() {
                    inst.status = Status::Executed;
                } else if inst.status == Status::Committed {
                    for (cmd_idx, (_, req)) in reqs.iter().enumerate() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            self.state_machine.submit_cmd(
                                Self::make_command_id(self.commit_bar, cmd_idx),
                                cmd.clone(),
                            )?;
                        } else {
                            continue; // ignore other types of requests
                        }
                    }
                    pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                       reqs.len(), self.commit_bar);
                }

                self.commit_bar += 1;
            }
        }

        Ok(())
    }

    /// Synthesized handler of durable logging result chan recv.
    fn handle_log_result(
        &mut self,
        action_id: LogActionId,
        log_result: LogResult<LogEntry>,
    ) -> Result<(), SummersetError> {
        let (slot, entry_type) = Self::split_log_action_id(action_id);
        assert!(slot < self.insts.len());

        if let LogResult::Append { now_size } = log_result {
            assert!(now_size >= self.log_offset);
            self.log_offset = now_size;
        } else {
            return logged_err!(self.id; "unexpected log result type: {:?}", log_result);
        }

        match entry_type {
            Status::Preparing => self.handle_logged_prepare_bal(slot),
            Status::Accepting => self.handle_logged_accept_data(slot),
            Status::Committed => self.handle_logged_commit_slot(slot),
            _ => {
                logged_err!(self.id; "unexpected log entry type: {:?}", entry_type)
            }
        }
    }

    /// Handler of Prepare message from leader.
    fn handle_msg_prepare(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Prepare <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is not smaller than what I have seen:
        if ballot >= self.bal_max_seen {
            // locate instance in memory, filling in null instances if needed
            while self.insts.len() <= slot {
                self.insts.push(self.null_instance()?);
            }
            let inst = &mut self.insts[slot];
            assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Preparing;
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // update largest ballot seen
            self.bal_max_seen = ballot;

            // record update to largest prepare ballot
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Preparing),
                LogAction::Append {
                    entry: LogEntry::PrepareBal { slot, ballot },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                               slot, ballot);
        }

        Ok(())
    }

    /// Handler of Prepare reply from replica.
    fn handle_msg_prepare_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        voted: Option<(Ballot, RSCodeword<ReqBatch>)>,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received PrepareReply <- {} for slot {} bal {} shards {:?}",
                           peer, slot, ballot,
                           voted.as_ref().map(|(_, cw)| cw.avail_shards_map()));

        // if ballot is what I'm currently waiting on for Prepare replies:
        if ballot == self.bal_prep_sent {
            assert!(slot < self.insts.len());
            let inst = &mut self.insts[slot];

            // ignore spurious duplications and outdated replies
            if (inst.status != Status::Preparing) || (ballot < inst.bal) {
                return Ok(());
            }
            assert_eq!(inst.bal, ballot);
            assert!(self.bal_max_seen >= ballot);
            assert!(inst.leader_bk.is_some());
            let leader_bk = inst.leader_bk.as_mut().unwrap();
            if leader_bk.prepare_acks.get(peer)? {
                return Ok(());
            }

            // bookkeep this Prepare reply
            leader_bk.prepare_acks.set(peer, true)?;
            if let Some((bal, val)) = voted {
                #[allow(clippy::comparison_chain)]
                if bal > leader_bk.prepare_max_bal {
                    // is of ballot > current maximum, so discard the current
                    // codeword and take the replied codeword
                    leader_bk.prepare_max_bal = bal;
                    inst.reqs_cw = val;
                } else if bal == leader_bk.prepare_max_bal {
                    // is of ballot == the one currently taken, so merge the
                    // replied codeword into the current one
                    inst.reqs_cw.absorb_other(val)?;
                }
            }

            // if quorum size reached AND enough shards are known to
            // reconstruct the original data, enter Accept phase for this
            // instance using the request batch value constructed using shards
            // with the highest ballot number in quorum
            if leader_bk.prepare_acks.count() >= self.quorum_cnt
                && inst.reqs_cw.avail_shards() >= self.quorum_cnt
            {
                if inst.reqs_cw.avail_data_shards() < self.quorum_cnt {
                    // have enough shards but need reconstruction
                    inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }

                inst.status = Status::Accepting;
                pf_debug!(self.id; "enter Accept phase for slot {} bal {}",
                                   slot, inst.bal);

                // update bal_prepared
                assert!(self.bal_prepared <= ballot);
                self.bal_prepared = ballot;

                // if parity shards not computed yet, compute them now
                if inst.reqs_cw.avail_shards() < self.population {
                    inst.reqs_cw.compute_parity(Some(&self.rs_coder))?;
                }

                // record update to largest accepted ballot and corresponding data
                let subset_copy = inst.reqs_cw.subset_copy(
                    Bitmap::from(
                        self.population,
                        Self::shards_for_replica(
                            self.id,
                            self.population,
                            self.config.shards_per_replica,
                        ),
                    ),
                    false,
                )?;
                inst.voted = (ballot, subset_copy.clone());
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: LogEntry::AcceptData {
                            slot,
                            ballot,
                            reqs_cw: subset_copy,
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                                   slot, ballot);

                // send Accept messages to all peers
                for peer in 0..self.population {
                    if peer == self.id {
                        continue;
                    }
                    self.transport_hub.send_msg(
                        PeerMsg::Accept {
                            slot,
                            ballot,
                            reqs_cw: inst.reqs_cw.subset_copy(
                                Bitmap::from(
                                    self.population,
                                    Self::shards_for_replica(
                                        peer,
                                        self.population,
                                        self.config.shards_per_replica,
                                    ),
                                ),
                                false,
                            )?,
                        },
                        peer,
                    )?;
                }
                pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                                   slot, ballot);
            }
        }

        Ok(())
    }

    /// Handler of Accept message from leader.
    fn handle_msg_accept(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Accept <- {} for slot {} bal {} shards {:?}",
                           peer, slot, ballot, reqs_cw.avail_shards_map());

        // if ballot is not smaller than what I have made promises for:
        if ballot >= self.bal_max_seen {
            // locate instance in memory, filling in null instances if needed
            while self.insts.len() <= slot {
                self.insts.push(self.null_instance()?);
            }
            let inst = &mut self.insts[slot];
            assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Accepting;
            inst.reqs_cw = reqs_cw;
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // update largest ballot seen
            self.bal_max_seen = ballot;

            // record update to largest prepare ballot
            inst.voted = (ballot, inst.reqs_cw.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Accepting),
                LogAction::Append {
                    entry: LogEntry::AcceptData {
                        slot,
                        ballot,
                        reqs_cw: inst.reqs_cw.clone(),
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                               slot, ballot);
        }

        Ok(())
    }

    /// Handler of Accept reply from replica.
    fn handle_msg_accept_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received AcceptReply <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is what I'm currently waiting on for Accept replies:
        if ballot == self.bal_prepared {
            assert!(slot < self.insts.len());
            let inst = &mut self.insts[slot];

            // ignore spurious duplications and outdated replies
            if (inst.status != Status::Accepting) || (ballot < inst.bal) {
                return Ok(());
            }
            assert_eq!(inst.bal, ballot);
            assert!(self.bal_max_seen >= ballot);
            assert!(inst.leader_bk.is_some());
            let leader_bk = inst.leader_bk.as_mut().unwrap();
            if leader_bk.accept_acks.contains_key(&peer) {
                return Ok(());
            }

            // bookkeep this Accept reply
            leader_bk.accept_acks.insert(
                peer,
                Bitmap::from(
                    self.population,
                    Self::shards_for_replica(
                        peer,
                        self.population,
                        self.config.shards_per_replica,
                    ),
                ),
            );

            // if quorum size reached AND enough number of shards are
            // remembered, mark this instance as committed
            if leader_bk.accept_acks.len() as u8 >= self.quorum_cnt
                && Self::coverage_under_faults(
                    self.population,
                    &leader_bk.accept_acks,
                    self.config.fault_tolerance,
                ) >= self.quorum_cnt
            {
                inst.status = Status::Committed;
                pf_debug!(self.id; "committed instance at slot {} bal {}",
                                   slot, inst.bal);

                // record commit event
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Committed),
                    LogAction::Append {
                        entry: LogEntry::CommitSlot { slot },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(self.id; "submitted CommitSlot log action for slot {} bal {}",
                                   slot, inst.bal);

                // send Commit messages to all peers
                self.transport_hub
                    .bcast_msg(PeerMsg::Commit { slot }, None)?;
                pf_trace!(self.id; "broadcast Commit messages for slot {} bal {}",
                                   slot, ballot);
            }
        }

        Ok(())
    }

    /// Handler of Commit message from leader.
    fn handle_msg_commit(
        &mut self,
        peer: ReplicaId,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Commit <- {} for slot {}", peer, slot);

        // locate instance in memory, filling in null instances if needed
        while self.insts.len() <= slot {
            self.insts.push(self.null_instance()?);
        }
        let inst = &mut self.insts[slot];

        // ignore spurious duplications
        if inst.status != Status::Accepting {
            return Ok(());
        }

        // mark this instance as committed
        inst.status = Status::Committed;
        pf_debug!(self.id; "committed instance at slot {} bal {}",
                           slot, inst.bal);

        // record commit event
        self.storage_hub.submit_action(
            Self::make_log_action_id(slot, Status::Committed),
            LogAction::Append {
                entry: LogEntry::CommitSlot { slot },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(self.id; "submitted CommitSlot log action for slot {} bal {}",
                           slot, inst.bal);

        Ok(())
    }

    /// Handler of Recover message from leader.
    fn handle_msg_recover(
        &mut self,
        peer: ReplicaId,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Recover <- {} for slot {}", peer, slot);

        // locate instance in memory, filling in null instances if needed
        while self.insts.len() <= slot {
            self.insts.push(self.null_instance()?);
        }
        let inst = &mut self.insts[slot];

        // ignore spurious duplications; also ignore if I have nothing to send back
        if inst.status < Status::Accepting || inst.reqs_cw.avail_shards() == 0 {
            return Ok(());
        }

        // send back my ballot for this slot and the available shards
        self.transport_hub.send_msg(
            PeerMsg::RecoverReply {
                slot,
                ballot: inst.bal,
                reqs_cw: inst.reqs_cw.clone(),
            },
            peer,
        )?;
        pf_trace!(self.id; "sent RecoverReply message for slot {} bal {}", slot, inst.bal);

        Ok(())
    }

    /// Handler of Recover reply from replica.
    fn handle_msg_recover_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received RecoverReply <- {} for slot {} bal {} shards {:?}",
                           peer, slot, ballot, reqs_cw.avail_shards_map());
        assert!(slot < self.insts.len());
        assert!(self.insts[slot].status >= Status::Committed);
        let num_insts = self.insts.len();
        let inst = &mut self.insts[slot];

        // if reply not outdated and ballot is up-to-date
        if inst.status < Status::Executed && ballot >= inst.bal {
            // absorb the shards from this replica
            inst.reqs_cw.absorb_other(reqs_cw)?;

            // if enough shards have been gathered, can push execution forward
            if slot == self.commit_bar {
                while self.commit_bar < num_insts {
                    let inst = &mut self.insts[self.commit_bar];
                    if inst.status < Status::Committed
                        || inst.reqs_cw.avail_shards() < self.quorum_cnt
                    {
                        break;
                    }

                    if inst.reqs_cw.avail_data_shards() < self.quorum_cnt {
                        // have enough shards but need reconstruction
                        inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                    }
                    let reqs = inst.reqs_cw.get_data()?;

                    // submit commands in committed instance to the state machine
                    // for execution
                    if reqs.is_empty() {
                        inst.status = Status::Executed;
                    } else {
                        for (cmd_idx, (_, req)) in reqs.iter().enumerate() {
                            if let ApiRequest::Req { cmd, .. } = req {
                                self.state_machine.submit_cmd(
                                    Self::make_command_id(
                                        self.commit_bar,
                                        cmd_idx,
                                    ),
                                    cmd.clone(),
                                )?;
                            } else {
                                continue; // ignore other types of requests
                            }
                        }
                        pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                           reqs.len(), self.commit_bar);
                    }

                    self.commit_bar += 1;
                }
            }
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::Prepare { slot, ballot } => {
                self.handle_msg_prepare(peer, slot, ballot)
            }
            PeerMsg::PrepareReply {
                slot,
                ballot,
                voted,
            } => self.handle_msg_prepare_reply(peer, slot, ballot, voted),
            PeerMsg::Accept {
                slot,
                ballot,
                reqs_cw,
            } => self.handle_msg_accept(peer, slot, ballot, reqs_cw),
            PeerMsg::AcceptReply { slot, ballot } => {
                self.handle_msg_accept_reply(peer, slot, ballot)
            }
            PeerMsg::Commit { slot } => self.handle_msg_commit(peer, slot),
            PeerMsg::Recover { slot } => self.handle_msg_recover(peer, slot),
            PeerMsg::RecoverReply {
                slot,
                ballot,
                reqs_cw,
            } => self.handle_msg_recover_reply(peer, slot, ballot, reqs_cw),
            PeerMsg::Heartbeat { ballot } => self.heard_heartbeat(peer, ballot),
        }
    }

    /// Handler of state machine exec result chan recv.
    fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        assert!(slot < self.insts.len());
        pf_trace!(self.id; "executed cmd in instance at slot {} idx {}",
                           slot, cmd_idx);

        let inst = &mut self.insts[slot];
        let reqs = inst.reqs_cw.get_data()?;
        assert!(cmd_idx < reqs.len());
        let (client, ref req) = reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            if inst.external && self.external_api.has_client(client) {
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

        // if all commands in this instance have been executed, set status to
        // Executed and update `exec_bar`
        if cmd_idx == reqs.len() - 1 {
            inst.status = Status::Executed;
            pf_debug!(self.id; "executed all cmds in instance at slot {}",
                               slot);

            // update index of the first non-executed instance
            if slot == self.exec_bar {
                while self.exec_bar < self.insts.len() {
                    let inst = &mut self.insts[self.exec_bar];
                    if inst.status < Status::Executed {
                        break;
                    }
                    self.exec_bar += 1;
                }
            }
        }

        Ok(())
    }

    /// Becomes a leader, sends self-initiated Prepare messages to followers
    /// for all in-progress instances, and starts broadcasting heartbeats.
    fn become_a_leader(&mut self) -> Result<(), SummersetError> {
        if self.is_leader {
            return Ok(());
        }

        self.is_leader = true; // this starts broadcasting heartbeats
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;
        pf_info!(self.id; "becoming a leader...");

        // broadcast a heartbeat right now
        self.bcast_heartbeats()?;

        // make a greater ballot number and invalidate all in-progress instances
        self.bal_prepared = 0;
        self.bal_prep_sent = self.make_greater_ballot(self.bal_max_seen);
        self.bal_max_seen = self.bal_prep_sent;

        for (slot, inst) in self.insts.iter_mut().enumerate() {
            // redo Prepare phase for all in-progress instances
            if inst.status < Status::Committed {
                inst.bal = self.bal_prep_sent;
                inst.status = Status::Preparing;
                pf_debug!(self.id; "enter Prepare phase for slot {} bal {}",
                                   slot, inst.bal);

                // record update to largest prepare ballot
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Preparing),
                    LogAction::Append {
                        entry: LogEntry::PrepareBal {
                            slot,
                            ballot: self.bal_prep_sent,
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                                   slot, inst.bal);

                // send Prepare messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::Prepare {
                        slot,
                        ballot: self.bal_prep_sent,
                    },
                    None,
                )?;
                pf_trace!(self.id; "broadcast Prepare messages for slot {} bal {}",
                                   slot, inst.bal);
            }

            // do recovery reads for all committed instances that do not
            // hold enough available shards for reconstruction
            if inst.status == Status::Committed
                && inst.reqs_cw.avail_shards() < self.quorum_cnt
            {
                self.transport_hub
                    .bcast_msg(PeerMsg::Recover { slot }, None)?;
                pf_trace!(self.id; "broadcast Recover messages for slot {} bal {} shards {:?}",
                                   slot, inst.bal, inst.reqs_cw.avail_shards_map());
            }
        }

        Ok(())
    }

    /// Broadcasts heartbeats to all replicas.
    fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                ballot: self.bal_prep_sent,
            },
            None,
        )?;
        self.heard_heartbeat(self.id, self.bal_prep_sent)?;

        // pf_trace!(self.id; "broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Chooses a random hb_hear_timeout from the min-max range and kicks off
    /// the hb_hear_timer.
    fn kickoff_hb_hear_timer(&mut self) -> Result<(), SummersetError> {
        let timeout_ms = thread_rng().gen_range(
            self.config.hb_hear_timeout_min..=self.config.hb_hear_timeout_max,
        );

        // pf_trace!(self.id; "kickoff hb_hear_timer @ {} ms", timeout_ms);
        self.hb_hear_timer
            .kickoff(Duration::from_millis(timeout_ms))?;
        Ok(())
    }

    /// Heard a heartbeat from some other replica. If the heartbeat carries a
    /// high enough ballot number, refreshes my hearing timer and clears my
    /// leader status if I currently think I'm a leader.
    fn heard_heartbeat(
        &mut self,
        _peer: ReplicaId,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        // ignore outdated hearbeat
        if ballot < self.bal_max_seen {
            return Ok(());
        }

        // reset hearing timer
        self.kickoff_hb_hear_timer()?;

        // clear my leader status if it carries a higher ballot number
        if self.is_leader && ballot > self.bal_max_seen {
            self.is_leader = false;
            self.control_hub
                .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
            pf_info!(self.id; "no longer a leader...");
        }

        // pf_trace!(self.id; "heard heartbeat <- {} bal {}", peer, ballot);
        Ok(())
    }

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

        // reset leader heartbeat timer
        self.kickoff_hb_hear_timer()?;

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

    /// Apply a durable storage log entry for recovery.
    async fn recover_apply_entry(
        &mut self,
        entry: LogEntry,
    ) -> Result<(), SummersetError> {
        match entry {
            LogEntry::PrepareBal { slot, ballot } => {
                // locate instance in memory, filling in null instances if needed
                while self.insts.len() <= slot {
                    self.insts.push(self.null_instance()?);
                }
                // update instance state
                let inst = &mut self.insts[slot];
                inst.bal = ballot;
                inst.status = Status::Preparing;
                // update bal_prep_sent and bal_max_seen, reset bal_prepared
                if self.bal_prep_sent < ballot {
                    self.bal_prep_sent = ballot;
                }
                if self.bal_max_seen < ballot {
                    self.bal_max_seen = ballot;
                }
                self.bal_prepared = 0;
            }

            LogEntry::AcceptData {
                slot,
                ballot,
                reqs_cw,
            } => {
                // locate instance in memory, filling in null instances if needed
                while self.insts.len() <= slot {
                    self.insts.push(self.null_instance()?);
                }
                // update instance state
                let inst = &mut self.insts[slot];
                inst.bal = ballot;
                inst.status = Status::Accepting;
                inst.reqs_cw = reqs_cw.clone();
                inst.voted = (ballot, reqs_cw);
                // update bal_prepared and bal_max_seen
                if self.bal_prepared < ballot {
                    self.bal_prepared = ballot;
                }
                if self.bal_max_seen < ballot {
                    self.bal_max_seen = ballot;
                }
                assert!(self.bal_prepared <= self.bal_prep_sent);
            }

            LogEntry::CommitSlot { slot } => {
                assert!(slot < self.insts.len());
                // update instance state
                self.insts[slot].status = Status::Committed;
                // submit commands in contiguously committed instance to the
                // state machine
                if slot == self.commit_bar {
                    while self.commit_bar < self.insts.len() {
                        let inst = &mut self.insts[self.commit_bar];
                        if inst.status < Status::Committed {
                            break;
                        }
                        // check number of available shards
                        if inst.reqs_cw.avail_shards() < self.quorum_cnt {
                            // can't execute if I don't have the complete request batch
                            break;
                        } else if inst.reqs_cw.avail_data_shards()
                            < self.quorum_cnt
                        {
                            // have enough shards but need reconstruction
                            inst.reqs_cw
                                .reconstruct_data(Some(&self.rs_coder))?;
                        }
                        // execute all commands in this instance on state machine
                        // synchronously
                        for (_, req) in inst.reqs_cw.get_data()?.clone() {
                            if let ApiRequest::Req { cmd, .. } = req {
                                // using 0 as a special command ID
                                self.state_machine.submit_cmd(0, cmd)?;
                                let _ = self.state_machine.get_result().await?;
                            }
                        }
                        // update commit_bar and exec_bar
                        self.commit_bar += 1;
                        self.exec_bar += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Recover state from durable storage log.
    async fn recover_from_log(&mut self) -> Result<(), SummersetError> {
        assert_eq!(self.log_offset, 0);
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
                    entry: Some(entry),
                    end_offset,
                } => {
                    self.recover_apply_entry(entry).await?;
                    // update log offset
                    self.log_offset = end_offset;
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
                offset: self.log_offset,
            },
        )?;
        let (_, log_result) = self.storage_hub.get_result().await?;
        if let LogResult::Truncate {
            offset_ok: true, ..
        } = log_result
        {
            Ok(())
        } else {
            logged_err!(self.id; "unexpected log result type or failed truncate")
        }
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
                                    batch_interval_us, max_batch_size,
                                    backer_path, logger_sync,
                                    hb_hear_timeout_min, hb_hear_timeout_max,
                                    hb_send_interval_ms,
                                    fault_tolerance, shards_per_replica,
                                    perf_storage_a, perf_storage_b,
                                    perf_network_a, perf_network_b)?;
        if config.batch_interval_us == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_us '{}'",
                config.batch_interval_us
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
            protocol: SmrProtocol::Crossword,
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
        let quorum_cnt = (population / 2) + 1;
        if config.fault_tolerance > (population - quorum_cnt) {
            return logged_err!(id; "invalid config.fault_tolerance '{}'",
                                   config.fault_tolerance);
        }
        if config.shards_per_replica == 0
            || config.shards_per_replica > quorum_cnt
        {
            return logged_err!(id; "invalid config.shards_per_replica '{}'",
                                   config.shards_per_replica);
        }
        let rs_coder = ReedSolomon::new(
            quorum_cnt as usize,
            (population - quorum_cnt) as usize,
        )?;

        // proactively connect to some peers, then wait for all population
        // have been connected with me
        for (peer, addr) in to_peers {
            transport_hub.connect_to_peer(peer, addr).await?;
        }
        transport_hub.wait_for_group(population).await?;

        // setup external API module, ready to take in client requests
        let external_api = ExternalApi::new_and_setup(
            id,
            api_addr,
            Duration::from_micros(config.batch_interval_us),
            config.max_batch_size,
        )
        .await?;

        let mut hb_send_interval =
            time::interval(Duration::from_millis(config.hb_send_interval_ms));
        hb_send_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Ok(CrosswordReplica {
            id,
            population,
            quorum_cnt,
            config,
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
            control_hub,
            external_api,
            state_machine,
            storage_hub,
            transport_hub,
            hb_hear_timer: Timer::new(),
            hb_send_interval,
            is_leader: false,
            insts: vec![],
            bal_prep_sent: 0,
            bal_prepared: 0,
            bal_max_seen: 0,
            commit_bar: 0,
            exec_bar: 0,
            log_offset: 0,
            rs_coder,
        })
    }

    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
        // recover state from durable storage log
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
                    if let Err(e) = self.handle_msg_recv(peer, msg) {
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
                    if let Err(e) = self.become_a_leader() {
                        pf_error!(self.id; "error becoming a leader: {}", e);
                    }
                },

                // leader sending heartbeat
                _ = self.hb_send_interval.tick(), if !paused && self.is_leader => {
                    if let Err(e) = self.bcast_heartbeats() {
                        pf_error!(self.id; "error broadcasting heartbeats: {}", e);
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
pub struct CrosswordClient {
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
        pf_info!("c"; "connecting to manager '{}'...", manager);
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
                servers,
            } => {
                // shift to a new server_id if current one not active
                assert!(!servers.is_empty());
                while !servers.contains_key(&self.server_id) {
                    self.server_id = (self.server_id + 1) % population;
                }
                // establish connection to all servers
                self.servers = servers
                    .into_iter()
                    .map(|(id, info)| (id, info.0))
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

            while api_stub.recv_reply().await? != ApiReply::Leave {}
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
                    assert!(self.servers.contains_key(&redirect_id));
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
