//! Replication protocol: MultiPaxos.
//!
//! Multi-decree Paxos protocol. References:
//!   - <https://www.microsoft.com/en-us/research/uploads/prod/2016/12/paxos-simple-Copy.pdf>
//!   - <https://dl.acm.org/doi/pdf/10.1145/1281100.1281103>
//!   - <https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf>
//!   - <https://github.com/josehu07/learn-tla/tree/main/Dr.-TLA%2B-selected/multipaxos_practical>
//!   - <https://github.com/efficient/epaxos/blob/master/src/paxos/paxos.go>

use std::collections::HashMap;
use std::path::Path;
use std::net::SocketAddr;

use crate::utils::{SummersetError, ReplicaMap};
use crate::server::{
    ReplicaId, StateMachine, CommandResult, CommandId, ExternalApi, ApiRequest,
    ApiReply, StorageHub, LogAction, LogResult, LogActionId, TransportHub,
    GenericReplica,
};
use crate::client::{
    ClientId, ClientApiStub, ClientSendStub, ClientRecvStub, GenericClient,
};

use async_trait::async_trait;

use serde::{Serialize, Deserialize};

use tokio::time::Duration;

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigMultiPaxos {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

    /// Path to backing file.
    pub backer_path: String,

    /// Base capacity for most channels.
    pub base_chan_cap: usize,

    /// Capacity for req/reply channels.
    pub api_chan_cap: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigMultiPaxos {
    fn default() -> Self {
        ReplicaConfigMultiPaxos {
            batch_interval_us: 5000,
            backer_path: "/tmp/summerset.multipaxos.wal".into(),
            base_chan_cap: 10000,
            api_chan_cap: 100000,
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
    prepare_acks: ReplicaMap,

    /// Max ballot among received Prepare replies.
    prepare_max_bal: Ballot,

    /// Replicas from which I have received Accept confirmations.
    accept_acks: ReplicaMap,
}

/// Follower-side bookkeeping info for each instance received.
#[derive(Debug, Clone)]
struct ReplicaBookkeeping {
    /// Source leader replica ID for replyiing to Prepares and Accepts.
    source: ReplicaId,
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

    /// Leader-side bookkeeping info.
    leader_bk: Option<LeaderBookkeeping>,

    /// Follower-side bookkeeping info.
    replica_bk: Option<ReplicaBookkeeping>,
}

/// Stable storage log entry type.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum LogEntry {
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

/// Peer-peer message type.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PeerMsg {
    /// Prepare message from leader to replicas.
    Prepare { slot: usize, ballot: Ballot },

    /// Prepare reply from replica to leader.
    PrepareReply {
        slot: usize,
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

    /// Commit notification from leader to replicas.
    Commit { slot: usize },
}

/// MultiPaxos server replica module.
pub struct MultiPaxosReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Cluster size (number of replicas).
    population: u8,

    /// Majority quorum size.
    quorum_cnt: u8,

    /// Configuraiton parameters struct.
    config: ReplicaConfigMultiPaxos,

    /// Address string for client requests API.
    api_addr: SocketAddr,

    /// Local address strings for peer-peer connections.
    conn_addrs: HashMap<ReplicaId, SocketAddr>,

    /// Map from peer replica ID -> address.
    peer_addrs: HashMap<ReplicaId, SocketAddr>,

    /// ExternalApi module.
    external_api: ExternalApi,

    /// StateMachine module.
    state_machine: StateMachine,

    /// StorageHub module.
    storage_hub: StorageHub<LogEntry>,

    /// TransportHub module.
    transport_hub: TransportHub<PeerMsg>,

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
}

impl MultiPaxosReplica {
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

    /// Handler of client request batch chan recv.
    async fn handle_req_batch(
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
                    self.external_api
                        .send_reply(
                            ApiReply::Reply {
                                id: req_id,
                                result: None,
                                redirect: Some(next_replica),
                            },
                            client,
                        )
                        .await?;
                    pf_trace!(self.id; "redirected client {} to replica {}",
                                       client, next_replica);
                }
            }
            return Ok(());
        }

        // create a new instance in the first null slot (or append a new one
        // at the end if no holes exist)
        // TODO: maybe use a null_idx variable to better keep track of this
        let mut slot = self.insts.len();
        for s in self.commit_bar..self.insts.len() {
            let old_inst = &mut self.insts[s];
            if old_inst.status == Status::Null {
                old_inst.reqs = req_batch.clone();
                old_inst.leader_bk = Some(LeaderBookkeeping {
                    prepare_acks: ReplicaMap::new(self.population, false)?,
                    prepare_max_bal: 0,
                    accept_acks: ReplicaMap::new(self.population, false)?,
                });
                slot = s;
                break;
            }
        }
        if slot == self.insts.len() {
            let new_inst = Instance {
                bal: 0,
                status: Status::Null,
                reqs: req_batch.clone(),
                leader_bk: Some(LeaderBookkeeping {
                    prepare_acks: ReplicaMap::new(self.population, false)?,
                    prepare_max_bal: 0,
                    accept_acks: ReplicaMap::new(self.population, false)?,
                }),
                replica_bk: None,
            };
            self.insts.push(new_inst);
        }

        let mut replicas = ReplicaMap::new(self.population, true)?;
        replicas.set(self.id, false)?; // don't send messages to myself

        // decide whether we can enter fast path for this instance
        // TODO: remember to reset bal_prepared to 0, update bal_max_seen,
        //       and re-handle all Preparing & Accepting instances in autonomous
        //       Prepare initiation
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
            self.storage_hub
                .submit_action(
                    Self::make_log_action_id(slot, Status::Preparing),
                    LogAction::Append {
                        entry: LogEntry::PrepareBal {
                            slot,
                            ballot: self.bal_prep_sent,
                        },
                        sync: true,
                    },
                )
                .await?;
            pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                               slot, inst.bal);

            // send Prepare messages to replicas
            self.transport_hub
                .bcast_msg(
                    PeerMsg::Prepare {
                        slot,
                        ballot: self.bal_prep_sent,
                    },
                    replicas,
                )
                .await?;
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
            self.storage_hub
                .submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: LogEntry::AcceptData {
                            slot,
                            ballot: inst.bal,
                            reqs: req_batch.clone(),
                        },
                        sync: true,
                    },
                )
                .await?;
            pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                               slot, inst.bal);

            // send Accept messages to replicas
            self.transport_hub
                .bcast_msg(
                    PeerMsg::Accept {
                        slot,
                        ballot: inst.bal,
                        reqs: req_batch,
                    },
                    replicas,
                )
                .await?;
            pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                               slot, inst.bal);
        }

        Ok(())
    }

    /// Handler of PrepareBal logging result chan recv.
    async fn handle_logged_prepare_bal(
        &mut self,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "finished PrepareBal logging for slot {} bal {}",
                                   slot, self.insts[slot].bal);
        let inst = &self.insts[slot];
        let voted = if inst.status >= Status::Accepting {
            Some((inst.bal, inst.reqs.clone()))
        } else {
            None
        };

        if self.is_leader {
            // on leader, finishing the logging of a PrepareBal entry
            // is equivalent to receiving a Prepare reply from myself
            // (as an acceptor role)
            self.handle_msg_prepare_reply(self.id, slot, inst.bal, voted)
                .await?;
        } else {
            // on follower replica, finishing the logging of a
            // PrepareBal entry leads to sending back a Prepare reply
            assert!(inst.replica_bk.is_some());
            let source = inst.replica_bk.as_ref().unwrap().source;
            self.transport_hub
                .send_msg(
                    PeerMsg::PrepareReply {
                        slot,
                        ballot: inst.bal,
                        voted,
                    },
                    source,
                )
                .await?;
            pf_trace!(self.id; "sent PrepareReply -> {} for slot {} bal {}",
                                       source, slot, inst.bal);
        }

        Ok(())
    }

    /// Handler of AcceptData logging result chan recv.
    async fn handle_logged_accept_data(
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
            self.handle_msg_accept_reply(self.id, slot, inst.bal)
                .await?;
        } else {
            // on follower replica, finishing the logging of an
            // AcceptData entry leads to sending back an Accept reply
            assert!(inst.replica_bk.is_some());
            let source = inst.replica_bk.as_ref().unwrap().source;
            self.transport_hub
                .send_msg(
                    PeerMsg::AcceptReply {
                        slot,
                        ballot: inst.bal,
                    },
                    source,
                )
                .await?;
            pf_trace!(self.id; "sent AcceptReply -> {} for slot {} bal {}",
                                       source, slot, inst.bal);
        }

        Ok(())
    }

    /// Handler of CommitSlot logging result chan recv.
    async fn handle_logged_commit_slot(
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

                // submit commands in committed instance to the
                // state machine for execution
                if inst.reqs.is_empty() {
                    inst.status = Status::Executed;
                } else if inst.status == Status::Committed {
                    for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
                        if let ApiRequest::Req { cmd, .. } = req {
                            self.state_machine
                                .submit_cmd(
                                    Self::make_command_id(
                                        self.commit_bar,
                                        cmd_idx,
                                    ),
                                    cmd.clone(),
                                )
                                .await?;
                        } else {
                            continue; // ignore other types of requests
                        }
                    }
                    pf_trace!(self.id; "submitted {} exec commands for slot {}",
                                               inst.reqs.len(), self.commit_bar);
                }

                self.commit_bar += 1;
            }
        }

        Ok(())
    }

    /// Synthesized handler of durable logging result chan recv.
    async fn handle_log_result(
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
            Status::Preparing => self.handle_logged_prepare_bal(slot).await,
            Status::Accepting => self.handle_logged_accept_data(slot).await,
            Status::Committed => self.handle_logged_commit_slot(slot).await,
            _ => {
                logged_err!(self.id; "unexpected log entry type: {:?}", entry_type)
            }
        }
    }

    /// Handler of Prepare message from leader.
    async fn handle_msg_prepare(
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
                self.insts.push(Instance {
                    bal: 0,
                    status: Status::Null,
                    reqs: Vec::new(),
                    leader_bk: None,
                    replica_bk: None,
                });
            }
            let inst = &mut self.insts[slot];
            assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Preparing;
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // update largest ballot seen
            self.bal_max_seen = ballot;

            // record update to largest prepare ballot
            self.storage_hub
                .submit_action(
                    Self::make_log_action_id(slot, Status::Preparing),
                    LogAction::Append {
                        entry: LogEntry::PrepareBal { slot, ballot },
                        sync: true,
                    },
                )
                .await?;
            pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                               slot, ballot);
        }

        Ok(())
    }

    /// Handler of Prepare reply from replica.
    async fn handle_msg_prepare_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        voted: Option<(Ballot, ReqBatch)>,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received PrepareReply <- {} for slot {} bal {}",
                           peer, slot, ballot);

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
                if bal > leader_bk.prepare_max_bal {
                    leader_bk.prepare_max_bal = bal;
                    inst.reqs = val;
                }
            }

            // if quorum size reached, enter Accept phase for this instance
            // using the request batch value with the highest ballot number
            // in quorum
            if leader_bk.prepare_acks.count() >= self.quorum_cnt {
                inst.status = Status::Accepting;
                pf_debug!(self.id; "enter Accept phase for slot {} bal {}",
                                   slot, inst.bal);

                // update bal_prepared
                assert!(self.bal_prepared <= ballot);
                self.bal_prepared = ballot;

                // record update to largest accepted ballot and corresponding data
                self.storage_hub
                    .submit_action(
                        Self::make_log_action_id(slot, Status::Accepting),
                        LogAction::Append {
                            entry: LogEntry::AcceptData {
                                slot,
                                ballot,
                                reqs: inst.reqs.clone(),
                            },
                            sync: true,
                        },
                    )
                    .await?;
                pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                                   slot, ballot);

                let mut replicas = ReplicaMap::new(self.population, true)?;
                replicas.set(self.id, false)?; // don't send messages to myself

                // send Accept messages to replicas
                self.transport_hub
                    .bcast_msg(
                        PeerMsg::Accept {
                            slot,
                            ballot,
                            reqs: inst.reqs.clone(),
                        },
                        replicas,
                    )
                    .await?;
                pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                                   slot, ballot);
            }
        }

        Ok(())
    }

    /// Handler of Accept message from leader.
    async fn handle_msg_accept(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Accept <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is not smaller than what I have made promises for:
        if ballot >= self.bal_max_seen {
            // locate instance in memory, filling in null instances if needed
            while self.insts.len() <= slot {
                self.insts.push(Instance {
                    bal: 0,
                    status: Status::Null,
                    reqs: Vec::new(),
                    leader_bk: None,
                    replica_bk: None,
                });
            }
            let inst = &mut self.insts[slot];
            assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Accepting;
            inst.reqs = reqs.clone();
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // update largest ballot seen
            self.bal_max_seen = ballot;

            // record update to largest prepare ballot
            self.storage_hub
                .submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: LogEntry::AcceptData { slot, ballot, reqs },
                        sync: true,
                    },
                )
                .await?;
            pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                               slot, ballot);
        }

        Ok(())
    }

    /// Handler of Accept reply from replica.
    async fn handle_msg_accept_reply(
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
            if leader_bk.accept_acks.get(peer)? {
                return Ok(());
            }

            // bookkeep this Accept reply
            leader_bk.accept_acks.set(peer, true)?;

            // if quorum size reached, mark this instance as committed
            if leader_bk.accept_acks.count() >= self.quorum_cnt {
                inst.status = Status::Committed;
                pf_debug!(self.id; "committed instance at slot {} bal {}",
                                   slot, inst.bal);

                // record commit event
                self.storage_hub
                    .submit_action(
                        Self::make_log_action_id(slot, Status::Committed),
                        LogAction::Append {
                            entry: LogEntry::CommitSlot { slot },
                            sync: true,
                        },
                    )
                    .await?;
                pf_trace!(self.id; "submitted CommitSlot log action for slot {} bal {}",
                                   slot, inst.bal);

                let mut replicas = ReplicaMap::new(self.population, true)?;
                replicas.set(self.id, false)?; // don't send messages to myself

                // send Commit messages to replicas
                self.transport_hub
                    .bcast_msg(
                        PeerMsg::Accept {
                            slot,
                            ballot,
                            reqs: inst.reqs.clone(),
                        },
                        replicas,
                    )
                    .await?;
                pf_trace!(self.id; "broadcast Accept messages for slot {} bal {}",
                                  slot, ballot);
            }
        }

        Ok(())
    }

    /// Handler of Commit message from leader.
    /// TODO: take care of missing/lost Commit messages
    async fn handle_msg_commit(
        &mut self,
        peer: ReplicaId,
        slot: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Commit <- {} for slot {}", peer, slot);

        // locate instance in memory, filling in null instances if needed
        while self.insts.len() <= slot {
            self.insts.push(Instance {
                bal: 0,
                status: Status::Null,
                reqs: Vec::new(),
                leader_bk: None,
                replica_bk: None,
            });
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
        self.storage_hub
            .submit_action(
                Self::make_log_action_id(slot, Status::Committed),
                LogAction::Append {
                    entry: LogEntry::CommitSlot { slot },
                    sync: true,
                },
            )
            .await?;
        pf_trace!(self.id; "submitted CommitSlot log action for slot {} bal {}",
                           slot, inst.bal);

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    async fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::Prepare { slot, ballot } => {
                self.handle_msg_prepare(peer, slot, ballot).await
            }
            PeerMsg::PrepareReply {
                slot,
                ballot,
                voted,
            } => {
                self.handle_msg_prepare_reply(peer, slot, ballot, voted)
                    .await
            }
            PeerMsg::Accept { slot, ballot, reqs } => {
                self.handle_msg_accept(peer, slot, ballot, reqs).await
            }
            PeerMsg::AcceptReply { slot, ballot } => {
                self.handle_msg_accept_reply(peer, slot, ballot).await
            }
            PeerMsg::Commit { slot } => {
                self.handle_msg_commit(peer, slot).await
            }
        }
    }

    /// Handler of state machine exec result chan recv.
    /// TODO: reply to client, update Status::Executed and exec_bar properly
    async fn handle_cmd_result(
        &mut self,
        cmd_id: CommandId,
        cmd_result: CommandResult,
    ) -> Result<(), SummersetError> {
        let (slot, cmd_idx) = Self::split_command_id(cmd_id);
        assert!(slot < self.insts.len());
        pf_trace!(self.id; "executed cmd in instance at slot {} idx {}",
                           slot, cmd_idx);

        let inst = &mut self.insts[slot];
        assert!(cmd_idx < inst.reqs.len());
        let (client, ref req) = inst.reqs[cmd_idx];

        // reply command result back to client
        if let ApiRequest::Req { id: req_id, .. } = req {
            self.external_api
                .send_reply(
                    ApiReply::Reply {
                        id: *req_id,
                        result: Some(cmd_result),
                        redirect: None,
                    },
                    client,
                )
                .await?;
            pf_trace!(self.id; "replied -> client {} for slot {} idx {}",
                               client, slot, cmd_idx);
        } else {
            return logged_err!(self.id; "unexpected API request type");
        }

        // if all commands in this instance have been executed, set status to
        // Executed and update `exec_bar`
        if cmd_idx == inst.reqs.len() - 1 {
            inst.status = Status::Executed;
            pf_debug!(self.id; "executed all cmds in instance at slot {}",
                               slot);

            // update index of the first non-executed instance
            if slot == self.exec_bar {
                while self.exec_bar < self.insts.len() {
                    let inst = &mut self.insts[self.exec_bar];
                    if inst.status < Status::Committed {
                        break;
                    }
                    self.exec_bar += 1;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl GenericReplica for MultiPaxosReplica {
    fn new(
        id: ReplicaId,
        population: u8,
        api_addr: SocketAddr,
        conn_addrs: HashMap<ReplicaId, SocketAddr>,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if population < 3 {
            return Err(SummersetError(format!(
                "invalid population {}",
                population
            )));
        }
        if id >= population {
            return Err(SummersetError(format!(
                "invalid replica ID {} / {}",
                id, population
            )));
        }
        if conn_addrs.len() != peer_addrs.len() {
            return logged_err!(
                id;
                "size of conn_addrs {} != size of peer_addrs {}",
                conn_addrs.len(), peer_addrs.len()
            );
        }

        let config = parsed_config!(config_str => ReplicaConfigMultiPaxos;
                                    batch_interval_us, backer_path,
                                    base_chan_cap, api_chan_cap)?;
        if config.batch_interval_us == 0 {
            return logged_err!(
                id;
                "invalid config.batch_interval_us '{}'",
                config.batch_interval_us
            );
        }
        if config.base_chan_cap == 0 {
            return logged_err!(
                id;
                "invalid config.base_chan_cap {}",
                config.base_chan_cap
            );
        }
        if config.api_chan_cap == 0 {
            return logged_err!(
                id;
                "invalid config.api_chan_cap {}",
                config.api_chan_cap
            );
        }

        Ok(MultiPaxosReplica {
            id,
            population,
            quorum_cnt: (population / 2) + 1,
            config,
            api_addr,
            conn_addrs,
            peer_addrs,
            external_api: ExternalApi::new(id),
            state_machine: StateMachine::new(id),
            storage_hub: StorageHub::new(id),
            transport_hub: TransportHub::new(id, population),
            is_leader: false,
            insts: vec![],
            bal_prep_sent: 0,
            bal_prepared: 0,
            bal_max_seen: 0,
            commit_bar: 0,
            exec_bar: 0,
            log_offset: 0,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        self.state_machine
            .setup(self.config.api_chan_cap, self.config.api_chan_cap)
            .await?;

        self.storage_hub
            .setup(
                Path::new(&self.config.backer_path),
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;

        self.transport_hub
            .setup(
                &self.conn_addrs,
                self.config.base_chan_cap,
                self.config.base_chan_cap,
            )
            .await?;
        if !self.peer_addrs.is_empty() {
            self.transport_hub
                .group_connect(&self.conn_addrs, &self.peer_addrs)
                .await?;
        }

        self.external_api
            .setup(
                self.api_addr,
                Duration::from_micros(self.config.batch_interval_us),
                self.config.api_chan_cap,
                self.config.api_chan_cap,
            )
            .await?;

        Ok(())
    }

    async fn run(&mut self) {
        // TODO: proper leader election
        if self.id == 0 {
            self.is_leader = true;
        }

        loop {
            tokio::select! {
                // client request batch
                req_batch = self.external_api.get_req_batch() => {
                    if let Err(e) = req_batch {
                        pf_error!(self.id; "error getting req batch: {}", e);
                        continue;
                    }
                    let req_batch = req_batch.unwrap();
                    if let Err(e) = self.handle_req_batch(req_batch).await {
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
                    if let Err(e) = self.handle_log_result(action_id, log_result).await {
                        pf_error!(self.id; "error handling log result {}: {}",
                                           action_id, e);
                    }
                },

                // message from peer
                msg = self.transport_hub.recv_msg() => {
                    if let Err(e) = msg {
                        pf_error!(self.id; "error receiving peer msg: {}", e);
                        continue;
                    }
                    let (peer, msg) = msg.unwrap();
                    if let Err(e) = self.handle_msg_recv(peer, msg).await {
                        pf_error!(self.id; "error handling msg recv <- {}: {}", peer, e);
                    }
                }

                // state machine execution result
                cmd_result = self.state_machine.get_result() => {
                    if let Err(e) = cmd_result {
                        pf_error!(self.id; "error getting cmd result: {}", e);
                        continue;
                    }
                    let (cmd_id, cmd_result) = cmd_result.unwrap();
                    if let Err(e) = self.handle_cmd_result(cmd_id, cmd_result).await {
                        pf_error!(self.id; "error handling cmd result {}: {}", cmd_id, e);
                    }
                },
            }
        }
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigMultiPaxos {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigMultiPaxos {
    fn default() -> Self {
        ClientConfigMultiPaxos { init_server_id: 0 }
    }
}

/// MultiPaxos client-side module.
pub struct MultiPaxosClient {
    /// Client ID.
    id: ClientId,

    /// Server addresses of the service.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Configuration parameters struct.
    config: ClientConfigMultiPaxos,

    /// CLient API stub for creating new connections.
    api_stub: ClientApiStub,

    /// Connection stubs for communicating with the current chosen server.
    conn_stubs: Option<(ClientSendStub, ClientRecvStub)>,
}

#[async_trait]
impl GenericClient for MultiPaxosClient {
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        if servers.is_empty() {
            return logged_err!(id; "empty servers list");
        }

        let config = parsed_config!(config_str => ClientConfigMultiPaxos;
                                    init_server_id)?;
        if !servers.contains_key(&config.init_server_id) {
            return logged_err!(
                id;
                "init_server_id {} not found in servers",
                config.init_server_id
            );
        }

        let api_stub = ClientApiStub::new(id);

        Ok(MultiPaxosClient {
            id,
            servers,
            config,
            api_stub,
            conn_stubs: None,
        })
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        // connect to default replica
        self.api_stub
            .connect(self.servers[&self.config.init_server_id])
            .await
            .map(|stubs| {
                self.conn_stubs = Some(stubs);
            })
    }

    async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        match self.conn_stubs {
            Some((ref mut send_stub, _)) => {
                send_stub.send_req(req).await?;
                Ok(())
            }
            None => logged_err!(self.id; "client is not set up"),
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        match self.conn_stubs {
            Some((ref mut send_stub, ref mut recv_stub)) => {
                let reply = recv_stub.recv_reply().await?;

                if let ApiReply::Reply {
                    ref result,
                    ref redirect,
                    ..
                } = reply
                {
                    // if the current server redirects me to a different server
                    if result.is_none() && redirect.is_some() {
                        let redirect_id = redirect.unwrap();
                        assert!((redirect_id as usize) < self.servers.len());
                        send_stub.send_req(ApiRequest::Leave).await?;
                        if let Ok(stubs) = self
                            .api_stub
                            .connect(self.servers[&redirect_id])
                            .await
                        {
                            self.conn_stubs = Some(stubs);
                            pf_debug!(self.id; "redirected to replica {} '{}'",
                                               redirect_id, self.servers[&redirect_id]);
                        }
                    }
                }

                Ok(reply)
            }
            None => logged_err!(self.id; "client is not set up"),
        }
    }
}
