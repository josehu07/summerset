//! MultiPaxos -- peer-peer messaging.

use super::*;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, LogAction};

// MultiPaxosReplica peer-peer messages handling
impl MultiPaxosReplica {
    /// Handler of Prepare message from leader.
    fn handle_msg_prepare(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received Prepare <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is not smaller than what I have seen:
        if ballot >= self.bal_max_seen {
            // update largest ballot seen and assumed leader
            self.check_leader(peer, ballot)?;
            self.kickoff_hb_hear_timer()?;

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= slot {
                self.insts.push(self.null_instance());
            }
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Preparing;
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // record update to largest prepare ballot
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Preparing),
                LogAction::Append {
                    entry: WalEntry::PrepareBal { slot, ballot },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                               slot, ballot);
        }

        Ok(())
    }

    /// Handler of Prepare reply from replica.
    pub fn handle_msg_prepare_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        voted: Option<(Ballot, ReqBatch)>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received PrepareReply <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is what I'm currently waiting on for Prepare replies:
        if ballot == self.bal_prep_sent {
            debug_assert!(slot < self.start_slot + self.insts.len());
            let is_leader = self.is_leader();
            let inst = &mut self.insts[slot - self.start_slot];

            // ignore spurious duplications and outdated replies
            if !is_leader
                || (inst.status != Status::Preparing)
                || (ballot < inst.bal)
            {
                return Ok(());
            }
            debug_assert_eq!(inst.bal, ballot);
            debug_assert!(self.bal_max_seen >= ballot);
            debug_assert!(inst.leader_bk.is_some());
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
                debug_assert!(self.bal_prepared <= ballot);
                self.bal_prepared = ballot;

                // record update to largest accepted ballot and corresponding data
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: WalEntry::AcceptData {
                            slot,
                            ballot,
                            reqs: inst.reqs.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                                   slot, ballot);

                // send Accept messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::Accept {
                        slot,
                        ballot,
                        reqs: inst.reqs.clone(),
                    },
                    None,
                )?;
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
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received Accept <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is not smaller than what I have made promises for:
        if ballot >= self.bal_max_seen {
            // update largest ballot seen and assumed leader
            self.check_leader(peer, ballot)?;
            self.kickoff_hb_hear_timer()?;

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= slot {
                self.insts.push(self.null_instance());
            }
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Accepting;
            inst.reqs = reqs.clone();
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // record update to largest prepare ballot
            inst.voted = (ballot, reqs.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Accepting),
                LogAction::Append {
                    entry: WalEntry::AcceptData { slot, ballot, reqs },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                               slot, ballot);
        }

        Ok(())
    }

    /// Handler of Accept reply from replica.
    pub fn handle_msg_accept_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        reply_ts: Option<SystemTime>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received AcceptReply <- {} for slot {} bal {}",
                           peer, slot, ballot);

        // if ballot is what I'm currently waiting on for Accept replies:
        if ballot == self.bal_prepared {
            debug_assert!(slot < self.start_slot + self.insts.len());
            let is_leader = self.is_leader();
            let inst = &mut self.insts[slot - self.start_slot];

            // ignore spurious duplications and outdated replies
            if !is_leader
                || (inst.status != Status::Accepting)
                || (ballot < inst.bal)
            {
                return Ok(());
            }
            debug_assert_eq!(inst.bal, ballot);
            debug_assert!(self.bal_max_seen >= ballot);
            debug_assert!(inst.leader_bk.is_some());
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

                // [for perf breakdown]
                if let Some(sw) = self.bd_stopwatch.as_mut() {
                    let _ = sw.record_now(slot, 2, reply_ts);
                    let _ = sw.record_now(slot, 3, None);
                }

                // record commit event
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Committed),
                    LogAction::Append {
                        entry: WalEntry::CommitSlot { slot },
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
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received Commit <- {} for slot {}", peer, slot);

        self.kickoff_hb_hear_timer()?;

        // locate instance in memory, filling in null instances if needed
        while self.start_slot + self.insts.len() <= slot {
            self.insts.push(self.null_instance());
        }
        let inst = &mut self.insts[slot - self.start_slot];

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
                entry: WalEntry::CommitSlot { slot },
                sync: self.config.logger_sync,
            },
        )?;
        pf_trace!(self.id; "submitted CommitSlot log action for slot {} bal {}",
                           slot, inst.bal);

        Ok(())
    }

    /// Handler of FillHoles message from a lagging peer.
    fn handle_msg_fill_holes(
        &mut self,
        peer: ReplicaId,
        slots: Vec<usize>,
    ) -> Result<(), SummersetError> {
        if !self.is_leader() {
            return Ok(());
        }
        pf_trace!(self.id; "received FillHoles <- {} for slots {:?}", peer, slots);

        let mut chunk_cnt = 0;
        for slot in slots {
            if slot < self.start_slot {
                continue;
            } else if slot >= self.start_slot + self.insts.len() {
                break;
            }
            let inst = &self.insts[slot - self.start_slot];

            if inst.status >= Status::Committed {
                // re-send Accept message for this slot
                self.transport_hub.send_msg(
                    PeerMsg::Accept {
                        slot,
                        ballot: self.bal_prepared,
                        reqs: inst.reqs.clone(),
                    },
                    peer,
                )?;
                pf_trace!(self.id; "sent Accept -> {} for slot {} bal {}",
                                   peer, slot, self.bal_prepared);
                chunk_cnt += 1;

                // inject heartbeats in the middle to keep peers happy
                if chunk_cnt >= self.config.msg_chunk_size {
                    self.transport_hub.bcast_msg(
                        PeerMsg::Heartbeat {
                            ballot: self.bal_max_seen,
                            exec_bar: self.exec_bar,
                            snap_bar: self.snap_bar,
                        },
                        None,
                    )?;
                    chunk_cnt = 0;
                }
            }
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    pub fn handle_msg_recv(
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
            PeerMsg::Accept { slot, ballot, reqs } => {
                self.handle_msg_accept(peer, slot, ballot, reqs)
            }
            PeerMsg::AcceptReply {
                slot,
                ballot,
                reply_ts,
            } => self.handle_msg_accept_reply(peer, slot, ballot, reply_ts),
            PeerMsg::Commit { slot } => self.handle_msg_commit(peer, slot),
            PeerMsg::FillHoles { slots } => {
                self.handle_msg_fill_holes(peer, slots)
            }
            PeerMsg::Heartbeat {
                ballot,
                exec_bar,
                snap_bar,
            } => self.heard_heartbeat(peer, ballot, exec_bar, snap_bar),
        }
    }
}
