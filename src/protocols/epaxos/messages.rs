//! EPaxos -- peer-peer messaging.

use std::cmp;

use super::*;

use crate::server::{LogAction, ReplicaId};
use crate::utils::SummersetError;

// EPaxosReplica peer-peer messages handling
impl EPaxosReplica {
    /// Handler of PreAccept message from command leader.
    fn handle_msg_pre_accept(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
        mut seq: SeqNum,
        mut deps: DepSet,
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received PreAccept <- {} for slot {} bal {} seq {} deps {}",
            peer,
            slot,
            ballot,
            seq,
            deps
        );

        // locate instance in memory, filling in null instances if needed
        while self.start_col + self.insts[row].len() <= col {
            let inst = self.null_instance();
            self.insts[row].push(inst);
        }
        let inst_bal = self.insts[row][col - self.start_col].bal;

        // if ballot is up-to-date:
        if ballot >= inst_bal {
            // update seq and deps according to my knowledge
            deps.union(&Self::identify_deps(
                &reqs,
                self.population,
                &self.highest_cols,
            ));
            seq = seq.max(1 + self.max_seq_num(&deps));

            let inst = &mut self.insts[row][col - self.start_col];
            inst.bal = ballot;
            inst.status = Status::PreAccepting;
            inst.seq = seq;
            inst.deps = deps.clone();
            inst.reqs.clone_from(&reqs);
            Self::refresh_highest_cols(
                slot,
                &reqs,
                self.population,
                &mut self.highest_cols,
            );
            if let Some(replica_bk) = inst.replica_bk.as_mut() {
                replica_bk.source = peer;
            } else {
                inst.replica_bk = Some(ReplicaBookkeeping { source: peer });
            }

            //  record update to instance status & data
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::PreAccepting),
                LogAction::Append {
                    entry: WalEntry::PreAcceptSlot {
                        slot,
                        ballot,
                        seq,
                        deps,
                        reqs,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(
                "submitted PreAcceptSlot log action for slot {} bal {} seq {} deps {}",
                slot,
                ballot,
                inst.seq,
                inst.deps
            );
        }

        Ok(())
    }

    /// Handler of PreAccept reply from replica.
    pub(super) fn handle_msg_pre_accept_reply(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received PreAcceptReply <- {} for slot {} bal {} seq {} deps {}",
            peer,
            slot,
            ballot,
            seq,
            deps
        );

        // ignore spurious duplications and outdated replies
        if col >= self.start_col + self.insts[row].len() {
            return Ok(());
        }
        let inst = &mut self.insts[row][col - self.start_col];
        if inst.status != Status::PreAccepting
            || inst.bal != ballot
            || inst.leader_bk.is_none()
        {
            return Ok(());
        }
        let leader_bk = inst.leader_bk.as_mut().unwrap();
        if leader_bk.pre_accept_acks.get(peer)? {
            return Ok(());
        }

        // bookkeep this PreAccept reply
        leader_bk.pre_accept_replies.push((seq, deps.clone()));
        leader_bk.pre_accept_acks.set(peer, true)?;

        // check the set of replies received so far:
        // NOTE: move the start-phase blocks into common helper functions
        match Self::fast_quorum_eligibility(
            leader_bk,
            self.population,
            self.super_quorum_cnt,
        ) {
            Some(true) => {
                // fast quorum size reached and has enough non-conflicting replies,
                // mark this instance as committed
                inst.status = Status::Committed;
                pf_debug!(
                    "committed instance at slot {} bal {} fast path",
                    slot,
                    inst.bal
                );

                // record commit event
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Committed),
                    LogAction::Append {
                        entry: WalEntry::CommitSlot {
                            slot,
                            ballot: inst.bal,
                            seq: inst.seq,
                            deps: inst.deps.clone(),
                            reqs: inst.reqs.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted CommitSlot log action for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps
                );

                // broadcast CommitNotice messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::CommitNotice {
                        slot,
                        ballot: inst.bal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs: inst.reqs.clone(),
                    },
                    None,
                )?;
                pf_trace!(
                    "broadcast CommitNotice messages for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps,
                );
            }

            Some(false) => {
                // enough replies received such that the fast-path commit condition
                // will never be reached; initiate slow-path Accepts directly
                inst.status = Status::Accepting;
                pf_debug!(
                    "enter Accept phase for slot {} bal {}",
                    slot,
                    inst.bal
                );

                // take union of all deps from all replies
                for (rseq, rdeps) in leader_bk.pre_accept_replies.iter() {
                    inst.deps.union(rdeps);
                    inst.seq = inst.seq.max(*rseq);
                }

                // record update to instance status & data
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: WalEntry::AcceptSlot {
                            slot,
                            ballot,
                            seq: inst.seq,
                            deps: inst.deps.clone(),
                            reqs: inst.reqs.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted AcceptSlot log action for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps
                );

                // broadcast Accept messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::Accept {
                        slot,
                        ballot: inst.bal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs: inst.reqs.clone(),
                    },
                    None,
                )?;
                pf_trace!(
                    "broadcast Accept messages for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps,
                );
            }

            None => {} // not enough information from replies yet
        }

        Ok(())
    }

    /// Handler of Accept message from command leader.
    fn handle_msg_accept(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received Accept <- {} for slot {} bal {} seq {} deps {}",
            peer,
            slot,
            ballot,
            seq,
            deps,
        );

        // locate instance in memory, filling in null instances if needed
        while self.start_col + self.insts[row].len() <= col {
            let inst = self.null_instance();
            self.insts[row].push(inst);
        }
        let inst = &mut self.insts[row][col - self.start_col];

        // if ballot is up-to-date:
        if ballot >= inst.bal {
            inst.bal = ballot;
            inst.status = Status::Accepting;
            inst.seq = seq;
            inst.deps = deps.clone();
            inst.reqs.clone_from(&reqs);
            Self::refresh_highest_cols(
                slot,
                &reqs,
                self.population,
                &mut self.highest_cols,
            );
            if let Some(replica_bk) = inst.replica_bk.as_mut() {
                replica_bk.source = peer;
            } else {
                inst.replica_bk = Some(ReplicaBookkeeping { source: peer });
            }

            // record update to instance status & data
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Accepting),
                LogAction::Append {
                    entry: WalEntry::AcceptSlot {
                        slot,
                        ballot,
                        seq,
                        deps,
                        reqs,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(
                "submitted AcceptSlot log action for slot {} bal {} seq {} deps {}",
                slot,
                ballot,
                inst.seq,
                inst.deps
            );
        }

        Ok(())
    }

    /// Handler of Accept reply from replica.
    pub(super) fn handle_msg_accept_reply(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received AcceptReply <- {} for slot {} bal {}",
            peer,
            slot,
            ballot,
        );

        // ignore spurious duplications and outdated replies
        if col >= self.start_col + self.insts[row].len() {
            return Ok(());
        }
        let inst = &mut self.insts[row][col - self.start_col];
        if inst.status != Status::Accepting
            || inst.bal != ballot
            || inst.leader_bk.is_none()
        {
            return Ok(());
        }
        let leader_bk = inst.leader_bk.as_mut().unwrap();
        if leader_bk.accept_acks.get(peer)? {
            return Ok(());
        }

        // bookkeep this Accept reply
        leader_bk.accept_acks.set(peer, true)?;

        // if enough Accept replies received, mark this instance as Committed
        if leader_bk.accept_acks.count() >= self.simple_quorum_cnt {
            inst.status = Status::Committed;
            pf_debug!(
                "committed instance at slot {} bal {} slow path",
                slot,
                inst.bal
            );

            // record commit event
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Committed),
                LogAction::Append {
                    entry: WalEntry::CommitSlot {
                        slot,
                        ballot: inst.bal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs: inst.reqs.clone(),
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(
                "submitted CommitSlot log action for slot {} bal {} seq {} deps {}",
                slot,
                inst.bal,
                inst.seq,
                inst.deps
            );

            // broadcast CommitNotice messages to all peers
            self.transport_hub.bcast_msg(
                PeerMsg::CommitNotice {
                    slot,
                    ballot: inst.bal,
                    seq: inst.seq,
                    deps: inst.deps.clone(),
                    reqs: inst.reqs.clone(),
                },
                None,
            )?;
            pf_trace!(
                "broadcast CommitNotice messages for slot {} bal {} seq {} deps {}",
                slot,
                inst.bal,
                inst.seq,
                inst.deps,
            );
        }

        Ok(())
    }

    /// Handler of CommitNotice message from command leader.
    fn handle_msg_commit_notice(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
        seq: SeqNum,
        deps: DepSet,
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received CommitNotice <- {} for slot {} bal {} seq {} deps {}",
            peer,
            slot,
            ballot,
            seq,
            deps
        );

        // locate instance in memory, filling in null instances if needed
        while self.start_col + self.insts[row].len() <= col {
            let inst = self.null_instance();
            self.insts[row].push(inst);
        }
        let inst_bal = self.insts[row][col - self.start_col].bal;

        // if ballot is up-to-date
        if ballot >= inst_bal {
            let inst = &mut self.insts[row][col - self.start_col];
            inst.bal = ballot;
            inst.status = Status::Committed;
            inst.seq = seq;
            inst.deps = deps.clone();
            inst.reqs.clone_from(&reqs);
            Self::refresh_highest_cols(
                slot,
                &reqs,
                self.population,
                &mut self.highest_cols,
            );

            // record commit event
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Committed),
                LogAction::Append {
                    entry: WalEntry::CommitSlot {
                        slot,
                        ballot: inst.bal,
                        seq,
                        deps: deps.clone(),
                        reqs,
                    },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(
                "submitted CommitSlot log action for slot {} bal {} seq {} deps {}",
                slot,
                inst.bal,
                seq,
                deps
            );
        }

        Ok(())
    }

    /// Handler of ExpPrepare message from leader.
    fn handle_msg_exp_prepare(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received Prepare <- {} for slot {} bal {}",
            peer,
            slot,
            ballot,
        );

        // locate instance in memory, filling in null instances if needed
        while self.start_col + self.insts[row].len() <= col {
            let inst = self.null_instance();
            self.insts[row].push(inst);
        }
        let inst = &self.insts[row][col - self.start_col];

        // if ballot is larger than what I've ever seen for this instance:
        if ballot > inst.bal {
            // send back ExpPrepare reply
            self.transport_hub.send_msg(
                PeerMsg::ExpPrepareReply {
                    slot,
                    ballot,
                    voted_bal: inst.bal,
                    voted_seq: inst.seq,
                    voted_deps: inst.deps.clone(),
                    voted_reqs: inst.reqs.clone(),
                },
                peer,
            )?;
            pf_trace!(
                "sent ExpPrepareReply -> {} for slot {} bal {} seq {} deps {}",
                peer,
                slot,
                inst.bal,
                inst.seq,
                inst.deps,
            );
        }

        Ok(())
    }

    /// Handler of ExpPrepare reply from replica.
    pub(super) fn handle_msg_exp_prepare_reply(
        &mut self,
        peer: ReplicaId,
        slot: SlotIdx,
        ballot: Ballot,
        voted_bal: Ballot,
        voted_seq: SeqNum,
        voted_deps: DepSet,
        voted_reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        let (row, col) = slot.unpack();
        if col < self.start_col {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received ExpPrepareReply <- {} for slot {} bal {} seq {} deps {}",
            peer,
            slot,
            voted_bal,
            voted_seq,
            voted_deps,
        );

        // ignore spurious duplications and outdated replies
        debug_assert!(ballot > voted_bal);
        if col >= self.start_col + self.insts[row].len() {
            return Ok(());
        }
        let inst = &mut self.insts[row][col - self.start_col];
        if ballot <= inst.bal || inst.leader_bk.is_none() {
            return Ok(());
        }
        let leader_bk = inst.leader_bk.as_mut().unwrap();
        if leader_bk.exp_prepare_acks.get(peer)? {
            return Ok(());
        }

        // bookkeep this ExpPrepare reply
        if voted_bal > leader_bk.exp_prepare_max_bal {
            leader_bk.exp_prepare_voteds =
                vec![(voted_seq, voted_deps, voted_reqs)];
        } else if voted_bal == leader_bk.exp_prepare_max_bal {
            leader_bk
                .exp_prepare_voteds
                .push((voted_seq, voted_deps, voted_reqs));
        }
        leader_bk.exp_prepare_acks.set(peer, true)?;

        // check the set of replies with highest ballot received so far:
        // NOTE: move the start-phase blocks into common helper functions
        match Self::exp_prepare_next_step(
            row as ReplicaId,
            leader_bk,
            self.simple_quorum_cnt,
        ) {
            Some((Status::Committed, seq, deps, reqs)) => {
                // can commit this slot
                inst.bal = ballot;
                inst.status = Status::Committed;
                inst.seq = seq;
                inst.deps = deps;
                inst.reqs.clone_from(&reqs);
                Self::refresh_highest_cols(
                    slot,
                    &reqs,
                    self.population,
                    &mut self.highest_cols,
                );
                pf_debug!(
                    "committed instance at slot {} bal {} by prepare",
                    slot,
                    inst.bal
                );

                // record commit event
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Committed),
                    LogAction::Append {
                        entry: WalEntry::CommitSlot {
                            slot,
                            ballot: inst.bal,
                            seq: inst.seq,
                            deps: inst.deps.clone(),
                            reqs: reqs.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted CommitSlot log action for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps
                );

                // broadcast CommitNotice messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::CommitNotice {
                        slot,
                        ballot: inst.bal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs,
                    },
                    None,
                )?;
                pf_trace!(
                    "broadcast CommitNotice messages for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps,
                );
            }

            Some((Status::Accepting, seq, deps, reqs)) => {
                // need to run Accept phase
                inst.bal = ballot;
                inst.status = Status::Accepting;
                inst.seq = seq;
                inst.deps = deps;
                inst.reqs.clone_from(&reqs);
                Self::refresh_highest_cols(
                    slot,
                    &reqs,
                    self.population,
                    &mut self.highest_cols,
                );
                pf_debug!(
                    "enter Accept phase for slot {} bal {}",
                    slot,
                    inst.bal
                );

                // record update to instance status & data
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: WalEntry::AcceptSlot {
                            slot,
                            ballot,
                            seq: inst.seq,
                            deps: inst.deps.clone(),
                            reqs: reqs.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted AcceptSlot log action for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps
                );

                // broadcast Accept messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::Accept {
                        slot,
                        ballot: inst.bal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs,
                    },
                    None,
                )?;
                pf_trace!(
                    "broadcast Accept messages for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps,
                );
            }

            Some((Status::PreAccepting, seq, deps, reqs)) => {
                // need to start over from PreAccept
                inst.bal = ballot;
                inst.status = Status::PreAccepting;
                inst.seq = seq;
                inst.deps = deps;
                inst.reqs.clone_from(&reqs);
                Self::refresh_highest_cols(
                    slot,
                    &reqs,
                    self.population,
                    &mut self.highest_cols,
                );

                // record update to instance status & data
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::PreAccepting),
                    LogAction::Append {
                        entry: WalEntry::PreAcceptSlot {
                            slot,
                            ballot: inst.bal,
                            seq: inst.seq,
                            deps: inst.deps.clone(),
                            reqs: reqs.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted PreAcceptSlot log action for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps,
                );

                // broadcast PreAccept messages to all peers
                self.transport_hub.bcast_msg(
                    PeerMsg::PreAccept {
                        slot,
                        ballot: inst.bal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs,
                    },
                    None,
                )?;
                pf_trace!(
                    "broadcast PreAccept messages for slot {} bal {} seq {} deps {}",
                    slot,
                    inst.bal,
                    inst.seq,
                    inst.deps,
                );
            }

            _ => {} // not enough information from replies yet
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    pub(super) async fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::PreAccept {
                slot,
                ballot,
                seq,
                deps,
                reqs,
            } => {
                self.handle_msg_pre_accept(peer, slot, ballot, seq, deps, reqs)
            }
            PeerMsg::PreAcceptReply {
                slot,
                ballot,
                seq,
                deps,
            } => {
                self.handle_msg_pre_accept_reply(peer, slot, ballot, seq, deps)
            }
            PeerMsg::Accept {
                slot,
                ballot,
                seq,
                deps,
                reqs,
            } => self.handle_msg_accept(peer, slot, ballot, seq, deps, reqs),
            PeerMsg::AcceptReply { slot, ballot } => {
                self.handle_msg_accept_reply(peer, slot, ballot)
            }
            PeerMsg::CommitNotice {
                slot,
                ballot,
                seq,
                deps,
                reqs,
            } => self
                .handle_msg_commit_notice(peer, slot, ballot, seq, deps, reqs),
            PeerMsg::ExpPrepare { slot, ballot } => {
                self.handle_msg_exp_prepare(peer, slot, ballot)
            }
            PeerMsg::ExpPrepareReply {
                slot,
                ballot,
                voted_bal,
                voted_seq,
                voted_deps,
                voted_reqs,
            } => self.handle_msg_exp_prepare_reply(
                peer, slot, ballot, voted_bal, voted_seq, voted_deps,
                voted_reqs,
            ),
            PeerMsg::Heartbeat {
                ballot,
                commit_bars,
                exec_bars,
                snap_bar,
            } => self.heard_heartbeat(
                peer,
                ballot,
                commit_bars,
                exec_bars,
                snap_bar,
            ),
        }
    }
}
