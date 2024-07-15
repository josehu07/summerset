//! MultiPaxos -- peer-peer messaging.

use std::cmp;

use super::*;

use crate::server::{LogAction, ReplicaId};
use crate::utils::SummersetError;

// MultiPaxosReplica peer-peer messages handling
impl MultiPaxosReplica {
    /// Handler of Prepare message from leader.
    fn handle_msg_prepare(
        &mut self,
        peer: ReplicaId,
        trigger_slot: usize,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if trigger_slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received Prepare <- {} trigger_slot {} bal {}",
            peer,
            trigger_slot,
            ballot
        );

        // if ballot is not smaller than what I have seen:
        if ballot >= self.bal_max_seen {
            // update largest ballot seen and assumed leader
            self.check_leader(peer, ballot)?;
            self.kickoff_hb_hear_timer()?;

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= trigger_slot {
                self.insts.push(self.null_instance());
            }

            // find the last non-null slot and use as endprep_slot; if none
            // found, use trigger_slot as a dummy entry
            let endprep_slot = cmp::max(
                self.start_slot
                    + self
                        .insts
                        .iter()
                        .rposition(|i| i.status > Status::Null)
                        .unwrap_or(0),
                trigger_slot,
            );

            // react to this Prepare for all slots >= trigger_slot
            for slot in trigger_slot..=endprep_slot {
                let inst = &mut self.insts[slot - self.start_slot];
                debug_assert!(inst.bal <= ballot);

                inst.bal = ballot;
                inst.status = Status::Preparing;
                inst.replica_bk = Some(ReplicaBookkeeping {
                    source: peer,
                    trigger_slot,
                    endprep_slot,
                });

                // record update to largest prepare ballot
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Preparing),
                    LogAction::Append {
                        entry: WalEntry::PrepareBal { slot, ballot },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted PrepareBal log action for slot {} bal {}",
                    slot,
                    ballot
                );
            }
        }

        Ok(())
    }

    /// Handler of Prepare reply from replica.
    pub(super) fn handle_msg_prepare_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        trigger_slot: usize,
        endprep_slot: usize,
        ballot: Ballot,
        voted: Option<(Ballot, ReqBatch)>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received PrepareReply <- {} for slot {} / {} bal {}",
            peer,
            slot,
            endprep_slot,
            ballot
        );

        // if ballot is what I'm currently waiting on for Prepare replies:
        if ballot == self.bal_prep_sent {
            // ignore spurious duplications and outdated replies
            if !self.is_leader() {
                return Ok(());
            }
            debug_assert!(slot >= trigger_slot && slot <= endprep_slot);
            debug_assert!(
                trigger_slot >= self.start_slot
                    && trigger_slot < self.start_slot + self.insts.len()
            );
            if self.insts[trigger_slot - self.start_slot]
                .leader_bk
                .is_none()
            {
                return Ok(());
            }

            // locate instance in memory, filling in null instance if needed
            // if slot is outside the tail of my current log, this means I did
            // not know at `become_leader()` that this slot existed on peers
            let my_endprep_slot = self.insts[trigger_slot - self.start_slot]
                .leader_bk
                .as_ref()
                .unwrap()
                .endprep_slot;
            while self.start_slot + self.insts.len() <= slot {
                let this_slot = self.start_slot + self.insts.len();
                self.insts.push(self.null_instance());
                let inst = &mut self.insts[this_slot - self.start_slot];

                // since this slot was not known at `become_leader()`, need to
                // fill necessary information here and make durable
                inst.external = true;
                inst.bal = self.bal_prep_sent;
                inst.status = Status::Preparing;
                inst.leader_bk = Some(LeaderBookkeeping {
                    trigger_slot,
                    endprep_slot: my_endprep_slot,
                    prepare_acks: Bitmap::new(self.population, false),
                    prepare_max_bal: 0,
                    accept_acks: Bitmap::new(self.population, false),
                });

                // record update to largest prepare ballot
                self.storage_hub.submit_action(
                    Self::make_log_action_id(this_slot, Status::Preparing),
                    LogAction::Append {
                        entry: WalEntry::PrepareBal {
                            slot: this_slot,
                            ballot: self.bal_prep_sent,
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(
                    "submitted PrepareBal log action for slot {} bal {}",
                    this_slot,
                    inst.bal
                );
            }

            {
                let inst = &mut self.insts[slot - self.start_slot];

                // ignore spurious duplications and outdated replies
                if (inst.status != Status::Preparing) || (ballot < inst.bal) {
                    return Ok(());
                }
                debug_assert_eq!(inst.bal, ballot);
                debug_assert!(self.bal_max_seen >= ballot);

                // bookkeep this Prepare reply
                if let Some((bal, val)) = voted {
                    debug_assert!(inst.leader_bk.is_some());
                    let leader_bk = inst.leader_bk.as_mut().unwrap();
                    if bal > leader_bk.prepare_max_bal {
                        leader_bk.prepare_max_bal = bal;
                        inst.reqs = val;
                    }
                }
            }

            // if all PrepareReplies up to endprep_slot have been received,
            // include the sender peer into the quorum (by updating the
            // prepare_acks field in the trigger_slot entry)
            if slot == endprep_slot {
                let trigger_inst =
                    &mut self.insts[trigger_slot - self.start_slot];
                debug_assert!(trigger_inst.leader_bk.is_some());
                let trigger_leader_bk =
                    trigger_inst.leader_bk.as_mut().unwrap();
                trigger_leader_bk.prepare_acks.set(peer, true)?;

                // if quorum size reached, enter Accept phase for all instances
                // at and after trigger_slot; for each entry, use the request
                // batch value with the highest ballot number in quorum
                if trigger_leader_bk.prepare_acks.count() >= self.quorum_cnt {
                    // update bal_prepared
                    debug_assert!(self.bal_prepared <= ballot);
                    self.bal_prepared = ballot;

                    for (this_slot, inst) in self
                        .insts
                        .iter_mut()
                        .enumerate()
                        .map(|(s, i)| (self.start_slot + s, i))
                        .skip(trigger_slot - self.start_slot)
                        .filter(|(_, i)| i.status == Status::Preparing)
                    {
                        inst.status = Status::Accepting;
                        pf_debug!(
                            "enter Accept phase for slot {} bal {}",
                            this_slot,
                            inst.bal
                        );

                        // record update to largest accepted ballot and its
                        // corresponding data
                        self.storage_hub.submit_action(
                            Self::make_log_action_id(
                                this_slot,
                                Status::Accepting,
                            ),
                            LogAction::Append {
                                entry: WalEntry::AcceptData {
                                    slot: this_slot,
                                    ballot,
                                    reqs: inst.reqs.clone(),
                                },
                                sync: self.config.logger_sync,
                            },
                        )?;
                        pf_trace!(
                            "submitted AcceptData log action for slot {} bal {}",
                            this_slot, ballot
                        );

                        // send Accept messages to all peers
                        self.transport_hub.bcast_msg(
                            PeerMsg::Accept {
                                slot: this_slot,
                                ballot,
                                reqs: inst.reqs.clone(),
                            },
                            None,
                        )?;
                        pf_trace!(
                            "broadcast Accept messages for slot {} bal {}",
                            this_slot,
                            ballot
                        );
                    }
                }
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
        pf_trace!(
            "received Accept <- {} for slot {} bal {}",
            peer,
            slot,
            ballot
        );

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
            inst.reqs.clone_from(&reqs);
            if let Some(replica_bk) = inst.replica_bk.as_mut() {
                replica_bk.source = peer;
            } else {
                inst.replica_bk = Some(ReplicaBookkeeping {
                    source: peer,
                    trigger_slot: 0,
                    endprep_slot: 0,
                });
            }

            // record update to instance ballot & data
            inst.voted = (ballot, reqs.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Accepting),
                LogAction::Append {
                    entry: WalEntry::AcceptData { slot, ballot, reqs },
                    sync: self.config.logger_sync,
                },
            )?;
            pf_trace!(
                "submitted AcceptData log action for slot {} bal {}",
                slot,
                ballot
            );
        }

        Ok(())
    }

    /// Handler of Accept reply from replica.
    pub(super) fn handle_msg_accept_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        reply_ts: Option<SystemTime>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received AcceptReply <- {} for slot {} bal {}",
            peer,
            slot,
            ballot
        );

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
                pf_debug!(
                    "committed instance at slot {} bal {}",
                    slot,
                    inst.bal
                );

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
                pf_trace!(
                    "submitted CommitSlot log action for slot {} bal {}",
                    slot,
                    inst.bal
                );
            }
        }

        Ok(())
    }

    /// Synthesized handler of receiving message from peer.
    pub(super) fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::Prepare {
                trigger_slot,
                ballot,
            } => self.handle_msg_prepare(peer, trigger_slot, ballot),
            PeerMsg::PrepareReply {
                slot,
                trigger_slot,
                endprep_slot,
                ballot,
                voted,
            } => self.handle_msg_prepare_reply(
                peer,
                slot,
                trigger_slot,
                endprep_slot,
                ballot,
                voted,
            ),
            PeerMsg::Accept { slot, ballot, reqs } => {
                self.handle_msg_accept(peer, slot, ballot, reqs)
            }
            PeerMsg::AcceptReply {
                slot,
                ballot,
                reply_ts,
            } => self.handle_msg_accept_reply(peer, slot, ballot, reply_ts),
            PeerMsg::Heartbeat {
                ballot,
                commit_bar,
                exec_bar,
                snap_bar,
            } => self
                .heard_heartbeat(peer, ballot, commit_bar, exec_bar, snap_bar),
        }
    }
}
