//! `RS-Paxos` -- peer-peer messaging.

use std::cmp;
use std::collections::HashMap;

use super::*;
use crate::server::{ApiRequest, LogAction, ReplicaId};
use crate::utils::{Bitmap, RSCodeword, SummersetError};

// RSPaxosReplica peer-peer messages handling
impl RSPaxosReplica {
    /// Handler of Prepare message from leader.
    async fn handle_msg_prepare(
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
            self.check_leader(peer, ballot).await?;
            if !self.config.disable_hb_timer {
                self.heartbeater.kickoff_hear_timer(Some(peer))?;
            }

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= trigger_slot {
                self.insts.push(self.null_instance()?);
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
    #[allow(clippy::too_many_lines)]
    pub(super) fn handle_msg_prepare_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        trigger_slot: usize,
        endprep_slot: usize,
        ballot: Ballot,
        voted: Option<(Ballot, RSCodeword<ReqBatch>)>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received PrepareReply <- {} for slot {} / {} bal {} shards {:?}",
            peer,
            slot,
            endprep_slot,
            ballot,
            voted.as_ref().map(|(_, cw)| cw.avail_shards_map())
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
                self.insts.push(self.null_instance()?);
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
                    #[allow(clippy::comparison_chain)]
                    if bal > leader_bk.prepare_max_bal {
                        // is of ballot > current maximum, so discard the
                        // current codeword and take the replied codeword
                        leader_bk.prepare_max_bal = bal;
                        inst.reqs_cw = val;
                    } else if bal == leader_bk.prepare_max_bal {
                        // is of ballot == the one currently taken, so merge
                        // the replied codeword into the current one
                        inst.reqs_cw.absorb_other(val)?;
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
                let prepare_acks_cnt = trigger_leader_bk.prepare_acks.count();

                // if quorum size reached, enter Accept phase for all instances
                // at and after trigger_slot; for each entry, use the request
                // batch value with the highest ballot number in quorum
                if prepare_acks_cnt >= self.majority {
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
                        if inst.reqs_cw.avail_shards() >= self.majority {
                            // if quorum size >= majority and enough shards
                            // with the highest ballot in quorum are gathered
                            // to reconstruct the original data, use the
                            // reconstructed request batch
                            if inst.reqs_cw.avail_data_shards() < self.majority
                            {
                                // have enough shards but need reconstruction
                                inst.reqs_cw
                                    .reconstruct_data(Some(&self.rs_coder))?;
                            }
                        } else if prepare_acks_cnt
                            >= (self.population - self.config.fault_tolerance)
                        {
                            // else, if quorum size >= (N - f) and shards with
                            // the highest ballot are not enough to reconstruct
                            // the original data, can choose any value; we just
                            // fill this instance with a null request batch
                            inst.reqs_cw = RSCodeword::from_data(
                                ReqBatch::new(),
                                self.majority,
                                self.population - self.majority,
                            )?;
                        } else {
                            // not yet for this instance
                            continue;
                        }

                        // if parity shards not computed yet, compute them now
                        if inst.reqs_cw.avail_shards() < self.population {
                            inst.reqs_cw
                                .compute_parity(Some(&self.rs_coder))?;
                        }

                        inst.status = Status::Accepting;
                        pf_debug!(
                            "enter Accept phase for slot {} bal {}",
                            this_slot,
                            inst.bal
                        );

                        // record update to largest accepted ballot and its
                        // corresponding data
                        let subset_copy = inst.reqs_cw.subset_copy(
                            &Bitmap::from((self.population, vec![self.id])),
                            false,
                        )?;
                        inst.voted = (ballot, subset_copy.clone());
                        self.storage_hub.submit_action(
                            Self::make_log_action_id(
                                this_slot,
                                Status::Accepting,
                            ),
                            LogAction::Append {
                                entry: WalEntry::AcceptData {
                                    slot: this_slot,
                                    ballot,
                                    reqs_cw: subset_copy,
                                },
                                sync: self.config.logger_sync,
                            },
                        )?;
                        pf_trace!(
                            "submitted AcceptData log action for slot {} bal {}",
                            this_slot,
                            ballot
                        );

                        // send Accept messages to all peers
                        for peer in 0..self.population {
                            if peer == self.id {
                                continue;
                            }
                            self.transport_hub.send_msg(
                                PeerMsg::Accept {
                                    slot: this_slot,
                                    ballot,
                                    reqs_cw: inst.reqs_cw.subset_copy(
                                        &Bitmap::from((
                                            self.population,
                                            vec![peer],
                                        )),
                                        false,
                                    )?,
                                },
                                peer,
                            )?;
                        }
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
    async fn handle_msg_accept(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        reqs_cw: RSCodeword<ReqBatch>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received Accept <- {} for slot {} bal {} shards {:?}",
            peer,
            slot,
            ballot,
            reqs_cw.avail_shards_map()
        );

        // if ballot is not smaller than what I have made promises for:
        if ballot >= self.bal_max_seen {
            // update largest ballot seen and assumed leader
            self.check_leader(peer, ballot).await?;
            if !self.config.disable_hb_timer {
                self.heartbeater.kickoff_hear_timer(Some(peer))?;
            }

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= slot {
                self.insts.push(self.null_instance()?);
            }
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Accepting;
            inst.reqs_cw = reqs_cw;
            inst.replica_bk = Some(ReplicaBookkeeping {
                source: peer,
                trigger_slot: 0,
                endprep_slot: 0,
            });

            // record update to instance ballot & data
            inst.voted = (ballot, inst.reqs_cw.clone());
            self.storage_hub.submit_action(
                Self::make_log_action_id(slot, Status::Accepting),
                LogAction::Append {
                    entry: WalEntry::AcceptData {
                        slot,
                        ballot,
                        reqs_cw: inst.reqs_cw.clone(),
                    },
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

            // if quorum size reached AND enough number of shards are
            // remembered, mark this instance as committed; in RS-Paxos, this
            // means accept_acks.count() >= self.majority + fault_tolerance
            if leader_bk.accept_acks.count()
                >= self.majority + self.config.fault_tolerance
            {
                inst.status = Status::Committed;
                pf_debug!(
                    "committed instance at slot {} bal {}",
                    slot,
                    inst.bal
                );

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

    /// Handler of Reconstruct message from leader.
    fn handle_msg_reconstruct(
        &mut self,
        peer: ReplicaId,
        slots: Vec<usize>,
    ) -> Result<(), SummersetError> {
        pf_trace!("received Reconstruct <- {} for slots {:?}", peer, slots);
        let mut slots_data = HashMap::new();

        for slot in slots {
            if slot < self.start_slot {
                // TODO: this has one caveat: a new leader trying to do
                //       reconstruction reads might find that all other peers
                //       have snapshotted that slot. Proper InstallSnapshot-
                //       style messages will be needed to deal with this; but
                //       since this scenario is just too rare and does not
                //       affect evaluation at all, it will be implemented after
                //       a rework of the storage backend module
                continue;
            }

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= slot {
                self.insts.push(self.null_instance()?);
            }
            let inst = &mut self.insts[slot - self.start_slot];

            // ignore spurious duplications; also ignore if I have nothing to send back
            if inst.status < Status::Accepting
                || inst.reqs_cw.avail_shards() == 0
            {
                continue;
            }

            // send back my ballot for this slot and the available shards
            slots_data.insert(slot, (inst.bal, inst.reqs_cw.clone()));
        }

        if !slots_data.is_empty() {
            let num_slots = slots_data.len();
            self.transport_hub
                .send_msg(PeerMsg::ReconstructReply { slots_data }, peer)?;
            pf_trace!(
                "sent ReconstructReply -> {} for {} slots",
                peer,
                num_slots
            );
        }
        Ok(())
    }

    /// Handler of Reconstruct reply from replica.
    fn handle_msg_reconstruct_reply(
        &mut self,
        peer: ReplicaId,
        slots_data: HashMap<usize, (Ballot, RSCodeword<ReqBatch>)>,
    ) -> Result<(), SummersetError> {
        for (slot, (ballot, reqs_cw)) in slots_data {
            if slot < self.start_slot {
                continue; // ignore if slot index outdated
            }
            pf_trace!(
                "in ReconstructReply <- {} for slot {} bal {} shards {:?}",
                peer,
                slot,
                ballot,
                reqs_cw.avail_shards_map()
            );
            debug_assert!(slot < self.start_slot + self.insts.len());
            debug_assert!(
                self.insts[slot - self.start_slot].status >= Status::Committed
            );
            let inst = &mut self.insts[slot - self.start_slot];

            // if reply not outdated and ballot is up-to-date
            if inst.status < Status::Executed && ballot >= inst.bal {
                // absorb the shards from this replica
                inst.reqs_cw.absorb_other(reqs_cw)?;

                // if enough shards have been gathered, can push execution forward
                if slot == self.commit_bar {
                    while self.commit_bar < self.start_slot + self.insts.len() {
                        let inst =
                            &mut self.insts[self.commit_bar - self.start_slot];
                        if inst.status < Status::Committed
                            || inst.reqs_cw.avail_shards() < self.majority
                        {
                            break;
                        }

                        if inst.reqs_cw.avail_data_shards() < self.majority {
                            // have enough shards but need reconstruction
                            inst.reqs_cw
                                .reconstruct_data(Some(&self.rs_coder))?;
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
                                }
                            }
                            pf_trace!(
                                "submitted {} exec commands for slot {}",
                                reqs.len(),
                                self.commit_bar
                            );
                        }

                        self.commit_bar += 1;
                    }
                }
            }
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
            PeerMsg::Prepare {
                trigger_slot,
                ballot,
            } => self.handle_msg_prepare(peer, trigger_slot, ballot).await,
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
            PeerMsg::Accept {
                slot,
                ballot,
                reqs_cw,
            } => self.handle_msg_accept(peer, slot, ballot, reqs_cw).await,
            PeerMsg::AcceptReply { slot, ballot } => {
                self.handle_msg_accept_reply(peer, slot, ballot)
            }
            PeerMsg::Reconstruct { slots } => {
                self.handle_msg_reconstruct(peer, slots)
            }
            PeerMsg::ReconstructReply { slots_data } => {
                self.handle_msg_reconstruct_reply(peer, slots_data)
            }
            PeerMsg::Heartbeat {
                ballot,
                commit_bar,
                exec_bar,
                snap_bar,
            } => {
                self.heard_heartbeat(
                    peer, ballot, commit_bar, exec_bar, snap_bar,
                )
                .await
            }
        }
    }
}
