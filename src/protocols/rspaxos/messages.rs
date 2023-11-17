//! RS-Paxos -- peer-peer messaging.

use std::collections::HashMap;

use super::*;

use crate::utils::{SummersetError, Bitmap, RSCodeword};
use crate::server::{ReplicaId, ApiRequest, LogAction};

// RSPaxosReplica peer-peer messages handling
impl RSPaxosReplica {
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
                self.insts.push(self.null_instance()?);
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
        voted: Option<(Ballot, RSCodeword<ReqBatch>)>,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received PrepareReply <- {} for slot {} bal {} shards {:?}",
                           peer, slot, ballot,
                           voted.as_ref().map(|(_, cw)| cw.avail_shards_map()));

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
            //
            // NOTE: missing a "choose anything" case here and might add later:
            //       when |Q| >= majority but #shards of the highest ballot is
            //       not enough, we are free to choose any value
            if leader_bk.prepare_acks.count() >= self.majority
                && inst.reqs_cw.avail_shards() >= self.majority
            {
                if inst.reqs_cw.avail_data_shards() < self.majority {
                    // have enough shards but need reconstruction
                    inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }

                inst.status = Status::Accepting;
                pf_debug!(self.id; "enter Accept phase for slot {} bal {}",
                                   slot, inst.bal);

                // update bal_prepared
                debug_assert!(self.bal_prepared <= ballot);
                self.bal_prepared = ballot;

                // if parity shards not computed yet, compute them now
                if inst.reqs_cw.avail_shards() < self.population {
                    inst.reqs_cw.compute_parity(Some(&self.rs_coder))?;
                }

                // record update to largest accepted ballot and corresponding data
                let subset_copy = inst.reqs_cw.subset_copy(
                    &Bitmap::from(self.population, vec![self.id]),
                    false,
                )?;
                inst.voted = (ballot, subset_copy.clone());
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: WalEntry::AcceptData {
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
                                &Bitmap::from(self.population, vec![peer]),
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
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(self.id; "received Accept <- {} for slot {} bal {} shards {:?}",
                           peer, slot, ballot, reqs_cw.avail_shards_map());

        // if ballot is not smaller than what I have made promises for:
        if ballot >= self.bal_max_seen {
            // update largest ballot seen and assumed leader
            self.check_leader(peer, ballot)?;
            self.kickoff_hb_hear_timer()?;

            // locate instance in memory, filling in null instances if needed
            while self.start_slot + self.insts.len() <= slot {
                self.insts.push(self.null_instance()?);
            }
            let inst = &mut self.insts[slot - self.start_slot];
            debug_assert!(inst.bal <= ballot);

            inst.bal = ballot;
            inst.status = Status::Accepting;
            inst.reqs_cw = reqs_cw;
            inst.replica_bk = Some(ReplicaBookkeeping { source: peer });

            // record update to largest prepare ballot
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

            // if quorum size reached AND enough number of shards are
            // remembered, mark this instance as committed; in RS-Paxos, this
            // means accept_acks.count() >= self.majority + fault_tolerance
            if leader_bk.accept_acks.count()
                >= self.majority + self.config.fault_tolerance
            {
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
        pf_trace!(self.id; "received Reconstruct <- {} for slots {:?}", peer, slots);
        let mut slots_data = HashMap::new();

        for slot in slots {
            if slot < self.start_slot {
                // NOTE: this has one caveat: a new leader trying to do
                // reconstruction reads might find that all other peers have
                // snapshotted that slot. Proper InstallSnapshot-style messages
                // will be needed to deal with this; but since this scenario is
                // just too rare, it is not implemented yet
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
            pf_trace!(self.id; "sent ReconstructReply -> {} for {} slots",
                               peer, num_slots);
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
            pf_trace!(self.id; "in ReconstructReply <- {} for slot {} bal {} shards {:?}",
                               peer, slot, ballot, reqs_cw.avail_shards_map());
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
            PeerMsg::Accept {
                slot,
                ballot,
                reqs_cw,
            } => self.handle_msg_accept(peer, slot, ballot, reqs_cw),
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
            } => self
                .heard_heartbeat(peer, ballot, commit_bar, exec_bar, snap_bar),
        }
    }
}
