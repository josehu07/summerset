//! Crossword -- peer-peer messaging.

use std::collections::HashMap;

use super::*;

use crate::utils::{SummersetError, Bitmap, RSCodeword};
use crate::server::{ReplicaId, ApiRequest, LogAction};

// CrosswordReplica peer-peer messages handling
impl CrosswordReplica {
    // Compute the subset coverage of acknowledge pattern `acks` when
    // considering at most `fault_tolerance` failures.
    #[inline]
    fn coverage_under_faults(
        rs_total_shards: u8,
        population: u8,
        acks: &HashMap<ReplicaId, Bitmap>,
        fault_tolerance: u8,
        assignment_balanced: bool,
    ) -> u8 {
        if acks.len() <= fault_tolerance as usize {
            return 0;
        }

        // if forcing balanced assignment, can compute this using a rather
        // simple calculation
        if assignment_balanced {
            let spr = acks.values().next().unwrap().count();
            let dj_spr = rs_total_shards / population;
            return (acks.len() as u8 - fault_tolerance - 1) * dj_spr + spr;
        }

        // enumerate all subsets of acks excluding fault number of replicas
        let cnt = (acks.len() - fault_tolerance as usize) as u32;
        let servers: Vec<ReplicaId> = acks.keys().cloned().collect();
        let mut min_coverage = rs_total_shards;
        for n in (0..2usize.pow(servers.len() as u32))
            .filter(|n| n.count_ones() == cnt)
        {
            let mut coverage = Bitmap::new(rs_total_shards, false);
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
                && inst.reqs_cw.avail_shards() >= inst.reqs_cw.num_data_shards()
            {
                if inst.reqs_cw.avail_data_shards()
                    < inst.reqs_cw.num_data_shards()
                {
                    // have enough shards but need reconstruction
                    inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                }

                inst.status = Status::Accepting;
                let assignment = Self::pick_assignment_policy(
                    self.assignment_adaptive,
                    self.assignment_balanced,
                    &self.init_assignment,
                    &self.brr_assignments,
                    self.rs_data_shards,
                    self.majority,
                    self.config.fault_tolerance,
                    inst.reqs_cw.data_len(),
                    self.config.vsize_lower_bound,
                    self.config.vsize_upper_bound,
                    &self.linreg_model,
                    &self.peer_alive,
                );
                pf_debug!(self.id; "enter Accept phase for slot {} bal {} asgmt {}",
                                   slot, inst.bal, Self::assignment_to_string(assignment));

                // update bal_prepared
                debug_assert!(self.bal_prepared <= ballot);
                self.bal_prepared = ballot;

                // if parity shards not computed yet, compute them now
                if inst.reqs_cw.avail_shards() < inst.reqs_cw.num_shards() {
                    inst.reqs_cw.compute_parity(Some(&self.rs_coder))?;
                }

                // record update to largest accepted ballot and corresponding data
                let subset_copy = inst
                    .reqs_cw
                    .subset_copy(&assignment[self.id as usize], false)?;
                inst.assignment = assignment.clone();
                inst.voted = (ballot, subset_copy.clone());
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: WalEntry::AcceptData {
                            slot,
                            ballot,
                            reqs_cw: subset_copy,
                            assignment: assignment.clone(),
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(self.id; "submitted AcceptData log action for slot {} bal {}",
                                   slot, ballot);

                // send Accept messages to all peers
                let now_us = self.startup_time.elapsed().as_micros();
                for peer in 0..self.population {
                    if peer == self.id {
                        continue;
                    }
                    self.transport_hub.send_msg(
                        PeerMsg::Accept {
                            slot,
                            ballot,
                            reqs_cw: inst.reqs_cw.subset_copy(
                                &assignment[peer as usize],
                                false,
                            )?,
                            assignment: assignment.clone(),
                        },
                        peer,
                    )?;
                    if self.peer_alive.get(peer)? {
                        self.pending_accepts
                            .get_mut(&peer)
                            .unwrap()
                            .push_back((now_us, slot));
                    }
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
        assignment: Vec<Bitmap>,
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
            inst.assignment = assignment;
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
                        assignment: inst.assignment.clone(),
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
        size: usize,
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
            if is_leader && peer != self.id {
                self.record_accept_rtt(
                    peer,
                    self.startup_time.elapsed().as_micros(),
                    slot,
                    size,
                );
            }
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
            if leader_bk.accept_acks.contains_key(&peer) {
                return Ok(());
            }

            // bookkeep this Accept reply
            leader_bk
                .accept_acks
                .insert(peer, inst.assignment[peer as usize].clone());

            // if quorum size reached AND enough number of shards are
            // remembered, mark this instance as committed
            if leader_bk.accept_acks.len() as u8 >= self.majority
                && Self::coverage_under_faults(
                    self.rs_total_shards,
                    self.population,
                    &leader_bk.accept_acks,
                    self.config.fault_tolerance,
                    self.assignment_balanced,
                ) >= inst.reqs_cw.num_data_shards()
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
            self.insts.push(self.null_instance()?);
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

    /// Handler of Reconstruct message from leader or gossiping peer.
    fn handle_msg_reconstruct(
        &mut self,
        peer: ReplicaId,
        slots_excl: Vec<(usize, Bitmap)>,
    ) -> Result<(), SummersetError> {
        pf_trace!(self.id; "received Reconstruct <- {} for {} slots",
                           peer, slots_excl.len());
        let mut slots_data = HashMap::new();

        for (slot, mut subset) in slots_excl {
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
            if inst.status < Status::Accepting {
                continue;
            }
            subset.flip(); // exclude unwanted shards the sender already has
            let reply_cw = inst.reqs_cw.subset_copy(&subset, false)?;
            if reply_cw.avail_shards() == 0 {
                continue;
            }

            // send back my ballot for this slot and the available shards
            slots_data.insert(slot, (inst.bal, reply_cw));
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
                            || inst.reqs_cw.avail_shards()
                                < inst.reqs_cw.num_data_shards()
                        {
                            break;
                        }

                        if inst.reqs_cw.avail_data_shards()
                            < inst.reqs_cw.num_data_shards()
                        {
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
                assignment,
            } => {
                self.handle_msg_accept(peer, slot, ballot, reqs_cw, assignment)
            }
            PeerMsg::AcceptReply { slot, ballot, size } => {
                self.handle_msg_accept_reply(peer, slot, ballot, size)
            }
            PeerMsg::Commit { slot } => self.handle_msg_commit(peer, slot),
            PeerMsg::Reconstruct { slots_excl } => {
                self.handle_msg_reconstruct(peer, slots_excl)
            }
            PeerMsg::ReconstructReply { slots_data } => {
                self.handle_msg_reconstruct_reply(peer, slots_data)
            }
            PeerMsg::Heartbeat {
                id: hb_id,
                ballot,
                exec_bar,
                snap_bar,
            } => self.heard_heartbeat(peer, hb_id, ballot, exec_bar, snap_bar),
        }
    }
}
