//! `Crossword` -- peer-peer messaging.

use std::cmp;
use std::collections::HashMap;

use super::*;
use crate::server::{LogAction, ReplicaId};
use crate::utils::{Bitmap, RSCodeword, SummersetError};

// CrosswordReplica peer-peer messages handling
impl CrosswordReplica {
    /// Computes the number of distinct shards that the acknowledged shard sets
    /// `acks` still cover after crossing out any `fault_tolerance` of them.
    pub(super) fn coverage_under_faults(
        acks: &HashMap<ReplicaId, Bitmap>,
        fault_tolerance: u8,
    ) -> u8 {
        if acks.len() <= fault_tolerance as usize {
            return 0;
        }

        // enumerate all subsets of acks excluding fault number of replicas
        #[allow(clippy::cast_possible_truncation)]
        let cnt = (acks.len() - fault_tolerance as usize) as u32;
        let shards: Vec<&Bitmap> = acks.values().collect();
        let mut coverage = shards[0].clone();
        let mut min_coverage = u8::MAX;
        for n in (0..1usize << shards.len()).filter(|n| n.count_ones() == cnt) {
            coverage.clear();
            for (_, acked) in shards
                .iter()
                .enumerate()
                .filter(|&(i, _)| (n >> i) & 1 == 1)
            {
                coverage
                    .union(acked)
                    .expect("shard sets should be of same size");
            }
            min_coverage = cmp::min(min_coverage, coverage.count());
        }
        min_coverage
    }

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
                if inst.bal == ballot
                    && ((inst.status > Status::Preparing && inst.status < Status::Committed)
                        || inst.prep_inflight > 0)
                {
                    // duplicate (re-sent) Prepare of what I've moved past or am already
                    // logging; will reply when that in-flight PrepareBal becomes durable
                    continue;
                }

                // committed instances keep their status, but still reply with
                // their value (once the promise is durable)
                inst.bal = ballot;
                if inst.status < Status::Committed {
                    inst.status = Status::Preparing;
                }
                inst.replica_bk = Some(ReplicaBookkeeping {
                    source: peer,
                    trigger_slot,
                    endprep_slot,
                });

                // record update to largest prepare ballot
                inst.prep_inflight += 1;
                self.storage_hub.submit_action(
                    LogCtx::PrepareBal { slot, ballot },
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
        voted: Option<AcceptedValue>,
    ) -> Result<(), SummersetError> {
        if trigger_slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        pf_trace!(
            "received PrepareReply <- {} for slot {} / {} bal {} shards {:?}",
            peer,
            slot,
            endprep_slot,
            ballot,
            voted.as_ref().map(|value| value.reqs_cw.avail_shards_map())
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
                    prepare_values: HashMap::new(),
                    accept_acks: HashMap::new(),
                });

                // record update to largest prepare ballot
                inst.prep_inflight += 1;
                self.storage_hub.submit_action(
                    LogCtx::PrepareBal {
                        slot: this_slot,
                        ballot: self.bal_prep_sent,
                    },
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

                // bookkeep this Prepare reply, unless spuriously duplicated or
                // outdated, or the instance has moved on (e.g., committed out
                // of order), in which case the reply still counts the peer
                if inst.status == Status::Preparing
                    && ballot == inst.bal
                    && let Some(value) = voted
                {
                    debug_assert!(inst.leader_bk.is_some());
                    let leader_bk = inst.leader_bk.as_mut().unwrap();
                    Self::record_prepared_value(&mut leader_bk.prepare_values, value)?;
                }
            }

            // if all PrepareReplies up to endprep_slot have been received,
            // include the sender peer into the quorum (by updating the
            // prepare_acks field in the trigger_slot entry)
            if slot == endprep_slot {
                let trigger_inst = &mut self.insts[trigger_slot - self.start_slot];
                debug_assert!(trigger_inst.leader_bk.is_some());
                let trigger_leader_bk = trigger_inst.leader_bk.as_mut().unwrap();
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
                        let candidate = inst
                            .leader_bk
                            .as_ref()
                            .unwrap()
                            .prepare_values
                            .values()
                            .max_by_key(|value| value.ballot);
                        if let Some(candidate) = candidate {
                            inst.value_id = Some(candidate.value_id);
                            inst.reqs_cw = candidate.reqs_cw.clone();
                        } else {
                            inst.value_id = None;
                            inst.reqs_cw = RSCodeword::from_null(
                                self.rs_data_shards,
                                self.rs_total_shards - self.rs_data_shards,
                            )?;
                        }

                        if inst.reqs_cw.avail_shards() >= self.rs_data_shards {
                            // if quorum size >= majority and enough shards
                            // of the highest-ballot value are gathered across
                            // all ballots, reconstruct the original batch
                            if inst.reqs_cw.avail_data_shards() < self.rs_data_shards {
                                // have enough shards but need reconstruction
                                inst.reqs_cw.reconstruct_data(Some(&self.rs_coder))?;
                            }
                            Self::verify_value_id(
                                inst.value_id
                                    .expect("non-null Prepare value should have an identity"),
                                &mut inst.reqs_cw,
                                self.rs_data_shards,
                                self.rs_total_shards,
                            )?;
                        } else if prepare_acks_cnt
                            >= (self.population - self.config.fault_tolerance)
                        {
                            // else, if quorum size >= (N - f) and shards with
                            // the highest ballot are not enough to reconstruct
                            // the original data, can choose any value; we just
                            // fill this instance with a null request batch
                            let reqs = ReqBatch::new();
                            inst.value_id = Some(Self::value_id(
                                &reqs,
                                self.rs_data_shards,
                                self.rs_total_shards,
                            )?);
                            inst.reqs_cw = RSCodeword::from_data(
                                reqs,
                                self.rs_data_shards,
                                self.rs_total_shards - self.rs_data_shards,
                            )?;
                        } else {
                            // not yet for this instance
                            continue;
                        }

                        // if parity shards not computed yet, compute them now
                        if inst.reqs_cw.avail_shards() < inst.reqs_cw.num_shards() {
                            inst.reqs_cw.compute_parity(Some(&self.rs_coder))?;
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
                            &self.linreg_model,
                            self.config.b_to_d_threshold,
                            &self.qdisc_info,
                            self.heartbeater.peer_alive(),
                        );
                        pf_debug!(
                            "enter Accept phase for slot {} bal {} asgmt {}",
                            this_slot,
                            inst.bal,
                            Self::assignment_to_string(assignment)
                        );

                        // record update to largest accepted ballot and corresponding data
                        let subset_copy = inst
                            .reqs_cw
                            .subset_copy(&assignment[self.id as usize], false)?;
                        let value_id = inst
                            .value_id
                            .expect("Accepting instance should have a value identity");
                        inst.assignment.clone_from(assignment);
                        Self::record_acceptance(
                            &mut inst.voted,
                            AcceptedValue {
                                ballot,
                                value_id,
                                reqs_cw: subset_copy.clone(),
                            },
                        )?;
                        self.storage_hub.submit_action(
                            LogCtx::AcceptData {
                                slot: this_slot,
                                ballot,
                                value_id,
                                shards: subset_copy.avail_shards_map(),
                            },
                            LogAction::Append {
                                entry: WalEntry::AcceptData {
                                    slot: this_slot,
                                    ballot,
                                    value_id,
                                    reqs_cw: subset_copy,
                                    assignment: assignment.clone(),
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
                        let now_us = self.startup_time.elapsed().as_micros();
                        for peer in 0..self.population {
                            if peer == self.id {
                                continue;
                            }
                            self.transport_hub.send_msg(
                                PeerMsg::Accept {
                                    slot: this_slot,
                                    ballot,
                                    value_id,
                                    reqs_cw: inst
                                        .reqs_cw
                                        .subset_copy(&assignment[peer as usize], false)?,
                                    assignment: assignment.clone(),
                                },
                                peer,
                            )?;
                            if self.heartbeater.peer_alive().get(peer)? {
                                self.pending_accepts
                                    .get_mut(&peer)
                                    .unwrap()
                                    .push_back((now_us, this_slot));
                            }
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
        value_id: ValueId,
        reqs_cw: RSCodeword<ReqBatch>,
        assignment: Vec<Bitmap>,
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

            let recv_size = reqs_cw.get_size();
            let wal_cw = reqs_cw.clone();
            Self::record_acceptance(
                &mut inst.voted,
                AcceptedValue {
                    ballot,
                    value_id,
                    reqs_cw,
                },
            )?;
            // committed instances keep their status and data, as the value accepted
            // at a higher ballot is the same one
            inst.bal = ballot;
            if inst.status < Status::Committed {
                inst.status = Status::Accepting;
                inst.value_id = Some(value_id);
                inst.reqs_cw = inst.voted.as_ref().unwrap().reqs_cw.clone();
                inst.assignment = assignment;
            } else if inst.status < Status::Executed && inst.value_id == Some(value_id) {
                inst.reqs_cw.absorb_other(wal_cw.clone())?;
            }
            if let Some(replica_bk) = inst.replica_bk.as_mut() {
                replica_bk.source = peer;
            } else {
                inst.replica_bk = Some(ReplicaBookkeeping {
                    source: peer,
                    trigger_slot: 0,
                    endprep_slot: 0,
                });
            }

            // [for perf breakdown only]
            if self.config.record_breakdown && self.config.record_size_recv {
                (*self
                    .bw_accumulators
                    .get_mut(&peer)
                    .expect("peer should exist in experiments")) += recv_size;
            }

            // record update to instance ballot & data
            self.storage_hub.submit_action(
                LogCtx::AcceptData {
                    slot,
                    ballot,
                    value_id,
                    shards: wal_cw.avail_shards_map(),
                },
                LogAction::Append {
                    entry: WalEntry::AcceptData {
                        slot,
                        ballot,
                        value_id,
                        reqs_cw: wal_cw,
                        assignment: inst.assignment.clone(),
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

    /// Re-sends Accepts that `peer` seems to have missed (e.g., when it was
    /// briefly down or they got lost): the Accepting slots below `slot` that
    /// it has not acked yet. If `adjacent`, as upon its reply to `slot`, only
    /// those right before `slot` count, since its replies come in order.
    fn resend_missed_accepts(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        adjacent: bool,
    ) -> Result<(), SummersetError> {
        for s in (self.commit_bar..slot).rev() {
            let inst = &self.insts[s - self.start_slot];
            if inst.status != Status::Accepting {
                continue;
            }
            let Some(leader_bk) = inst.leader_bk.as_ref() else {
                continue;
            };
            if leader_bk
                .accept_acks
                .get(&peer)
                .is_some_and(|acked| acked.is_superset(&inst.assignment[peer as usize]))
            {
                if adjacent {
                    break;
                }
                continue;
            }
            if inst.bal == self.bal_prepared
                && let Some(value_id) = inst.value_id
            {
                self.transport_hub.send_msg(
                    PeerMsg::Accept {
                        slot: s,
                        ballot: inst.bal,
                        value_id,
                        reqs_cw: inst
                            .reqs_cw
                            .subset_copy(&inst.assignment[peer as usize], false)?,
                        assignment: inst.assignment.clone(),
                    },
                    peer,
                )?;
                pf_debug!("re-sent Accept -> {} for slot {} bal {}", peer, s, inst.bal);
            }
        }

        Ok(())
    }

    /// Re-sends Accepts that peers have left unacked for a whole hearing
    /// timeout, as no later replies may come to re-trigger them (e.g., with a
    /// closed-loop client).
    pub(super) fn retrigger_accepts(&mut self) -> Result<(), SummersetError> {
        let (since, bar) = self.accept_retry;
        if since.elapsed() < Duration::from_millis(self.config.hb_hear_timeout_min) {
            return Ok(());
        }

        let end = self.start_slot + self.insts.len();
        self.accept_retry = (Instant::now(), end);
        if self.is_leader() {
            let me = self.id;
            for peer in (0..self.population).filter(|&p| p != me) {
                self.resend_missed_accepts(peer, bar.min(end), false)?;
            }
        }

        Ok(())
    }

    /// Handler of Accept reply from replica.
    pub(super) fn handle_msg_accept_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        ballot: Ballot,
        value_id: ValueId,
        shards: &Bitmap,
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
            if self.is_leader() && peer != self.id {
                self.resend_missed_accepts(peer, slot, true)?;
            }
            let is_leader = self.is_leader();
            if is_leader && peer != self.id {
                // payload size that the corresponding Accept carried
                let size = shards.count() as usize
                    * self.insts[slot - self.start_slot].reqs_cw.shard_len();
                self.record_accept_rtt(peer, self.startup_time.elapsed().as_micros(), slot, size);
            }
            let inst = &mut self.insts[slot - self.start_slot];

            // ignore spurious duplications and outdated replies, including
            // those about a different value
            if !is_leader
                || (inst.status != Status::Accepting)
                || (ballot < inst.bal)
                || inst.value_id != Some(value_id)
            {
                return Ok(());
            }
            debug_assert_eq!(inst.bal, ballot);
            debug_assert!(self.bal_max_seen >= ballot);
            debug_assert!(inst.leader_bk.is_some());
            let leader_bk = inst.leader_bk.as_mut().unwrap();

            // bookkeep the shards this Accept reply confirms durable, which
            // may differ from what the peer is assigned now (e.g., when
            // replying to an older Accept before a reassignment)
            let acked = leader_bk
                .accept_acks
                .entry(peer)
                .or_insert_with(|| Bitmap::new(shards.size(), false));
            if acked.is_superset(shards) {
                return Ok(());
            }
            acked.union(shards)?;

            // if quorum size reached AND enough number of shards are
            // remembered, mark this instance as committed
            if u8::try_from(leader_bk.accept_acks.len())? >= self.majority
                && Self::coverage_under_faults(&leader_bk.accept_acks, self.config.fault_tolerance)
                    >= inst.reqs_cw.num_data_shards()
            {
                inst.status = Status::Committed;
                pf_debug!("committed instance at slot {} bal {}", slot, inst.bal);

                // [for perf breakdown only]
                if let Some(sw) = self.bd_stopwatch.as_mut() {
                    let _ = sw.record_now(slot, 3, reply_ts);
                    let _ = sw.record_now(slot, 4, None);
                }

                // record commit event
                self.storage_hub.submit_action(
                    LogCtx::CommitSlot { slot },
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

    /// Handler of Reconstruct message from leader or gossiping peer.
    fn handle_msg_reconstruct(
        &mut self,
        peer: ReplicaId,
        slots_excl: Vec<(usize, Bitmap)>,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "received Reconstruct <- {} for {} slots",
            peer,
            slots_excl.len()
        );
        let mut slots_data = HashMap::new();

        for (slot, mut subset) in slots_excl {
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
            if inst.status < Status::Accepting {
                continue;
            }
            subset.flip(); // exclude unwanted shards the sender already has
            let reply_cw = inst.reqs_cw.subset_copy(&subset, false)?;
            if reply_cw.avail_shards() == 0 {
                continue;
            }

            let Some(value_id) = inst.value_id else {
                continue;
            };
            slots_data.insert(
                slot,
                AcceptedValue {
                    ballot: inst.bal,
                    value_id,
                    reqs_cw: reply_cw,
                },
            );
        }

        if !slots_data.is_empty() {
            let num_slots = slots_data.len();
            self.transport_hub
                .send_msg(PeerMsg::ReconstructReply { slots_data }, peer)?;
            pf_trace!("sent ReconstructReply -> {} for {} slots", peer, num_slots);
        }
        Ok(())
    }

    /// Handler of Reconstruct reply from replica.
    fn handle_msg_reconstruct_reply(
        &mut self,
        peer: ReplicaId,
        slots_data: HashMap<usize, AcceptedValue>,
    ) -> Result<(), SummersetError> {
        for (
            slot,
            AcceptedValue {
                ballot,
                value_id,
                reqs_cw,
            },
        ) in slots_data
        {
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
            debug_assert!(self.insts[slot - self.start_slot].status >= Status::Committed);
            let inst = &mut self.insts[slot - self.start_slot];

            // [for perf breakdown only]
            if self.config.record_breakdown && self.config.record_size_recv {
                (*self
                    .bw_accumulators
                    .get_mut(&peer)
                    .expect("peer should exist in experiments")) += reqs_cw.get_size();
            }

            // same-value shards remain useful regardless of accepted ballot
            if inst.status < Status::Executed && inst.value_id == Some(value_id) {
                inst.reqs_cw.absorb_other(reqs_cw)?;
            }
        }

        // if enough shards have been gathered, can push execution forward
        self.advance_commit_bar()
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
            } => {
                self.handle_msg_prepare_reply(peer, slot, trigger_slot, endprep_slot, ballot, voted)
            }
            PeerMsg::Accept {
                slot,
                ballot,
                value_id,
                reqs_cw,
                assignment,
            } => {
                self.handle_msg_accept(peer, slot, ballot, value_id, reqs_cw, assignment)
                    .await
            }
            PeerMsg::AcceptReply {
                slot,
                ballot,
                value_id,
                shards,
                reply_ts,
            } => self.handle_msg_accept_reply(peer, slot, ballot, value_id, &shards, reply_ts),
            PeerMsg::Reconstruct { slots_excl } => self.handle_msg_reconstruct(peer, slots_excl),
            PeerMsg::ReconstructReply { slots_data } => {
                self.handle_msg_reconstruct_reply(peer, slots_data)
            }
            PeerMsg::Heartbeat {
                id: hb_id,
                ballot,
                commit_bar,
                exec_bar,
                snap_bar,
            } => {
                self.heard_heartbeat(peer, hb_id, ballot, commit_bar, exec_bar, snap_bar)
                    .await
            }
        }
    }
}
