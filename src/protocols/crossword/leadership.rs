//! Crossword -- leader election.

use std::collections::{HashMap, VecDeque};

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::manager::CtrlMsg;
use crate::server::{ReplicaId, LogAction};

use rand::prelude::*;

use tokio::time::Duration;

// CrosswordReplica leadership related logic
impl CrosswordReplica {
    /// If a larger ballot number is seen, consider that peer as new leader.
    pub(super) fn check_leader(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if ballot > self.bal_max_seen {
            self.bal_max_seen = ballot;

            // clear my leader status if I was one
            if self.is_leader() {
                self.control_hub
                    .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
                pf_info!(self.id; "no longer a leader...");
            }

            // reset heartbeat timeout timer to prevent me from trying to
            // compete with a new leader when it is doing reconstruction
            self.kickoff_hb_hear_timer()?;

            // set this peer to be the believed leader
            self.leader = Some(peer);
        }

        Ok(())
    }

    /// Becomes a leader, sends self-initiated Prepare messages to followers
    /// for all in-progress instances, and starts broadcasting heartbeats.
    pub(super) fn become_a_leader(&mut self) -> Result<(), SummersetError> {
        if self.is_leader() {
            return Ok(());
        }

        self.leader = Some(self.id); // this starts broadcasting heartbeats
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;
        pf_info!(self.id; "becoming a leader...");

        // clear peers' heartbeat reply counters, and broadcast a heartbeat now
        for cnts in self.hb_reply_cnts.values_mut() {
            *cnts = (1, 0, 0);
        }
        self.bcast_heartbeats()?;

        // re-initialize peer_exec_bar information
        for slot in self.peer_exec_bar.values_mut() {
            *slot = 0;
        }

        // make a greater ballot number and invalidate all in-progress instances
        self.bal_prepared = 0;
        self.bal_prep_sent = self.make_greater_ballot(self.bal_max_seen);
        self.bal_max_seen = self.bal_prep_sent;

        // clear pending perf monitoring timestamps
        for pending in self.pending_accepts.values_mut() {
            pending.clear();
        }
        for pending in self.pending_heartbeats.values_mut() {
            pending.clear();
        }
        let now_us = self.startup_time.elapsed().as_micros();

        // find the first and last slot index for which to redo Prepare phase
        let trigger_slot = self.start_slot
            + self
                .insts
                .iter()
                .position(|i| i.status < Status::Committed)
                .unwrap_or(self.insts.len());
        let endprep_slot = self.start_slot
            + self
                .insts
                .iter()
                .rposition(|i| i.status < Status::Committed)
                .unwrap_or(self.insts.len());
        debug_assert!(trigger_slot <= endprep_slot);
        if trigger_slot == self.start_slot + self.insts.len() {
            // append a null instance to act as the trigger_slot
            self.insts.push(self.null_instance()?);
        }
        pf_debug!(self.id; "enter Prepare phase trigger_slot {} bal {}",
                           trigger_slot, self.bal_prep_sent);

        // redo Prepare phase for all in-progress instances
        let mut recon_slots: Vec<(usize, Bitmap)> = vec![];
        for (slot, inst) in self
            .insts
            .iter_mut()
            .enumerate()
            .map(|(s, i)| (self.start_slot + s, i))
            .skip(self.exec_bar - self.start_slot)
        {
            if inst.status == Status::Executed {
                continue;
            }
            inst.external = true; // so replies to clients can be triggered

            if inst.status < Status::Committed {
                inst.bal = self.bal_prep_sent;
                inst.status = Status::Preparing;
                inst.leader_bk = Some(LeaderBookkeeping {
                    trigger_slot,
                    endprep_slot,
                    prepare_acks: Bitmap::new(self.population, false),
                    prepare_max_bal: 0,
                    accept_acks: HashMap::new(),
                });

                // record update to largest prepare ballot
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Preparing),
                    LogAction::Append {
                        entry: WalEntry::PrepareBal {
                            slot,
                            ballot: self.bal_prep_sent,
                        },
                        sync: self.config.logger_sync,
                    },
                )?;
                pf_trace!(self.id; "submitted PrepareBal log action for slot {} bal {}",
                                   slot, inst.bal);
            }

            // do reconstruction reads for all committed instances that do not
            // hold enough available shards for reconstruction. It would be too
            // complicated and slow to do the "data shards only" optimization
            // during fail-over, so just do this conservatively here
            if inst.status == Status::Committed
                && inst.reqs_cw.avail_shards() < inst.reqs_cw.num_data_shards()
            {
                recon_slots.push((slot, inst.reqs_cw.avail_shards_map()));
            }
        }

        // send Prepare message to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Prepare {
                trigger_slot,
                ballot: self.bal_prep_sent,
            },
            None,
        )?;
        pf_trace!(self.id; "broadcast Prepare messages trigger_slot {} bal {}",
                           trigger_slot, self.bal_prep_sent);

        // send reconstruction read messages in chunks
        for chunk in recon_slots.chunks(self.config.msg_chunk_size) {
            let slots = chunk.to_vec();
            let num_slots = slots.len();
            // pf_warn!(self.id; "recons {:?}", slots);
            self.transport_hub
                .bcast_msg(PeerMsg::Reconstruct { slots_excl: slots }, None)?;
            pf_trace!(self.id; "broadcast Reconstruct messages for {} slots", num_slots);

            // inject a heartbeat after every chunk to keep peers happy
            self.transport_hub.bcast_msg(
                PeerMsg::Heartbeat {
                    id: self.next_hb_id,
                    ballot: self.bal_max_seen,
                    commit_bar: self.commit_bar,
                    exec_bar: self.exec_bar,
                    snap_bar: self.snap_bar,
                },
                None,
            )?;
            for (&peer, pending) in self.pending_heartbeats.iter_mut() {
                if self.peer_alive.get(peer)? {
                    pending.push_back((now_us, self.next_hb_id));
                }
            }
            self.next_hb_id += 1;
        }

        self.update_qdisc_info()?;
        Ok(())
    }

    /// Broadcasts heartbeats to all replicas.
    pub(super) fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        let now_us = self.startup_time.elapsed().as_micros();
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                id: self.next_hb_id,
                ballot: self.bal_max_seen,
                commit_bar: self.commit_bar,
                exec_bar: self.exec_bar,
                snap_bar: self.snap_bar,
            },
            None,
        )?;
        for (&peer, pending) in self.pending_heartbeats.iter_mut() {
            if self.peer_alive.get(peer)? {
                pending.push_back((now_us, self.next_hb_id));
            }
        }

        // update max heartbeat reply counters and their repetitions seen
        let mut peer_death = false;
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
                    * 3;
                if cnts.2 > repeat_threshold {
                    // did not receive hb reply from this peer for too many
                    // past hbs sent from me; this peer is probably dead
                    if self.peer_alive.get(peer)? {
                        self.peer_alive.set(peer, false)?;
                        pf_info!(self.id; "peer_alive updated: {:?}", self.peer_alive);
                        peer_death = true;
                    }
                    cnts.2 = 0;
                }
            }
        }

        // I also heard this heartbeat from myself
        self.heard_heartbeat(
            self.id,
            self.next_hb_id,
            self.bal_max_seen,
            self.commit_bar,
            self.exec_bar,
            self.snap_bar,
        )?;
        self.next_hb_id += 1;

        // if we need to do soft fallback to a config with smaller fast-path
        // quorum size, redo Accept phase for certain slots for performance
        if peer_death {
            self.fallback_redo_accepts()?;
        }

        // pf_trace!(self.id; "broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Chooses a random hb_hear_timeout from the min-max range and kicks off
    /// the hb_hear_timer.
    pub(super) fn kickoff_hb_hear_timer(
        &mut self,
    ) -> Result<(), SummersetError> {
        self.hb_hear_timer.cancel()?;

        if !self.config.disable_hb_timer {
            let timeout_ms = thread_rng().gen_range(
                self.config.hb_hear_timeout_min
                    ..=self.config.hb_hear_timeout_max,
            );
            // pf_trace!(self.id; "kickoff hb_hear_timer @ {} ms", timeout_ms);
            self.hb_hear_timer
                .kickoff(Duration::from_millis(timeout_ms))?;
        }

        Ok(())
    }

    /// Heard a heartbeat from some other replica. If the heartbeat carries a
    /// high enough ballot number, refreshes my hearing timer and clears my
    /// leader status if I currently think I'm a leader.
    pub(super) fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        hb_id: HeartbeatId,
        ballot: Ballot,
        commit_bar: usize,
        exec_bar: usize,
        snap_bar: usize,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
            if self.is_leader() {
                self.record_heartbeat_rtt(
                    peer,
                    self.startup_time.elapsed().as_micros(),
                    hb_id,
                );
            }

            self.hb_reply_cnts.get_mut(&peer).unwrap().0 += 1;
            if !self.peer_alive.get(peer)? {
                self.peer_alive.set(peer, true)?;
                pf_info!(self.id; "peer_alive updated: {:?}", self.peer_alive);
            }

            // if the peer has made a higher ballot number, consider it as
            // a new leader
            self.check_leader(peer, ballot)?;

            // reply back with a Heartbeat message
            if self.leader == Some(peer) {
                self.transport_hub.send_msg(
                    PeerMsg::Heartbeat {
                        id: hb_id,
                        ballot: self.bal_max_seen,
                        commit_bar: self.commit_bar,
                        exec_bar: self.exec_bar,
                        snap_bar: self.snap_bar,
                    },
                    peer,
                )?;
            }
        }

        // ignore outdated heartbeats, reset hearing timer
        if ballot < self.bal_max_seen {
            return Ok(());
        }
        self.kickoff_hb_hear_timer()?;
        if exec_bar < self.exec_bar {
            return Ok(());
        }

        // all slots up to received commit_bar are safe to commit; submit their
        // commands for execution
        if commit_bar > self.commit_bar {
            while self.start_slot + self.insts.len() < commit_bar {
                self.insts.push(self.null_instance()?);
            }

            let mut commit_cnt = 0;
            for slot in self.commit_bar..commit_bar {
                let inst = &mut self.insts[slot - self.start_slot];
                if inst.status < Status::Accepting {
                    break;
                } else if inst.status >= Status::Committed {
                    continue;
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

                commit_cnt += 1;
            }

            if commit_cnt > 0 {
                pf_trace!(self.id; "heartbeat commit <- {} < slot {}", peer, commit_bar);
            }
        }

        if peer != self.id {
            // update peer_exec_bar if larger then known; if all servers'
            // exec_bar (including myself) have passed a slot, that slot
            // is definitely safe to be snapshotted
            if exec_bar > self.peer_exec_bar[&peer] {
                *self.peer_exec_bar.get_mut(&peer).unwrap() = exec_bar;
                let passed_cnt = 1 + self
                    .peer_exec_bar
                    .values()
                    .filter(|&&e| e >= exec_bar)
                    .count() as u8;
                if passed_cnt == self.population {
                    // all servers have executed up to exec_bar
                    self.snap_bar = exec_bar;
                }
            }

            // if snap_bar is larger than mine, update snap_bar
            if snap_bar > self.snap_bar {
                self.snap_bar = snap_bar;
            }
        }

        // pf_trace!(self.id; "heard heartbeat <- {} bal {}", peer, ballot);
        Ok(())
    }

    /// Check all instances in the Accepting phase and redo their Accepts
    /// using the current assignment policy. This is a performance optimization
    /// for soft fallback triggered when peer_alive count decreases.
    fn fallback_redo_accepts(&mut self) -> Result<(), SummersetError> {
        let now_us = self.startup_time.elapsed().as_micros();
        let alive_cnt = self.peer_alive.count();
        let mut new_pending_accepts: HashMap<
            ReplicaId,
            VecDeque<(u128, usize)>,
        > = (0..self.population)
            .filter_map(|s| {
                if s == self.id {
                    None
                } else {
                    Some((s, VecDeque::new()))
                }
            })
            .collect();

        let mut chunk_cnt = 0;
        for (slot, inst) in self
            .insts
            .iter_mut()
            .enumerate()
            .map(|(s, i)| (self.start_slot + s, i))
        {
            if inst.status == Status::Accepting && inst.leader_bk.is_some() {
                if self.assignment_balanced
                    && inst.assignment[0].count()
                        >= Self::min_shards_per_replica(
                            self.rs_data_shards,
                            self.majority,
                            self.config.fault_tolerance,
                            alive_cnt,
                        )
                {
                    // the assignment policy used for this instance was already
                    // responsive for current # of healthy nodes
                    for (peer, pending) in self.pending_accepts.iter_mut() {
                        while let Some(record) = pending.pop_front() {
                            if slot == record.1 {
                                new_pending_accepts
                                    .get_mut(peer)
                                    .unwrap()
                                    .push_back(record);
                            }
                        }
                    }
                    continue;
                }

                inst.bal = self.bal_prepared;
                inst.leader_bk.as_mut().unwrap().accept_acks.clear();
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
                    &self.peer_alive,
                );
                pf_debug!(self.id; "enter Accept phase for slot {} bal {} asgmt {}",
                                   slot, inst.bal, Self::assignment_to_string(assignment));

                let subset_copy = inst
                    .reqs_cw
                    .subset_copy(&assignment[self.id as usize], false)?;
                inst.assignment.clone_from(assignment);
                inst.voted = (inst.bal, subset_copy.clone());

                // record update to largest accepted ballot and corresponding data
                self.storage_hub.submit_action(
                    Self::make_log_action_id(slot, Status::Accepting),
                    LogAction::Append {
                        entry: WalEntry::AcceptData {
                            slot,
                            ballot: inst.bal,
                            // persist only some shards on myself
                            reqs_cw: subset_copy,
                            assignment: assignment.clone(),
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
                                       slot, inst.bal);
                chunk_cnt += 1;

                // inject heartbeats in the middle to keep peers happy
                if chunk_cnt >= self.config.msg_chunk_size {
                    self.transport_hub.bcast_msg(
                        PeerMsg::Heartbeat {
                            id: self.next_hb_id,
                            ballot: self.bal_max_seen,
                            commit_bar: self.commit_bar,
                            exec_bar: self.exec_bar,
                            snap_bar: self.snap_bar,
                        },
                        None,
                    )?;
                    for (&peer, pending) in self.pending_heartbeats.iter_mut() {
                        if self.peer_alive.get(peer)? {
                            pending.push_back((now_us, self.next_hb_id));
                        }
                    }
                    self.next_hb_id += 1;
                    chunk_cnt = 0;
                }
            }
        }

        self.pending_accepts = new_pending_accepts;
        Ok(())
    }
}
