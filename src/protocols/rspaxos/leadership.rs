//! RS-Paxos -- leader election.

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::manager::CtrlMsg;
use crate::server::{ReplicaId, LogAction};

use rand::prelude::*;

use tokio::time::Duration;

// RSPaxosReplica leadership related logic
impl RSPaxosReplica {
    /// If a larger ballot number is seen, consider that peer as new leader.
    pub fn check_leader(
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
    pub fn become_a_leader(&mut self) -> Result<(), SummersetError> {
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
        let mut recon_slots = Vec::new();
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
                    accept_acks: Bitmap::new(self.population, false),
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
            // hold enough available shards for reconstruction
            if inst.status == Status::Committed
                && inst.reqs_cw.avail_shards() < self.majority
            {
                recon_slots.push(slot);
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
            self.transport_hub
                .bcast_msg(PeerMsg::Reconstruct { slots }, None)?;
            pf_trace!(self.id; "broadcast Reconstruct messages for {} slots", num_slots);

            // inject a heartbeat after every chunk to keep peers happy
            self.transport_hub.bcast_msg(
                PeerMsg::Heartbeat {
                    ballot: self.bal_max_seen,
                    commit_bar: self.commit_bar,
                    exec_bar: self.exec_bar,
                    snap_bar: self.snap_bar,
                },
                None,
            )?;
        }
        Ok(())
    }

    /// Broadcasts heartbeats to all replicas.
    pub fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                ballot: self.bal_max_seen,
                commit_bar: self.commit_bar,
                exec_bar: self.exec_bar,
                snap_bar: self.snap_bar,
            },
            None,
        )?;

        // update max heartbeat reply counters and their repetitions seen
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
                    as u8;
                if cnts.2 > repeat_threshold {
                    // did not receive hb reply from this peer for too many
                    // past hbs sent from me; this peer is probably dead
                    if self.peer_alive.get(peer)? {
                        self.peer_alive.set(peer, false)?;
                        pf_info!(self.id; "peer_alive updated: {:?}", self.peer_alive);
                    }
                    cnts.2 = 0;
                }
            }
        }

        // I also heard this heartbeat from myself
        self.heard_heartbeat(
            self.id,
            self.bal_max_seen,
            self.commit_bar,
            self.exec_bar,
            self.snap_bar,
        )?;

        // pf_trace!(self.id; "broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Chooses a random hb_hear_timeout from the min-max range and kicks off
    /// the hb_hear_timer.
    pub fn kickoff_hb_hear_timer(&mut self) -> Result<(), SummersetError> {
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
    pub fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
        commit_bar: usize,
        exec_bar: usize,
        snap_bar: usize,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
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
}
