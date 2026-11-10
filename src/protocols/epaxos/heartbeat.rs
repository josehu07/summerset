//! `EPaxos` -- leader election.

use super::*;
use crate::server::ReplicaId;
use crate::utils::SummersetError;

// EPaxosReplica leadership related logic
impl EPaxosReplica {
    /// Reacts to a heartbeat timeout with a peer, suspecting that the peer
    /// has failed. Triggers the explicit prepare phase for all in-progress
    /// instances in that peer's row of my instance space.
    //
    // NOTE: one possible optimization is to sleep for another random, long
    //       enough duration so that healthy peers do not all attempt taking
    //       ownership of the failed row at the same time. But hopefully, the
    //       random heartbeat timeout range is enough to separate them apart
    pub(super) async fn heartbeat_timeout(
        &mut self,
        timeout_source: ReplicaId,
    ) -> Result<(), SummersetError> {
        // clear peer's heartbeat reply counters, and broadcast a heartbeat now
        self.heartbeater.clear_reply_cnts(Some(timeout_source))?;
        self.bcast_heartbeats().await?;

        // re-initialize peer_exec_bar information
        if let Some(col) = self.peer_exec_min.get_mut(&timeout_source) {
            *col = 0;
        } else {
            return logged_err!("peer {} not found in peer_exec_min", timeout_source);
        }

        // re-evaluate fast quorum eligibility for all instances currently in
        // PreAccept phase where I'm the command leader of. This prevents them
        // from being stuck in the fast quorum eligibility check
        let mut markees = vec![];
        for row in 0..self.population as usize {
            for (col, inst) in self.insts[row]
                .iter_mut()
                .enumerate()
                .map(|(c, i)| (self.start_col + c, i))
                .skip(self.commit_bars[row] - self.start_col)
            {
                if inst.status == Status::PreAccepting && inst.leader_bk.is_some() {
                    markees.push(SlotIdx(ReplicaId::try_from(row)?, col));
                }
            }
        }
        for slot in markees {
            self.handle_msg_pre_accept_reply(
                timeout_source,
                slot,
                0,
                0,
                DepSet::empty(self.population),
            )?;
        }

        // start the explicit ExpPrepare phase for all in-progress instances
        // on that peer's row
        let row = timeout_source as usize;
        let mut prepares = vec![];
        pf_info!("try explicitly preparing row {}...", row);

        for (col, inst) in self.insts[row]
            .iter_mut()
            .enumerate()
            .map(|(c, i)| (self.start_col + c, i))
            .skip(self.exec_bars[row] - self.start_col)
        {
            if inst.status >= Status::Executing
                || (inst
                    .replica_bk
                    .as_ref()
                    .is_some_and(|bk| bk.source != timeout_source))
            {
                continue;
            }
            inst.external = true; // so replies to clients can be triggered
            if inst.status == Status::Committed {
                continue;
            }

            let slot = SlotIdx(timeout_source, col);
            let new_ballot = Self::make_greater_ballot(self.id, inst.bal);
            inst.leader_bk = Some(LeaderBookkeeping {
                pre_accept_acks: Bitmap::new(self.population, false),
                pre_accept_replies: HashMap::new(),
                accept_acks: Bitmap::new(self.population, false),
                exp_prepare_acks: Bitmap::new(self.population, false),
                exp_prepare_max_bal: 0,
                exp_prepare_voteds: HashMap::new(),
            });

            // broadcast ExpPrepare messages to all peers; my own reply to
            // myself comes once my promise of it is durable
            self.transport_hub
                .bcast_msg(PeerMsg::ExpPrepare { slot, new_ballot }, None)?;
            pf_trace!(
                "broadcast ExpPrepare messages slot {} bal {}",
                slot,
                new_ballot
            );

            prepares.push((slot, new_ballot));
        }

        // also promise to my own ExpPrepares
        for (slot, new_ballot) in prepares {
            self.promise_exp_prepare(slot, new_ballot)?;
        }

        Ok(())
    }

    /// Re-sends `PreAccept`s or Accepts of my instances that peers have left
    /// unacked for a whole hearing timeout, as nothing else would re-trigger
    /// them (e.g., when they got lost).
    fn retrigger_accepts(&mut self) -> Result<(), SummersetError> {
        let (since, bar) = self.accept_retry;
        if since.elapsed() < Duration::from_millis(self.config.hb_hear_timeout_min) {
            return Ok(());
        }

        let row = self.id as usize;
        let end = self.start_col + self.insts[row].len();
        self.accept_retry = (Instant::now(), end);
        for col in self.commit_bars[row]..bar.min(end) {
            let inst = &self.insts[row][col - self.start_col];
            let Some(leader_bk) = inst.leader_bk.as_ref() else {
                continue;
            };
            if inst.bal != inst.vbal {
                continue; // a higher ballot has been promised to another
            }

            let slot = SlotIdx(self.id, col);
            let (seq, deps, reqs) = (inst.seq, inst.deps.clone(), inst.reqs.clone());
            let (msg, acks) = match inst.status {
                Status::PreAccepting => (
                    PeerMsg::PreAccept {
                        slot,
                        ballot: inst.bal,
                        seq,
                        deps,
                        reqs,
                    },
                    &leader_bk.pre_accept_acks,
                ),
                Status::Accepting => (
                    PeerMsg::Accept {
                        slot,
                        ballot: inst.bal,
                        seq,
                        deps,
                        reqs,
                    },
                    &leader_bk.accept_acks,
                ),
                _ => continue,
            };

            let targets: Vec<ReplicaId> = acks
                .iter()
                .filter_map(|(peer, acked)| (!acked && peer != self.id).then_some(peer))
                .collect();
            self.transport_hub
                .bcast_msg(msg, Some(Bitmap::from((self.population, targets))))?;
            pf_debug!(
                "re-sent {:?} for slot {} bal {}",
                inst.status,
                slot,
                inst.bal
            );
        }

        Ok(())
    }

    /// Re-sends `CommitNotice`s, once per hearing timeout, of the committed
    /// instances a peer has not executed yet in my row (or in rows of peers
    /// that seem down, which I may have committed by explicit prepares), as
    /// it may have missed them.
    fn retrigger_commits(
        &mut self,
        peer: ReplicaId,
        exec_bars: &[usize],
    ) -> Result<(), SummersetError> {
        let timeout = Duration::from_millis(self.config.hb_hear_timeout_min);
        if self
            .commit_retry
            .get(&peer)
            .is_some_and(|since| since.elapsed() < timeout)
        {
            return Ok(());
        }
        self.commit_retry.insert(peer, Instant::now());

        for (row, &exec_bar) in exec_bars.iter().enumerate() {
            let owner = ReplicaId::try_from(row)?;
            if owner != self.id && self.heartbeater.peer_alive().get(owner)? {
                continue;
            }
            let from = exec_bar.max(self.start_col);
            let to = self.commit_bars[row].min(from + self.config.msg_chunk_size);
            for col in from..to {
                let inst = &self.insts[row][col - self.start_col];
                if inst.status < Status::Committed {
                    continue;
                }
                self.transport_hub.send_msg(
                    PeerMsg::CommitNotice {
                        slot: SlotIdx(owner, col),
                        ballot: inst.vbal,
                        seq: inst.seq,
                        deps: inst.deps.clone(),
                        reqs: inst.reqs.clone(),
                    },
                    peer,
                )?;
            }
            if from < to {
                pf_debug!(
                    "re-sent CommitNotices -> {} for row {} cols {} - {}",
                    peer,
                    row,
                    from,
                    to - 1
                );
            }
        }
        Ok(())
    }

    /// Broadcasts heartbeats to all replicas.
    pub(super) async fn bcast_heartbeats(&mut self) -> Result<(), SummersetError> {
        self.retrigger_accepts()?;

        // broadcast heartbeat to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                exec_bars: self.exec_bars.clone(),
                snap_bar: self.snap_bar,
            },
            None,
        )?;

        // update max heartbeat reply counters and their repetitions seen,
        // and peers' liveness status accordingly
        self.heartbeater.update_bcast_cnts()?;

        // I also heard this heartbeat from myself
        self.heard_heartbeat(self.id, Vec::new(), self.snap_bar)?;

        // pf_trace!("broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Heard a heartbeat from some other replica. If the heartbeat carries a
    /// high enough ballot number, refreshes my hearing timer and clears my
    /// leader status if I currently think I'm a leader.
    #[allow(clippy::needless_pass_by_value)]
    pub(super) fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        exec_bars: Vec<usize>,
        snap_bar: usize,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
            // update the peer's reply cnt and its liveness status accordingly
            self.heartbeater.update_heard_cnt(peer)?;

            // reply back with a Heartbeat message
            // NOTE: commented out to favor the new all-to-all heartbeats
            //       pattern; performance-wise should have little impact
            // if self.leader == Some(peer) {
            //     self.transport_hub.send_msg(
            //         PeerMsg::Heartbeat {
            //             exec_bars: self.exec_bars.clone(),
            //             snap_bar: self.snap_bar,
            //         },
            //         peer,
            //     )?;
            // }
        }

        // reset hearing timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer(Some(peer))?;
        }
        if exec_bars.len() != self.exec_bars.len() {
            return Ok(());
        }
        if peer != self.id
            && exec_bars
                .iter()
                .zip(&self.commit_bars)
                .any(|(&exec_bar, &commit_bar)| exec_bar < commit_bar)
        {
            self.retrigger_commits(peer, &exec_bars)?;
        }
        if exec_bars < self.exec_bars {
            return Ok(());
        }

        if peer != self.id {
            // update peer_exec_bar if larger then known; if all servers'
            // exec_bar (including myself) have passed a slot, that slot
            // is (probably but not completely) safe to be snapshotted
            let exec_min = *exec_bars.iter().min().unwrap_or(&0);
            if exec_min > self.peer_exec_min[&peer] {
                *self.peer_exec_min.get_mut(&peer).unwrap() = exec_min;
                #[allow(clippy::cast_possible_truncation)]
                let passed_cnt = 1 + self
                    .peer_exec_min
                    .values()
                    .filter(|&&e| e >= exec_min)
                    .count() as u8;
                if passed_cnt == self.population {
                    // all servers have executed up to exec_min
                    self.snap_bar = exec_min;
                }
            }

            // if snap_bar is larger than mine, update snap_bar
            if snap_bar > self.snap_bar {
                self.snap_bar = snap_bar;
            }
        }

        // pf_trace!("heard heartbeat <- {} bal {}", peer, ballot);
        Ok(())
    }
}
