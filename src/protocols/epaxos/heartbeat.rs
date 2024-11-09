//! EPaxos -- leader election.

use super::*;

use crate::server::ReplicaId;
use crate::utils::SummersetError;

// EPaxosReplica leadership related logic
impl EPaxosReplica {
    /// Reacts to a heartbeat timeout with a peer, suspecting that the peer
    /// has failed. Triggers the explicit prepare phase for all in-progress
    /// instances in that peer's row of my instance space.
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
            return logged_err!(
                "peer {} not found in peer_exec_min",
                timeout_source
            );
        }

        // start the explicit ExpPrepare phase for all in-progress instances
        // on that peer's row
        let row = timeout_source as usize;
        for (col, inst) in self.insts[row]
            .iter_mut()
            .enumerate()
            .map(|(c, i)| (self.start_col + c, i))
            .skip(self.exec_bars[row] - self.start_col)
        {
            if inst.status == Status::Executed {
                continue;
            }
            inst.external = true; // so replies to clients can be triggered
            if inst.status == Status::Committed {
                continue;
            }

            // broadcast ExpPrepare messages to all peers. Note that the
            // instance state on my own replica is not updated; my reply to
            // myself will be part of the set of ExpPrepareReplies
            let new_ballot =
                Self::make_greater_ballot(timeout_source, inst.bal);
            self.transport_hub.bcast_msg(
                PeerMsg::ExpPrepare {
                    slot: SlotIdx(timeout_source, col),
                    ballot: new_ballot,
                },
                None,
            )?;
            pf_trace!(
                "broadcast ExpPrepare messages slot {} bal {}",
                SlotIdx(timeout_source, col),
                new_ballot
            );
        }

        Ok(())
    }

    /// Broadcasts heartbeats to all replicas.
    pub(super) async fn bcast_heartbeats(
        &mut self,
    ) -> Result<(), SummersetError> {
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
        self.heard_heartbeat(self.id, Vec::with_capacity(0), self.snap_bar)?;

        // pf_trace!("broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Heard a heartbeat from some other replica. If the heartbeat carries a
    /// high enough ballot number, refreshes my hearing timer and clears my
    /// leader status if I currently think I'm a leader.
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
            // FIXME: correct per-peer timeouts
            self.heartbeater.kickoff_hear_timer(Some(peer))?;
        }
        if exec_bars.len() != self.exec_bars.len() || exec_bars < self.exec_bars
        {
            return Ok(());
        }

        if peer != self.id {
            // update peer_exec_bar if larger then known; if all servers'
            // exec_bar (including myself) have passed a slot, that slot
            // is (probably but not completely) safe to be snapshotted
            let exec_min = *exec_bars.iter().min().unwrap_or(&0);
            if exec_min > self.peer_exec_min[&peer] {
                *self.peer_exec_min.get_mut(&peer).unwrap() = exec_min;
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
