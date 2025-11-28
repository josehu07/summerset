//! `Bodega` -- leader election & heartbeats.

use std::mem;

use rand::prelude::*;

use super::*;
use crate::manager::CtrlMsg;
use crate::server::{LeaseMsg, LogAction, ReplicaId};
use crate::utils::SummersetError;

// BodegaReplica heartbeats related logic
impl BodegaReplica {
    /// If a larger ballot number is seen, promptly unset my special roles in
    /// configuration (real config change will be carried over in some heartbeat
    /// from some peer soon).
    pub(super) fn check_ballot(
        &mut self,
        _peer: ReplicaId,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if ballot > self.bal_max_seen {
            // clear my leader status on manager if I was one
            if self.is_leader() {
                self.control_hub
                    .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
                pf_info!("no longer leader, down to filtered conf...");
            }

            self.bodega_conf =
                mem::take(&mut self.bodega_conf).into_filtered(self.id)?;
            self.bal_max_seen = ballot;
        }

        Ok(())
    }

    /// Basically a duplicate of how Heartbeater kicks off a heartbeat timer.
    /// Could certainly have better implementation here, but non-essential.
    pub(super) fn refresh_volunteer_timer(
        &mut self,
    ) -> Result<(), SummersetError> {
        self.volunteer_timer.cancel()?;

        let timeout_ms = rand::rng().random_range(
            self.config.hb_hear_timeout_min..=self.config.hb_hear_timeout_max,
        );
        // pf_trace!("kickoff volunteer_timer @ {} ms", timeout_ms);
        self.volunteer_timer
            .kickoff(Duration::from_millis(timeout_ms as u64))
    }

    /// Refreshes the heartbeat timer for given peer, and if current config has
    /// a leader, also refreshes the volunteer step-up timer. If `peer` is
    /// `None`, refreshes the heartbeat timer for everyone.
    pub(super) fn refresh_heartbeat_timer(
        &mut self,
        peer: Option<ReplicaId>,
    ) -> Result<(), SummersetError> {
        if !self.config.disable_hb_timer {
            // refresh heartbeat timer for given peer
            self.heartbeater.kickoff_hear_timer(peer)?;

            // if current config has a leader, also prevent volunteering step-up
            if self.bodega_conf.leader.is_some() {
                self.refresh_volunteer_timer()?;
            }
        }

        Ok(())
    }

    /// Reacts to a heartbeat timeout with a peer, suspecting that the peer
    /// has failed. Proposes a new config with a higher unique ballot number;
    /// the failed peer will be removed from any special roles in the new
    /// config. If it was the leader, I will try to step up instead.
    pub(super) async fn heartbeat_timeout(
        &mut self,
        timeout_source: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_debug!("heartbeat hearing timeout <- {}", timeout_source);

        // compose a new proper config by removing the timed-out peer from any
        // special roles in the old config, and step myself up as leader if
        // there wasn't one
        let mut new_conf = if timeout_source == self.id {
            self.bodega_conf.clone() // special volunteer timeout, keep myself
        } else {
            self.bodega_conf.clone_filtered(timeout_source)?
        };
        if new_conf.leader.is_none() && !self.config.disallow_step_up {
            new_conf.leader = Some(self.id);
        }

        // if the composed new config has changes from the old one, announce
        // the new config to peers; if I am leader in the new config, will also
        // trigger the step-up procedure
        if new_conf != self.bodega_conf {
            self.announce_new_conf(new_conf.clone()).await?;
            self.control_hub.send_ctrl(CtrlMsg::RespondersConf {
                conf_num: self.bal_max_seen,
                new_conf,
            })?;
        }

        Ok(())
    }

    /// Makes a higher unique ballot number and announces the new config to
    /// peers by broadcasting a round of heartbeats carrying the new config.
    pub(super) async fn announce_new_conf(
        &mut self,
        new_conf: RespondersConf<()>,
    ) -> Result<(), SummersetError> {
        let new_bal = self.make_greater_ballot(self.bal_max_seen);
        pf_debug!(
            "announcing higher bal {} new conf {:?}...",
            new_bal,
            new_conf
        );

        // broadcast heartbeat to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                ballot: new_bal,
                conf: new_conf.clone(),
                commit_bar: self.commit_bar,
                exec_bar: self.exec_bar,
                snap_bar: self.snap_bar,
            },
            None,
        )?;

        // update max heartbeat reply counters and their repetitions seen,
        // and peers' liveness status accordingly
        self.heartbeater.update_bcast_cnts()?;

        // I also heard this heartbeat from myself
        self.heard_heartbeat(
            self.id,
            new_bal,
            Some(new_conf),
            self.commit_bar,
            self.exec_bar,
            self.snap_bar,
        )
        .await?;

        Ok(())
    }

    /// Broadcasts regular heartbeats to all replicas, carrying my current
    /// ballot and config.
    pub(super) async fn bcast_heartbeats(
        &mut self,
    ) -> Result<(), SummersetError> {
        // check and send lease refreshers to all peers
        let to_refresh = self.lease_manager.attempt_refresh(None)?;
        if to_refresh.count() > 0 {
            self.transport_hub.bcast_lease_msg(
                0, // only one lease purpose exists in Bodega
                self.bal_max_seen,
                LeaseMsg::Promise,
                Some(to_refresh),
            )?;
        }

        // broadcast heartbeat to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                ballot: self.bal_max_seen,
                conf: self.bodega_conf.clone(),
                commit_bar: self.commit_bar,
                exec_bar: self.exec_bar,
                snap_bar: self.snap_bar,
            },
            None,
        )?;

        // update max heartbeat reply counters and their repetitions seen,
        // and peers' liveness status accordingly
        self.heartbeater.update_bcast_cnts()?;

        // I also heard this heartbeat from myself
        self.heard_heartbeat(
            self.id,
            self.bal_max_seen,
            None,
            self.commit_bar,
            self.exec_bar,
            self.snap_bar,
        )
        .await?;

        // pf_trace!("broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Heard a heartbeat from some other replica. If the heartbeat carries a
    /// high enough ballot number, shifts to the newer config.
    pub(super) async fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
        conf: Option<RespondersConf<()>>,
        commit_bar: usize,
        exec_bar: usize,
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
            //             ballot: self.bal_max_seen,
            //             commit_bar: self.commit_bar,
            //             exec_bar: self.exec_bar,
            //             snap_bar: self.snap_bar,
            //         },
            //         peer,
            //     )?;
            // }
        }

        // ignore outdated heartbeats, reset hearing timer; notice here we do
        // refresh for outdated ballots also -- no any negative effects here
        // but helps with config leases shifts
        self.refresh_heartbeat_timer(Some(peer))?;
        if ballot < self.bal_max_seen {
            return Ok(());
        }

        // if seeing a higher ballot, then the heartbeat must be carrying a new
        // config; do the config update procedure
        if ballot > self.bal_max_seen {
            debug_assert!(conf.is_some());
            if let Some(new_conf) = conf {
                self.heard_new_conf(ballot, new_conf).await?;
            } else {
                return logged_err!(
                    "heartbeat contains higher ballot but no conf"
                );
            }
        }

        // all slots up to received commit_bar are safe to commit; submit their
        // commands for execution
        if exec_bar < self.exec_bar {
            return Ok(());
        }
        self.advance_commit_bar(peer, ballot, commit_bar)?;

        if peer != self.id {
            // update peer_exec_bar if larger then known; if all servers'
            // exec_bar (including myself) have passed a slot, that slot
            // is definitely safe to be snapshotted
            if exec_bar > self.peer_exec_bar[&peer] {
                *self.peer_exec_bar.get_mut(&peer).unwrap() = exec_bar;
                #[allow(clippy::cast_possible_truncation)]
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

        // pf_trace!("heard heartbeat <- {} bal {}", peer, ballot);
        Ok(())
    }

    /// React to a `CommitNotice` message from leader.
    pub(super) fn heard_commit_notice(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
        commit_bar: usize,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "received CommitNotice <- {} for bal {} commit_bar {}",
            peer,
            ballot,
            commit_bar
        );

        if ballot == self.bal_max_seen {
            self.advance_commit_bar(peer, ballot, commit_bar)?;
        }

        Ok(())
    }

    /// React to an updated `commit_bar` received from (probably) leader. Slots
    /// up to received `commit_bar` are safe to commit; submit their commands
    /// for execution.
    fn advance_commit_bar(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
        commit_bar: usize,
    ) -> Result<(), SummersetError> {
        if commit_bar > self.commit_bar {
            while self.start_slot + self.insts.len() < commit_bar {
                self.insts.push(self.null_instance());
            }

            let mut commit_cnt = 0;
            for slot in self.commit_bar..commit_bar {
                let inst = &mut self.insts[slot - self.start_slot];
                if inst.bal < ballot || inst.status < Status::Accepting {
                    break;
                } else if inst.status >= Status::Committed {
                    continue;
                }

                // mark this instance as committed
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

                commit_cnt += 1;
            }

            if commit_cnt > 0 {
                pf_trace!(
                    "advancing commit <- {} until slot {}",
                    peer,
                    commit_bar
                );
            }
        }

        Ok(())
    }
}
