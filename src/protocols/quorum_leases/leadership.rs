//! QuorumLeases -- leader election & heartbeats.

use super::*;

use crate::manager::CtrlMsg;
use crate::server::{LeaseNotice, LogAction, ReplicaId};
use crate::utils::{Bitmap, SummersetError};

// QuorumLeasesReplica heartbeats related logic
impl QuorumLeasesReplica {
    /// If a larger ballot number is seen, consider that peer as new leader.
    pub(super) async fn check_leader(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
    ) -> Result<(), SummersetError> {
        if ballot > self.bal_max_seen {
            // clear my leader status if I was one
            if self.is_leader() {
                self.control_hub
                    .send_ctrl(CtrlMsg::LeaderStatus { step_up: false })?;
                pf_info!("no longer a leader...");
            }

            // reset heartbeat timeout timer promptly
            if !self.config.disable_hb_timer {
                self.heartbeater.kickoff_hear_timer(Some(peer))?;
            }

            // if leasing enabled, revoke old lease if any made to old leader,
            // then initiate granting to the new leader
            if self.config.enable_leader_leases {
                if let Some(old_leader) = self.leader {
                    if old_leader != self.id {
                        self.llease_manager.add_notice(
                            self.bal_max_seen,
                            LeaseNotice::DoRevoke {
                                peers: Some(Bitmap::from((
                                    self.population,
                                    vec![old_leader],
                                ))),
                            },
                        )?;
                        self.ensure_llease_revoked(old_leader).await?;
                    }
                }

                // initiate granting to new leader
                self.llease_manager.add_notice(
                    ballot, // use new ballot as lease_num
                    LeaseNotice::NewGrants {
                        peers: Some(Bitmap::from((
                            self.population,
                            vec![peer],
                        ))),
                    },
                )?;
            }

            // set this peer to be the believed leader
            debug_assert_ne!(peer, self.id);
            self.leader = Some(peer);
            self.bal_max_seen = ballot;
        }

        Ok(())
    }

    /// If current leader is not me but times out, steps up as leader, and
    /// sends self-initiated Prepare messages to followers for all in-progress
    /// instances.
    pub(super) async fn become_a_leader(
        &mut self,
        timeout_source: ReplicaId,
    ) -> Result<(), SummersetError> {
        if self.leader.as_ref().is_some_and(|&l| l != timeout_source) {
            return Ok(());
        }

        // if leasing enabled, start to revoke old lease
        let mut old_leader = None;
        if self.config.enable_leader_leases {
            if let Some(leader) = self.leader {
                if leader != self.id {
                    self.llease_manager.add_notice(
                        self.bal_max_seen,
                        LeaseNotice::DoRevoke {
                            peers: Some(Bitmap::from((
                                self.population,
                                vec![leader],
                            ))),
                        },
                    )?;
                    old_leader = Some(leader);
                }
            }
        }

        self.leader = Some(self.id);
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;
        pf_info!("becoming a leader...");

        // clear peers' heartbeat reply counters, and broadcast a heartbeat now
        self.heartbeater.clear_reply_cnts(None)?;
        self.bcast_heartbeats().await?;

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
            self.insts.push(self.null_instance());
        }
        pf_debug!(
            "enter Prepare phase trigger_slot {} bal {}",
            trigger_slot,
            self.bal_prep_sent
        );

        // redo Prepare phase for all in-progress instances
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
            if inst.status == Status::Committed {
                continue;
            }

            inst.bal = self.bal_prep_sent;
            inst.status = Status::Preparing;
            inst.leader_bk = Some(LeaderBookkeeping {
                trigger_slot,
                endprep_slot,
                prepare_acks: Bitmap::new(self.population, false),
                prepare_max_bal: 0,
                accept_acks: Bitmap::new(self.population, false),
                accept_grant_sets: HashMap::new(),
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
            pf_trace!(
                "submitted PrepareBal log action for slot {} bal {}",
                slot,
                inst.bal
            );
        }

        // clear peers' accept_bar information
        for bar in self.peer_accept_bar.values_mut() {
            *bar = usize::MAX;
        }
        self.peer_accept_max = usize::MAX;

        // send Prepare message to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Prepare {
                trigger_slot,
                ballot: self.bal_prep_sent,
            },
            None,
        )?;
        pf_trace!(
            "broadcast Prepare messages trigger_slot {} bal {}",
            trigger_slot,
            self.bal_prep_sent
        );

        // before moving on, ensure that any lease to old leader has either
        // been RevokeReplied or timed out
        if self.config.enable_leader_leases {
            if let Some(old_leader) = old_leader {
                self.ensure_llease_revoked(old_leader).await?;
            }
        }

        Ok(())
    }

    /// Broadcasts heartbeats to all replicas.
    pub(super) async fn bcast_heartbeats(
        &mut self,
    ) -> Result<(), SummersetError> {
        // check and send leader lease promise refresh to leader
        if self.config.enable_leader_leases {
            if let Some(leader) = self.leader {
                if leader != self.id {
                    let to_refresh = self.llease_manager.attempt_refresh(
                        Some(&Bitmap::from((self.population, vec![leader]))),
                    )?;
                    if to_refresh.count() > 0 {
                        self.transport_hub.bcast_lease_msg(
                            0, // gid 0 for leader leases
                            self.bal_max_seen,
                            LeaseMsg::Promise,
                            Some(to_refresh),
                        )?;
                    }
                }
            }
        }

        // check and send quorum lease promise refresh to active grantees
        if self.qlease_num as usize + 1 == self.commit_bar {
            let to_refresh = self
                .qlease_manager
                .attempt_refresh(Some(&self.qlease_cfg.grantees))?;
            if to_refresh.count() > 0 {
                self.transport_hub.bcast_lease_msg(
                    1, // gid 1 for quorum leases
                    self.qlease_num,
                    LeaseMsg::Promise,
                    Some(to_refresh),
                )?;
            }
        }

        // broadcast heartbeat to all peers
        self.transport_hub.bcast_msg(
            PeerMsg::Heartbeat {
                ballot: self.bal_max_seen,
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
            self.commit_bar,
            self.exec_bar,
            self.snap_bar,
        )
        .await?;

        // pf_trace!("broadcast heartbeats bal {}", self.bal_prep_sent);
        Ok(())
    }

    /// Heard a heartbeat from some other replica. If the heartbeat carries a
    /// high enough ballot number, refreshes my hearing timer and clears my
    /// leader status if I currently think I'm a leader.
    pub(super) async fn heard_heartbeat(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
        commit_bar: usize,
        exec_bar: usize,
        snap_bar: usize,
    ) -> Result<(), SummersetError> {
        if peer != self.id {
            // update the peer's reply cnt and its liveness status accordingly
            self.heartbeater.update_heard_cnt(peer)?;

            // if the peer has made a higher ballot number, consider it as
            // a new leader
            self.check_leader(peer, ballot).await?;

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

        // ignore outdated heartbeats, reset hearing timer
        if ballot < self.bal_max_seen {
            return Ok(());
        }
        if !self.config.disable_hb_timer
            && self.leader == Some(peer)
            && self.bal_max_seen == ballot
        {
            self.heartbeater.kickoff_hear_timer(Some(peer))?;
        }
        if exec_bar < self.exec_bar {
            return Ok(());
        }

        // all slots up to received commit_bar are safe to commit; submit their
        // commands for execution
        self.advance_commit_bar(peer, ballot, commit_bar)?;

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

        // pf_trace!("heard heartbeat <- {} bal {}", peer, ballot);
        Ok(())
    }

    /// React to a CommitNotice message from leader.
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

    /// React to an updated commit_bar received from (probably) leader. Slots
    /// up to received commit_bar are safe to commit; submit their commands
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
