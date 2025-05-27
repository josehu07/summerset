//! Bodega -- config lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNotice, LeaseNum, LogAction};

// BodegaReplica config lease-related actions logic
impl BodegaReplica {
    /// Runs the leasing procedure for when a newer config with higher ballot
    /// number is heard.
    pub(super) async fn heard_new_conf(
        &mut self,
        new_bal: Ballot,
        new_conf: RespondersConf,
    ) -> Result<(), SummersetError> {
        debug_assert!(new_bal > self.bal_max_seen);
        pf_debug!(
            "discovered higher bal {} new conf {:?}...",
            new_bal,
            new_conf
        );

        // revoke leases on the old config
        self.revoke_conf_leases().await?;

        // update my highest seen ballot and config
        self.bal_max_seen = new_bal;
        self.bodega_conf = new_conf;

        // clear peers' accept_bar information
        for (&peer, bar) in self.peer_accept_bar.iter_mut() {
            if peer == self.id {
                *bar = self.accept_bar;
            } else {
                *bar = usize::MAX;
            }
        }
        self.peer_accept_max = usize::MAX;

        // initiate granting leases for the new config
        self.initiate_conf_leases().await?;

        // if I will be the new leader:
        if self.bodega_conf.is_leader(self.id) {
            self.become_a_leader().await?;
        }

        Ok(())
    }

    /// Revokes leases I'm currenting granting out, wait for their confirmation
    /// replies or lease granting timeout.
    async fn revoke_conf_leases(&mut self) -> Result<(), SummersetError> {
        self.lease_manager.add_notice(
            self.bal_max_seen,
            LeaseNotice::DoRevoke { peers: None },
        )?;

        // ensure every peer has either RevokeReplied or timed out
        while self.lease_manager.grant_set().count() > 0 {
            loop {
                match time::timeout(
                    Duration::from_millis(self.config.hb_send_interval_ms),
                    self.lease_manager.get_action(),
                )
                .await
                {
                    Ok(lease_action) => {
                        let (lease_num, lease_action) = lease_action?;
                        if self
                            .handle_lease_action(lease_num, lease_action)
                            .await?
                        {
                            break;
                        }
                    }

                    // a heartbeat interval has passed without receiving any
                    // new lease action; promptively broadcast heartbeats
                    // here to prevent temporary starving due to possibly
                    // having to wait on lease expirations
                    // NOTE: a nicer implementation could make the heartbeat
                    //       bcast action a separate bg periodic task; this
                    //       hack looks ugly but works for now
                    Err(_) => {
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
                    }
                }
            }

            // grant_set might have shrunk, re-check
        }

        // safe kickoff of all heartbeat timers for peers to prevent false
        // negative timeouts from the main event loop once returned, since
        // their heartbeat messages might be queued up in the transport recv
        // channel during my wait on lease expirations
        self.refresh_heartbeat_timer(None)?;

        Ok(())
    }

    /// Initiates granting leases for a new config.
    async fn initiate_conf_leases(&mut self) -> Result<(), SummersetError> {
        self.lease_manager.add_notice(
            self.bal_max_seen,
            LeaseNotice::NewGrants {
                peers: None,
                accept_bar: Some(self.accept_bar),
            },
        )?;

        Ok(())
    }

    /// Things to do when stepping up as leader in a new config. Assumes both
    /// `bal_max_seen` and `bodega_conf` fields have been updated.
    async fn become_a_leader(&mut self) -> Result<(), SummersetError> {
        // update leader status to manager
        self.control_hub
            .send_ctrl(CtrlMsg::LeaderStatus { step_up: true })?;
        pf_info!("becoming a leader, new conf {:?}...", self.bodega_conf);

        // clear peers' heartbeat reply counters
        self.heartbeater.clear_reply_cnts(None)?;

        // re-initialize peer_exec_bar information
        for slot in self.peer_exec_bar.values_mut() {
            *slot = 0;
        }

        // make a greater ballot number and invalidate all in-progress instances
        self.bal_prepared = 0;
        self.bal_prep_sent = self.bal_max_seen;

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

        // clear any held read requests just to be safe
        for slot in trigger_slot..=endprep_slot {
            self.release_held_read_reqs(slot)?;
        }

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

        Ok(())
    }

    /// Synthesized handler of leader leasing actions from its LeaseManager.
    /// Returns true if this action is a possible indicator that the grant_set
    /// shrunk; otherwise returns false.
    pub(super) async fn handle_lease_action(
        &mut self,
        lease_num: LeaseNum,
        lease_action: LeaseAction,
    ) -> Result<bool, SummersetError> {
        match lease_action {
            LeaseAction::SendLeaseMsg { peer, msg } => {
                self.transport_hub.send_lease_msg(
                    0, // only one type of leases exist in Bodega
                    lease_num, msg, peer,
                )?;
            }
            LeaseAction::BcastLeaseMsgs { peers, msg } => {
                self.transport_hub.bcast_lease_msg(
                    0, // only one type of leases exist in Bodega
                    lease_num,
                    msg,
                    Some(peers),
                )?;
            }

            LeaseAction::GrantRemoved { .. }
            | LeaseAction::GrantTimeout { .. }
            | LeaseAction::HigherNumber => {
                // tell revoker that it might want to double check grant_set
                return Ok(true);
            }

            LeaseAction::GuardAcceptBar { peer, accept_bar } => {
                // update this peer's accept_bar information, and then update
                // peer_accept_max as the minimum of the maximums of majority sets
                if let Some(old_bar) =
                    self.peer_accept_bar.insert(peer, accept_bar)
                {
                    if accept_bar < old_bar {
                        let mut peer_accept_bars: Vec<usize> =
                            self.peer_accept_bar.values().copied().collect();
                        peer_accept_bars.sort_unstable();
                        let peer_accept_max =
                            peer_accept_bars[self.quorum_cnt as usize - 1];
                        if peer_accept_max < self.peer_accept_max {
                            self.peer_accept_max = peer_accept_max;
                            pf_debug!(
                                "peer_accept_max updated: {}",
                                self.peer_accept_max
                            );
                        }
                    }
                }
            }

            _ => {
                // nothing special protocol-specific to do for other actions
            }
        }

        Ok(false)
    }
}
