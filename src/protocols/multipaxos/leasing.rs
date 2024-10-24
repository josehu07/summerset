//! MultiPaxos -- lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNotice};

// MultiPaxosReplica lease-related actions logic
impl MultiPaxosReplica {
    /// Marks the next heartbeat to given peer as a lease promise refresh.
    fn handle_schedule_refresh(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Initiates granting lease to current leader after successful revocation
    /// of the old.
    fn handle_revoke_replied(
        &mut self,
        _lease_num: LeaseNum,
        _peer: ReplicaId,
        _held: bool,
    ) -> Result<(), SummersetError> {
        if !self.config.disable_leasing {
            if let Some(leader) = self.leader {
                if leader != self.id {
                    if self.bal_max_seen < self.lease_num {
                        pf_debug!(
                            "new ballot {} < active lease_num {}, granting not initiated",
                            self.bal_max_seen,
                            self.lease_num
                        );
                    } else {
                        self.lease_num = self.bal_max_seen;
                        self.lease_manager.add_notice(
                            self.lease_num,
                            LeaseNotice::NewGrants {
                                peers: Bitmap::from((
                                    self.population,
                                    vec![leader],
                                )),
                            },
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle a grantor-side timeout.
    fn handle_grant_timeout(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_debug!("lease grant timeout @ {} -> {}", lease_num, peer);
        self.handle_revoke_replied(lease_num, peer, false)
    }

    /// Handle a leaseholder-side timeout.
    fn handle_lease_timeout(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_debug!("leaseholder timeout @ {} <- {}", lease_num, peer);
        Ok(())
    }

    /// Synthesized handler of lease-related actions from LeaseManager.
    pub(super) fn handle_lease_action(
        &mut self,
        lease_num: LeaseNum,
        lease_action: LeaseAction,
    ) -> Result<(), SummersetError> {
        if lease_num != self.lease_num {
            pf_warn!(
                "mismatching lease_num in action: {} != curr {}",
                lease_num,
                self.lease_num
            );
            return Ok(());
        }

        match lease_action {
            LeaseAction::SendLeaseMsg { peer, msg } => {
                self.transport_hub.send_msg(
                    PeerMsg::LeaseMsg {
                        lease_num,
                        lease_msg: msg,
                    },
                    peer,
                )?;
            }
            LeaseAction::BcastLeaseMsgs { peers, msg } => {
                self.transport_hub.bcast_msg(
                    PeerMsg::LeaseMsg {
                        lease_num,
                        lease_msg: msg,
                    },
                    Some(peers),
                )?;
            }
            LeaseAction::ScheduleRefresh { peer } => {
                self.handle_schedule_refresh(lease_num, peer)?;
            }
            LeaseAction::RevokeReplied { peer, held } => {
                self.handle_revoke_replied(lease_num, peer, held)?;
            }
            LeaseAction::GrantTimeout { peer } => {
                self.handle_grant_timeout(lease_num, peer)?;
            }
            LeaseAction::LeaseTimeout { peer } => {
                self.handle_lease_timeout(lease_num, peer)?;
            }
            LeaseAction::SyncBarrier => {
                return logged_err!("unexpected SyncBarrier lease action");
            }
        }

        Ok(())
    }

    /// Synthesized handler of lease-related messages from peer.
    pub(super) fn handle_lease_msg(
        &mut self,
        peer: ReplicaId,
        lease_num: LeaseNum,
        lease_msg: LeaseMsg,
    ) -> Result<(), SummersetError> {
        #[allow(clippy::comparison_chain)]
        if lease_num < self.lease_num {
            pf_debug!(
                "ignoring outdated lease_num: {} < {}",
                lease_num,
                self.lease_num
            );
            return Ok(());
        } else if lease_num > self.lease_num {
            // starting the grants of a new lease number, invalidate everything
            // with old number
            pf_debug!(
                "observed higher lease_num: {} > {}",
                lease_num,
                self.lease_num
            );
            self.lease_num = lease_num;
        }

        self.lease_manager.add_notice(
            lease_num,
            LeaseNotice::RecvLeaseMsg {
                peer,
                msg: lease_msg,
            },
        )
    }
}
