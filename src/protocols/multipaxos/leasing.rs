//! MultiPaxos -- lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNotice};

// MultiPaxosReplica lease-related actions logic
impl MultiPaxosReplica {
    /// Mark the next heartbeat to given peer as a lease promise refresh.
    fn mark_next_hb_refresh(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Handle a grantor-side timeout.
    fn handle_grant_timeout(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Handle a leaseholder-side timeout.
    fn handle_lease_timeout(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
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
                self.mark_next_hb_refresh(peer)?;
            }
            LeaseAction::GrantTimeout { peer } => {
                self.handle_grant_timeout(peer)?;
            }
            LeaseAction::LeaseTimeout { peer } => {
                self.handle_lease_timeout(peer)?;
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
            pf_warn!(
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
