//! MultiPaxos -- lease-related operations.

use super::*;

use crate::server::LeaseAction;

// MultiPaxosReplica lease-related actions logic
impl MultiPaxosReplica {
    /// Wait on lease actions until I'm sure I'm no longer granting to a peer.
    pub(super) async fn ensure_lease_revoked(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Synthesized handler of lease-related actions from LeaseManager.
    /// Returns a peer's ID if this action is a possible indicator of
    /// a `grant_set()` change for the peer.
    pub(super) async fn handle_lease_action(
        &mut self,
        lease_num: LeaseNum,
        lease_action: LeaseAction,
    ) -> Result<Option<ReplicaId>, SummersetError> {
        match lease_action {
            LeaseAction::SendLeaseMsg { peer, msg } => {
                self.transport_hub.send_lease_msg(lease_num, msg, peer)?;
            }
            LeaseAction::BcastLeaseMsgs { peers, msg } => {
                self.transport_hub.bcast_lease_msg(
                    lease_num,
                    msg,
                    Some(peers),
                )?;
            }
            LeaseAction::RevokeReplied { peer, .. }
            | LeaseAction::GrantTimeout { peer } => {
                // tell caller that it might want to double check `grant_set()`
                return Ok(Some(peer));
            }
            _ => {}
        }

        Ok(None)
    }
}
