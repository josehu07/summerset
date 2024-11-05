//! QuorumLeases -- leader lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNum};

// QuorumLeasesReplica leader lease-related actions logic
impl QuorumLeasesReplica {
    /// Checks if I'm a stable majority-leased leader.
    #[inline]
    pub(super) fn is_stable_leader(&self) -> bool {
        self.is_leader()
            && self.bal_prepared > 0
            && ((self.config.enable_leader_leases
                 && self.bal_max_seen == self.bal_prepared
                 && self.llease_manager.lease_cnt() + 1 >= self.quorum_cnt
                 && self.commit_bar >= self.peer_accept_max)
                // [for benchmarking purposes only]
                || self.config.sim_read_lease)
    }

    /// Wait on lease actions until I'm sure I'm no longer granting to a peer.
    pub(super) async fn ensure_llease_revoked(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        while self.llease_manager.grant_set().get(peer)? {
            loop {
                let (lease_num, lease_action) =
                    self.llease_manager.get_action().await?;

                if self.handle_llease_action(lease_num, lease_action).await? {
                    break;
                }
            }
            // grant_set might have shrunk, re-check
        }

        Ok(())
    }

    /// Synthesized handler of leader leasing actions from its LeaseManager.
    /// Returns true if this action is a possible indicator that the grant_set
    /// shrunk; otherwise returns false.
    pub(super) async fn handle_llease_action(
        &mut self,
        lease_num: LeaseNum,
        lease_action: LeaseAction,
    ) -> Result<bool, SummersetError> {
        match lease_action {
            LeaseAction::SendLeaseMsg { peer, msg } => {
                self.transport_hub.send_lease_msg(
                    0, // gid 0 for leader leases
                    lease_num, msg, peer,
                )?;
            }
            LeaseAction::BcastLeaseMsgs { peers, msg } => {
                self.transport_hub.bcast_lease_msg(
                    0, // gid 0 for leader leases
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

            _ => {
                // nothing special protocol-specific to do for other actions
            }
        }

        Ok(false)
    }
}
