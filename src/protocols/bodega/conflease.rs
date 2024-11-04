//! Bodega -- config lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNum};

// BodegaReplica config lease-related actions logic
impl BodegaReplica {
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

            _ => {
                // nothing special protocol-specific to do for other actions
            }
        }

        Ok(false)
    }
}
