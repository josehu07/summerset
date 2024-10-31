//! QuorumLeases -- lease-related operations.

use super::*;

use crate::server::LeaseAction;

// QuorumLeasesReplica lease-related actions logic
impl QuorumLeasesReplica {
    /// Checks if a leaser roles configuration is valid.
    #[inline]
    pub(super) fn is_valid_conf(&self, conf: &LeaserRoles) -> bool {
        conf.grantors.size() == self.population
            && conf.grantees.size() == self.population
            && conf.grantors.count() >= self.quorum_cnt
    }

    /// Checks if I'm a majority-leased local reader.
    #[inline]
    pub(super) fn is_local_reader(&self) -> bool {
        // TODO:
        // self.is_leader()
        //     && self.bal_prepared > 0
        //     && ((self.config.enable_leader_leases
        //          && self.bal_max_seen == self.bal_prepared
        //          && self.lease_manager.lease_cnt() + 1 >= self.quorum_cnt)
        //        // [for benchmarking purposes only]
        //        || self.config.sim_read_lease)
        false
    }

    /// Processes a batch of leaser roles config change requests (taken off
    /// from a committed instance's request batch). Replies to all of them, and
    /// applies the last valid one as current config.
    pub(super) fn commit_conf_changes(
        &mut self,
        slot: usize,
        external: bool,
        conf_changes: Vec<(ClientId, RequestId, LeaserRoles)>,
    ) -> Result<(), SummersetError> {
        debug_assert!(!conf_changes.is_empty());

        // find the last valid one, if any
        let mut apply_idx = conf_changes.len();
        for (idx, (_, _, conf)) in conf_changes.iter().enumerate().rev() {
            if self.is_valid_conf(conf) {
                apply_idx = idx;
                break;
            }
        }

        // reply to all of them
        for (idx, (client, req_id, conf)) in
            conf_changes.into_iter().enumerate()
        {
            if idx != apply_idx {
                // not applied
                self.external_api.send_reply(
                    ApiReply::Conf {
                        id: req_id,
                        success: false,
                    },
                    client,
                )?;
                pf_trace!(
                    "replied -> client {} slot {} conf ignored",
                    client,
                    slot
                );
            } else {
                // applied
                pf_debug!(
                    "leaser_roles changed: v{} {:?}",
                    self.commit_bar,
                    conf
                );
                self.control_hub.send_ctrl(CtrlMsg::LeaserStatus {
                    is_grantor: conf.grantors.get(self.id)?,
                    is_grantee: conf.grantees.get(self.id)?,
                })?;
                self.leasers_cfg = conf;
                self.leasers_ver = self.commit_bar;

                if external {
                    self.external_api.send_reply(
                        ApiReply::Conf {
                            id: req_id,
                            success: true,
                        },
                        client,
                    )?;
                    pf_trace!(
                        "replied -> client {} slot {} conf applied",
                        client,
                        slot
                    );
                }
            }
        }

        Ok(())
    }

    /// Wait on lease actions until I'm sure I'm no longer granting to a peer.
    pub(super) async fn ensure_lease_revoked(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        while self.lease_manager.grant_set().get(peer)? {
            loop {
                let (lease_num, lease_action) =
                    self.lease_manager.get_action().await?;
                if self.handle_lease_action(lease_num, lease_action).await? {
                    break;
                }
            }
            // grant_set might have shrunk, re-check
        }

        Ok(())
    }

    /// Synthesized handler of lease-related actions from LeaseManager.
    /// Returns true if this action is a possible indicator that the grant_set
    /// shrunk; otherwise returns false.
    pub(super) async fn handle_lease_action(
        &mut self,
        lease_num: LeaseNum,
        lease_action: LeaseAction,
    ) -> Result<bool, SummersetError> {
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
