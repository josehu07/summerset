//! QuorumLeases -- quorum lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNum};

// QuorumLeasesReplica quorum lease-related actions logic
impl QuorumLeasesReplica {
    /// Checks if I'm a majority-leased local reader.
    #[inline]
    pub(super) fn is_local_reader(&self) -> Result<bool, SummersetError> {
        Ok((self.qlease_grantees().get(self.id)?
                && self.qlease_manager.lease_cnt() >= self.quorum_cnt
                && (self.config.no_lease_retraction
                    || self.qlease_num as usize + 1 == self.commit_bar))
           // [for benchmarking purposes only]
           || self.config.sim_read_lease)
    }

    /// The commit condition check. Besides requiring an AcceptReply quorum
    /// size of at least majority, it also requires that replies from all
    /// possible grantees have been received.
    pub(super) fn commit_condition(
        leader_bk: &LeaderBookkeeping,
        quorum_cnt: u8,
    ) -> Result<bool, SummersetError> {
        if leader_bk.accept_acks.count() < quorum_cnt {
            return Ok(false);
        }

        for grant_set in leader_bk.accept_grant_sets.values() {
            for grantee in
                grant_set.iter().filter_map(
                    |(p, flag)| {
                        if flag {
                            Some(p)
                        } else {
                            None
                        }
                    },
                )
            {
                if !leader_bk.accept_acks.get(grantee)? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Wait on lease actions until I'm sure I've cleared all promises I held.
    pub(super) async fn ensure_qlease_cleared(
        &mut self,
    ) -> Result<(), SummersetError> {
        loop {
            let (lease_num, lease_action) =
                self.qlease_manager.get_action().await?;

            let cleared = matches!(
                lease_action,
                LeaseAction::LeaseCleared | LeaseAction::HigherNumber
            );
            self.handle_qlease_action(lease_num, lease_action).await?;
            if cleared {
                break;
            }
        }

        Ok(())
    }

    /// Wait on lease actions until I'm sure I'm no longer granting to a peer.
    pub(super) async fn ensure_qlease_revoked(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        while self.qlease_manager.grant_set().get(peer)? {
            loop {
                let (lease_num, lease_action) =
                    self.qlease_manager.get_action().await?;

                if self.handle_qlease_action(lease_num, lease_action).await? {
                    break;
                }

                // promptively broadcast heartbeats here to prevent temporary
                // starving due to possibly having to wait on lease expirations
                // NOTE: a nicer implementation could make the heartbeat bcast
                //       action a separate background periodic task
                self.transport_hub.bcast_msg(
                    PeerMsg::Heartbeat {
                        ballot: self.bal_max_seen,
                        commit_bar: self.commit_bar,
                        exec_bar: self.exec_bar,
                        snap_bar: self.snap_bar,
                    },
                    None,
                )?;
            }

            // grant_set might have shrunk, re-check
        }

        Ok(())
    }

    /// Synthesized handler of quorum leasing actions from its LeaseManager.
    /// Returns true if this action is a possible indicator that the grant_set
    /// shrunk; otherwise returns false.
    pub(super) async fn handle_qlease_action(
        &mut self,
        lease_num: LeaseNum,
        lease_action: LeaseAction,
    ) -> Result<bool, SummersetError> {
        match lease_action {
            LeaseAction::SendLeaseMsg { peer, msg } => {
                self.transport_hub.send_lease_msg(
                    1, // gid 1 for quorum leases
                    lease_num, msg, peer,
                )?;
            }
            LeaseAction::BcastLeaseMsgs { peers, msg } => {
                self.transport_hub.bcast_lease_msg(
                    1, // gid 1 for quorum leases
                    lease_num,
                    msg,
                    Some(peers),
                )?;
            }

            LeaseAction::GrantRemoved { .. }
            | LeaseAction::GrantTimeout { .. } => {
                // promptly let leader know about my grant_set becoming empty
                // so that it may proceed with instances that stuck during
                // gathering AcceptReplies
                // NOTE: this is an equivalent way of handling leaseholder
                //       unresponsiveness as what's described in the paper;
                //       we just let client do configuration changes explicitly
                if self.qlease_num == lease_num
                    && self.qlease_manager.grant_set().count() == 0
                {
                    if let Some(leader) = self.leader {
                        if leader != self.id {
                            self.transport_hub.send_msg(
                                PeerMsg::NoGrants {
                                    ballot: self.bal_max_seen,
                                    qlease_num: lease_num,
                                },
                                leader,
                            )?;
                            pf_trace!(
                                "sent NoGrants -> {} for bal {} qlease_num {}",
                                leader,
                                self.bal_max_seen,
                                lease_num
                            );
                        } else {
                            self.handle_msg_no_grants(
                                self.id,
                                self.bal_max_seen,
                                lease_num,
                            )?;
                        }
                    }
                }

                // tell revoker that it might want to double check grant_set
                return Ok(true);
            }

            LeaseAction::HigherNumber => {
                if self.qlease_num < lease_num {
                    self.qlease_num = lease_num;
                }

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
