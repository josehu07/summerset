//! QuorumLeases -- lease-related operations.

use super::*;

use crate::server::{LeaseAction, LeaseNotice, LeaseNum};

// QuorumLeasesReplica lease-related actions logic
impl QuorumLeasesReplica {
    /// Update the highest_slot tracking info given a new request batch about
    /// to be saved into a slot.
    pub(super) fn refresh_highest_slot(
        slot: usize,
        reqs: &ReqBatch,
        highest_slot: &mut HashMap<String, usize>,
    ) {
        for (_, req) in reqs {
            if let ApiRequest::Req {
                cmd: Command::Put { key, .. },
                ..
            } = req
            {
                if let Some(highest_slot) = highest_slot.get_mut(key) {
                    *highest_slot = slot.max(*highest_slot);
                } else {
                    highest_slot.insert(key.clone(), slot);
                }
            }
        }
    }

    /// Get the value at the highest slot index ever seen for a key and its
    /// current commit status.
    // NOTE: current implementation might loop through requests in the batch of
    //       that slot at each inspect call; good enough but can be improved
    pub(super) fn inspect_highest_slot(
        &self,
        key: &String,
    ) -> Result<Option<(usize, Option<String>)>, SummersetError> {
        if let Some(&slot) = self.highest_slot.get(key) {
            if slot < self.start_slot
                || slot >= self.start_slot + self.insts.len()
            {
                // slot has been GCed or not locatable for some reason; we play
                // safe and return with not-committed status
                Ok(Some((slot, None)))
            } else {
                let inst = &self.insts[slot - self.start_slot];
                if inst.status < Status::Committed {
                    // instance not committed on me yet
                    Ok(Some((slot, None)))
                } else {
                    // instance committed, return the latest value for the key
                    // in batch
                    for (_, req) in inst.reqs.iter().rev() {
                        if let ApiRequest::Req {
                            cmd: Command::Put { key: k, value },
                            ..
                        } = req
                        {
                            if k == key {
                                return Ok(Some((slot, Some(value.clone()))));
                            }
                        }
                    }
                    logged_err!(
                        "no write to key '{}' at slot {}; wrong highest_slot",
                        key,
                        slot
                    )
                }
            }
        } else {
            // never seen this key
            Ok(None)
        }
    }

    /// Checks if a leaser roles configuration is valid.
    #[inline]
    pub(super) fn is_valid_conf(&self, conf: &LeaserRoles) -> bool {
        conf.grantors.size() == self.population
            && conf.grantees.size() == self.population
            && conf.grantors.count() >= self.quorum_cnt
    }

    /// Checks if I'm a majority-leased local reader.
    #[inline]
    pub(super) fn is_local_reader(&self) -> Result<bool, SummersetError> {
        Ok((self.leasers_cfg.is_grantee(self.id)?
                && self.lease_manager.lease_cnt() + 1 >= self.quorum_cnt)
            // [for benchmarking purposes only]
            || self.config.sim_read_lease)
    }

    /// Processes a batch of leaser roles config change requests (taken off
    /// from a committed instance's request batch). Replies to all of them, and
    /// applies the last valid one as current config.
    pub(super) async fn commit_conf_changes(
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
                // to apply, revoke old leases if I was granting any
                debug_assert!(self.leasers_ver < self.commit_bar);
                if self.leasers_cfg.is_grantor(self.id)? {
                    let peers = self.leasers_cfg.grantees.clone();
                    self.lease_manager.add_notice(
                        self.bal_max_seen,
                        LeaseNotice::DoRevoke {
                            peers: Some(peers.clone()),
                        },
                    )?;
                    for (peer, flag) in peers.iter() {
                        if peer != self.id && flag {
                            self.ensure_lease_revoked(peer).await?;
                        }
                    }
                }

                // then apply new config
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

                // initiate granting if I'm a grantor in committed config
                if self.leasers_cfg.is_grantor(self.id)? {
                    let mut peers = self.leasers_cfg.grantees.clone();
                    peers.set(self.id, false)?;
                    self.lease_manager.add_notice(
                        self.leasers_ver as LeaseNum,
                        LeaseNotice::NewGrants { peers: Some(peers) },
                    )?;
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
