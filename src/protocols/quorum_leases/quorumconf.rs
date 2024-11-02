//! QuorumLeases -- quorum leaser roles configuration maintenance.

use super::*;

use crate::server::LeaseNotice;

// QuorumLeasesReplica quorum leaser roles configuration logic
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
                // to apply; first ensure revocation of all the existing
                // quorum leases
                debug_assert!(self.qlease_ver < self.commit_bar);
                if self.qlease_cfg.is_grantor(self.id)? {
                    let peers = self.qlease_cfg.grantees.clone();
                    self.qlease_manager.add_notice(
                        self.qlease_num,
                        LeaseNotice::DoRevoke {
                            peers: Some(peers.clone()),
                        },
                    )?;
                    for (peer, flag) in peers.iter() {
                        if peer != self.id && flag {
                            self.ensure_qlease_revoked(peer).await?;
                        }
                    }
                }

                // then apply the new config
                if self.is_leader() {
                    pf_info!("qlease_cfg: v{} {:?}", self.commit_bar, conf);
                } else {
                    pf_debug!("qlease_cfg: v{} {:?}", self.commit_bar, conf);
                }
                self.control_hub.send_ctrl(CtrlMsg::LeaserStatus {
                    is_grantor: conf.grantors.get(self.id)?,
                    is_grantee: conf.grantees.get(self.id)?,
                })?;
                self.qlease_cfg = conf;
                self.qlease_ver = self.commit_bar;
                self.qlease_num = self.commit_bar as LeaseNum;

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
}
