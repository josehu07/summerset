//! QuorumLeases -- quorum leaseholder roles configuration maintenance.

use super::*;

use crate::server::LeaseNotice;

// QuorumLeasesReplica quorum leaseholder roles configuration logic
impl QuorumLeasesReplica {
    /// Convenience method for getting the (only) leaseholders bitmap.
    #[inline]
    pub(super) fn qlease_grantees(&self) -> &Bitmap {
        self.qlease_conf.get_responders_by_idx(&()).unwrap()
    }

    /// Checks if a configuration change delta is valid.
    // NOTE: currently only supports conf change that overwrites the full
    //       range of keys
    pub(super) fn is_valid_delta(delta: &ConfChange, population: u8) -> bool {
        if delta.reset {
            pf_warn!("explicit conf reset not supported yet");
            return false;
        }

        if let Some(leader) = delta.leader {
            if leader >= population {
                return false;
            }
        }

        if let Some(responders) = &delta.responders {
            if let Some((_, _)) = &delta.range {
                pf_warn!("key-ranged conf change not supported yet");
                false
            } else {
                responders.size() == population
            }
        } else {
            true
        }
    }

    /// Applies a valid configuration change delta.
    pub(super) fn apply_conf_delta(
        delta: ConfChange,
        qlease_conf: &mut RespondersConf,
    ) -> Result<(), SummersetError> {
        debug_assert!(!delta.reset);
        if let Some(leader) = delta.leader {
            qlease_conf.set_leader(leader);
        }
        if let Some(responders) = delta.responders {
            // NOTE: currently only supports conf change that overwrites the
            //       full range of keys
            qlease_conf.set_responders(None, responders, Some(()))?;
        }
        Ok(())
    }

    /// Processes a batch of leaseholder roles config change requests (taken off
    /// from a committed instance's request batch). Replies to all of them, and
    /// applies the last valid one as current config.
    pub(super) async fn commit_conf_changes(
        &mut self,
        slot: usize,
        external: bool,
        conf_changes: Vec<(ClientId, RequestId, ConfChange)>,
    ) -> Result<(), SummersetError> {
        debug_assert!(!conf_changes.is_empty());

        // find the last valid one, if any
        let mut apply_idx = conf_changes.len();
        for (idx, (_, _, delta)) in conf_changes.iter().enumerate().rev() {
            if Self::is_valid_delta(delta, self.population) {
                apply_idx = idx;
                break;
            }
        }

        // reply to all of them
        for (idx, (client, req_id, delta)) in
            conf_changes.into_iter().enumerate()
        {
            if idx != apply_idx {
                // not applied; directly reply to client as ignored
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
                debug_assert!(self.commit_bar > self.qlease_ver as usize);
                let peers = self.qlease_grantees().clone();
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

                // then apply the new config (currently assuming bluntly
                // applying to full range)
                Self::apply_conf_delta(delta, &mut self.qlease_conf)?;
                self.qlease_ver = self.commit_bar as ConfNum;
                self.qlease_num = self.commit_bar as LeaseNum;

                if self.is_leader() {
                    pf_info!(
                        "qlease_cfg: v{} {:?}",
                        self.commit_bar,
                        self.qlease_conf
                    );
                } else {
                    pf_debug!(
                        "qlease_cfg: v{} {:?}",
                        self.commit_bar,
                        self.qlease_conf
                    );
                }
                self.control_hub.send_ctrl(CtrlMsg::RespondersConf {
                    conf_num: self.qlease_ver,
                    new_conf: self.qlease_conf.clone(),
                })?;

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
}
