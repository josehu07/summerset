//! Bodega -- local read optimization related actions.

use super::*;

impl BodegaReplica {
    /// Checks if I'm a stable leader as in a majority-leased config.
    /// FIXME: fixme
    #[inline]
    pub(super) fn is_stable_leader(&self) -> bool {
        // self.is_leader()
        //     && self.bal_prepared > 0
        //     && (true
        //         // [for benchmarking purposes only]
        //         || self.config.sim_read_lease)
        true
    }

    /// Checks if I'm a local reader as in a majority-leased config.
    /// FIXME: fixme
    #[inline]
    pub(super) fn is_local_reader(&self) -> Result<bool, SummersetError> {
        // Ok((self.bodega_cfg.is_grantee(self.id)?
        //         && self.lease_manager.lease_cnt() + 1 >= self.quorum_cnt)
        //     // [for benchmarking purposes only]
        //     || self.config.sim_read_lease)
        Ok(self.bodega_cfg.is_grantee(self.id)?
            // [for benchmarking purposes only]
            || self.config.sim_read_lease)
    }

    /// The commit condition check. Besides requiring an AcceptReply quorum
    /// size of at least majority, it also requires that replies from all
    /// lease grantees have been received.
    /// FIXME: fixme
    pub(super) fn commit_condition(
        leader_bk: &LeaderBookkeeping,
        quorum_cnt: u8,
        bodega_cfg: &LeaserRoles,
    ) -> Result<bool, SummersetError> {
        if leader_bk.accept_acks.count() < quorum_cnt {
            return Ok(false);
        }

        for (grantee, flag) in bodega_cfg.grantees.iter() {
            if flag && !leader_bk.accept_acks.get(grantee)? {
                return Ok(false);
            }
        }

        Ok(true)
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

    /// React to a CommitNotice message from leader.
    pub(super) fn heard_commit_notice(
        &mut self,
        peer: ReplicaId,
        ballot: Ballot,
        commit_bar: usize,
    ) -> Result<(), SummersetError> {
        if ballot == self.bal_max_seen {
            self.advance_commit_bar(peer, commit_bar)?;
        }

        Ok(())
    }
}
