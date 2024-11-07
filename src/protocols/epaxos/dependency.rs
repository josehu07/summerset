//! EPaxos -- dependencies set helpers.

use super::*;

impl fmt::Display for DepSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        let mut first_idx = true;
        for c in &self.0 {
            if !first_idx {
                write!(f, ",")?;
            } else {
                first_idx = false;
            }
            if let Some(c) = c {
                write!(f, "{}", c)?;
            } else {
                write!(f, "_")?;
            }
        }
        write!(f, "]")
    }
}

impl ops::Index<ReplicaId> for DepSet {
    type Output = Option<usize>;

    fn index(&self, idx: ReplicaId) -> &Self::Output {
        &self.0[idx as usize]
    }
}

impl ops::IndexMut<ReplicaId> for DepSet {
    fn index_mut(&mut self, idx: ReplicaId) -> &mut Self::Output {
        &mut self.0[idx as usize]
    }
}

impl ops::Index<usize> for DepSet {
    type Output = Option<usize>;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.0[idx]
    }
}

impl ops::IndexMut<usize> for DepSet {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.0[idx]
    }
}

impl DepSet {
    /// Creates an empty dependency set given population.
    #[inline]
    pub(super) fn empty(population: u8) -> Self {
        DepSet(vec![None; population as usize])
    }

    /// Dummy wrapper of the `.len()` method of inner `Vec`.
    #[inline]
    pub(super) fn len(&self) -> u8 {
        self.0.len() as u8
    }

    /// Dummy wrapper of the `.iter()` method of inner `Vec`.
    #[inline]
    pub(super) fn iter(&self) -> slice::Iter<Option<usize>> {
        self.0.iter()
    }

    /// Updates me to be the "union" with another dependencies set (by taking
    /// the max column index of each row).
    #[inline]
    pub(super) fn union(&mut self, other: &DepSet) {
        for (s, o) in self.0.iter_mut().zip(other.iter()) {
            if let Some(s) = s {
                if let Some(o) = o {
                    *s = (*s).max(*o);
                }
            } else {
                *s = *o;
            }
        }
    }
}

// EPaxosReplica dependencies set helpers
impl EPaxosReplica {
    /// Computes the maximum sequence number of a dependencies set.
    pub(super) fn max_seq_num(&self, deps: &DepSet) -> SeqNum {
        debug_assert_eq!(deps.len(), self.population);
        deps.iter()
            .enumerate()
            .filter_map(|(r, c)| {
                if let Some(c) = c {
                    Some(self.insts[r][*c].seq)
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0)
    }

    /// Identifies the dependencies set of a request batch based my current
    /// instance space state.
    pub(super) fn identify_deps(
        reqs: &ReqBatch,
        population: u8,
        highest_cols: &HashMap<String, DepSet>,
    ) -> DepSet {
        let mut deps = DepSet::empty(population);
        for (_, req) in reqs {
            if let ApiRequest::Req {
                cmd: Command::Put { key, .. },
                ..
            } = req
            {
                if let Some(cols) = highest_cols.get(key) {
                    deps.union(cols);
                }
            }
        }
        deps
    }

    /// Updates the highest_cols tracking info given a new request batch about
    /// to be saved into a slot.
    pub(super) fn refresh_highest_cols(
        slot: SlotIdx,
        reqs: &ReqBatch,
        population: u8,
        highest_cols: &mut HashMap<String, DepSet>,
    ) {
        let (row, col) = slot.unpack();
        for (_, req) in reqs {
            if let ApiRequest::Req {
                cmd: Command::Put { key, .. },
                ..
            } = req
            {
                if let Some(highest_cols) = highest_cols.get_mut(key) {
                    let highest_col = &mut highest_cols[row];
                    if let Some(hc) = highest_col {
                        *hc = (*hc).max(col);
                    } else {
                        *highest_col = Some(col);
                    }
                } else {
                    let mut cols = DepSet::empty(population);
                    cols[row] = Some(col);
                    highest_cols.insert(key.clone(), cols);
                }
            }
        }
    }

    /// Checks the fast-path quorum eligibility for a set of received
    /// PreAccept replies. Returns:
    ///   - `None` if can't decide yet
    ///   - `Some(true)` if conflict-free fast quorum formed
    ///   - `Some(false)` if fast quorum impossible
    pub(super) fn fast_quorum_eligibility(
        leader_bk: &LeaderBookkeeping,
        population: u8,
        super_quorum_cnt: u8,
    ) -> Option<bool> {
        let mut repeats = HashMap::new();
        for reply in leader_bk.pre_accept_replies.iter().cloned() {
            *repeats.entry(reply).or_insert(0) += 1;
        }
        let max_cnt = repeats.into_values().max().unwrap_or(0) as u8;
        let all_cnt = leader_bk.pre_accept_acks.count();
        debug_assert!(super_quorum_cnt <= population);
        debug_assert!(all_cnt <= population);
        debug_assert!(max_cnt <= all_cnt);

        if max_cnt >= super_quorum_cnt {
            Some(true)
        } else if max_cnt + (population - all_cnt) < super_quorum_cnt {
            Some(false)
        } else {
            None
        }
    }

    /// Checks the set of highest-ballot ExpPrepare replies and returns the
    /// proper next phase to run. Returns `None` if can't decide yet, otherwise
    /// returns:
    ///   - `Status::Committed` if can commit
    ///   - `Status::Accepting` if need slow-path Accept
    ///   - `Status::PreAccepting` if need to start over from PreAccept
    /// Also returns the instance state to feed into the next phase.
    pub(super) fn exp_prepare_next_step(
        row: ReplicaId,
        leader_bk: &LeaderBookkeeping,
        simple_quorum_cnt: u8,
    ) -> Option<(Status, SeqNum, DepSet, ReqBatch)> {
        if leader_bk.exp_prepare_acks.count() < simple_quorum_cnt {
            return None;
        }

        None
    }
}
