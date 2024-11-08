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
        avoid_fast_path: bool,
        leader_bk: &LeaderBookkeeping,
        population: u8,
        simple_quorum_cnt: u8,
        super_quorum_cnt: u8,
    ) -> Option<bool> {
        let all_cnt = leader_bk.pre_accept_acks.count();

        if avoid_fast_path {
            // don't consider fast path at all
            if all_cnt >= simple_quorum_cnt {
                return Some(false);
            } else {
                return None;
            }
        } else {
            // will consider fast path if eligible
            let mut repeats = HashMap::new();
            for reply in leader_bk.pre_accept_replies.values().cloned() {
                *repeats.entry(reply).or_insert(0) += 1;
            }
            let max_cnt = repeats.into_values().max().unwrap_or(0) as u8;
            debug_assert!(super_quorum_cnt <= population);
            debug_assert!(simple_quorum_cnt <= super_quorum_cnt);
            debug_assert!(all_cnt <= population);
            debug_assert!(max_cnt <= all_cnt);

            if max_cnt >= super_quorum_cnt {
                Some(true)
            } else if all_cnt >= simple_quorum_cnt
                && max_cnt + (population - all_cnt) < super_quorum_cnt
            {
                Some(false)
            } else {
                None
            }
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
        slot_row: ReplicaId,
        leader_bk: &LeaderBookkeeping,
        population: u8,
        simple_quorum_cnt: u8,
    ) -> Option<(Status, SeqNum, DepSet, ReqBatch)> {
        debug_assert!(simple_quorum_cnt <= population);
        if leader_bk.exp_prepare_acks.count() < simple_quorum_cnt {
            return None;
        }

        // has at least one Committed or Accepting or PreAccepting? records
        // an index into replies vec that meets each
        let (mut has_commit, mut has_accept, mut has_pre_accept) =
            (None, None, None);
        for (r, (status, _, _, _)) in leader_bk.exp_prepare_voteds.iter() {
            match status {
                Status::Committed => has_commit = Some(r),
                Status::Accepting => has_accept = Some(r),
                Status::PreAccepting => has_pre_accept = Some(r),
                _ => {}
            }
        }

        if let Some(r) = has_commit {
            let (_, seq, deps, reqs) = leader_bk.exp_prepare_voteds[r].clone();
            return Some((Status::Committed, seq, deps, reqs));
        }
        if let Some(r) = has_accept {
            let (_, seq, deps, reqs) = leader_bk.exp_prepare_voteds[r].clone();
            return Some((Status::Accepting, seq, deps, reqs));
        }

        // has at lease N/2 identical replies for row's default ballot and
        // none of those are from row peer itself
        let has_enough_identical = if leader_bk.exp_prepare_max_bal
            != Self::make_default_ballot(slot_row)
        {
            None
        } else {
            let voteds: Vec<(SeqNum, DepSet, ReqBatch)> = leader_bk
                .exp_prepare_voteds
                .clone()
                .into_iter()
                .filter_map(|(r, (status, seq, deps, reqs))| {
                    if r != slot_row && status == Status::PreAccepting {
                        Some((seq, deps, reqs))
                    } else {
                        None
                    }
                })
                .collect();
            if voteds.len() + 1 < simple_quorum_cnt as usize {
                None
            } else {
                Self::get_enough_identical(voteds, simple_quorum_cnt)
            }
        };

        if let Some((seq, deps, reqs)) = has_enough_identical {
            return Some((Status::Accepting, seq, deps, reqs));
        }
        if let Some(r) = has_pre_accept {
            let (_, seq, deps, reqs) = leader_bk.exp_prepare_voteds[r].clone();
            return Some((Status::PreAccepting, seq, deps, reqs));
        } else {
            return Some((
                Status::PreAccepting,
                1,
                DepSet::empty(population),
                ReqBatch::new(),
            ));
        }
    }

    /// Returns the instance state among filtered ExpPrepare replies that
    /// occurs at least N/2 times.
    fn get_enough_identical(
        mut voteds: Vec<(SeqNum, DepSet, ReqBatch)>,
        simple_quorum_cnt: u8,
    ) -> Option<(SeqNum, DepSet, ReqBatch)> {
        debug_assert!(voteds.len() >= simple_quorum_cnt as usize);
        let mut first = 0usize;
        let mut visited = vec![false; voteds.len()];
        visited[0] = true;

        while first < voteds.len() {
            let mut next_first = first;
            let mut same_cnt = 1;
            for i in (first + 1)..voteds.len() {
                if !visited[i] {
                    // not visited yet
                    if voteds[i] == voteds[first] {
                        visited[i] = true;
                        same_cnt += 1;
                    } else if next_first == first {
                        next_first = i;
                    }
                }
            }

            if same_cnt + 1 >= simple_quorum_cnt {
                return Some(voteds.remove(first));
            } else {
                first = next_first;
            }
        }

        None
    }
}
