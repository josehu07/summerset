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

impl From<Vec<usize>> for DepSet {
    fn from(v: Vec<usize>) -> Self {
        DepSet(v.into_iter().map(|c| Some(c)).collect())
    }
}

// Allow indexing using u8 ReplicaId:
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

// Allow indexing using normal usize:
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
    /// Computes the maximum sequence number of a dependencies set, 0 if empty.
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
            match req {
                ApiRequest::Req {
                    cmd: Command::Put { key, .. },
                    ..
                } => {
                    if let Some(cols) = highest_cols.get(key) {
                        deps.union(cols);
                    }
                }
                ApiRequest::Req {
                    cmd: Command::Get { key },
                    ..
                } => {
                    if let Some(cols) = highest_cols.get(key) {
                        deps.union(cols);
                    }
                }
                _ => {}
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
    /// PreAccept replies. Returns `None` if can't decide yet, otherwise
    /// returns:
    ///   - `Status::Committed` if conflict-free fast quorum formed
    ///   - `Status::Accepting` if fast quorum already impossible
    /// Also returns the instance state to feed into the next phase.
    pub(super) fn fast_quorum_eligibility(
        avoid_fast_path: bool,
        leader_bk: &LeaderBookkeeping,
        population: u8,
        simple_quorum_cnt: u8,
        super_quorum_cnt: u8,
    ) -> Option<(Status, SeqNum, DepSet)> {
        let all_cnt = leader_bk.pre_accept_acks.count();
        debug_assert_ne!(simple_quorum_cnt, 0);
        debug_assert!(simple_quorum_cnt <= super_quorum_cnt);
        debug_assert!(super_quorum_cnt <= population);
        debug_assert!(all_cnt <= population);
        if all_cnt < simple_quorum_cnt {
            // can't decide anything yet
            return None;
        }

        if avoid_fast_path {
            // don't consider fast path at all
            // need slow-path Accept, take union of deps and max of seqs
            let (mut seq, mut deps) = (0, DepSet::empty(population));
            for (rseq, rdeps) in leader_bk.pre_accept_replies.values() {
                deps.union(rdeps);
                seq = seq.max(*rseq);
            }
            return Some((Status::Accepting, seq, deps));
        } else {
            // will consider fast path if eligible
            let (max_seq_deps, max_cnt) = Self::get_enough_identical(
                leader_bk.pre_accept_replies.values().cloned().collect(),
                super_quorum_cnt,
            );

            if let Some((seq, deps)) = max_seq_deps {
                // can commit on fast path, return the state corresponding to
                // the max_cnt
                debug_assert!(max_cnt >= super_quorum_cnt);
                Some((Status::Committed, seq, deps))
            } else if max_cnt + (population - all_cnt) < super_quorum_cnt {
                // need slow-path Accept, take union of deps and max of seqs
                let (mut seq, mut deps) = (0, DepSet::empty(population));
                for (rseq, rdeps) in leader_bk.pre_accept_replies.values() {
                    deps.union(rdeps);
                    seq = seq.max(*rseq);
                }
                Some((Status::Accepting, seq, deps))
            } else {
                // can't decide fast path eligibility yet; wait for more
                None
            }
        }
    }

    /// Checks the set of highest-ballot ExpPrepare replies and returns the
    /// proper next phase to run. Returns `None` if can't decide yet, otherwise
    /// returns:
    ///   - `Status::Committed` if can commit
    ///   - `Status::Accepting` if need a round of Accept
    ///   - `Status::PreAccepting` if need to start over from PreAccept
    /// Also returns the instance state to feed into the next phase.
    pub(super) fn exp_prepare_next_step(
        slot_row: ReplicaId,
        leader_bk: &LeaderBookkeeping,
        population: u8,
        simple_quorum_cnt: u8,
    ) -> Option<(Status, SeqNum, DepSet, ReqBatch)> {
        debug_assert_ne!(simple_quorum_cnt, 0);
        debug_assert!(simple_quorum_cnt <= population);
        if leader_bk.exp_prepare_acks.count() < simple_quorum_cnt {
            // can't decide yet
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
            // can commit
            let (_, seq, deps, reqs) = leader_bk.exp_prepare_voteds[r].clone();
            return Some((Status::Committed, seq, deps, reqs));
        }
        if let Some(r) = has_accept {
            // need a round of Accept
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
            if voteds.len() < (simple_quorum_cnt - 1) as usize {
                None
            } else {
                Self::get_enough_identical(voteds, simple_quorum_cnt).0
            }
        };

        if let Some((seq, deps, reqs)) = has_enough_identical {
            // need a round of Accept
            return Some((Status::Accepting, seq, deps, reqs));
        }
        if let Some(r) = has_pre_accept {
            // need to start over from PreAccept
            let (_, seq, deps, reqs) = leader_bk.exp_prepare_voteds[r].clone();
            return Some((Status::PreAccepting, seq, deps, reqs));
        } else {
            // use no-op at this instance
            return Some((
                Status::PreAccepting,
                1,
                DepSet::empty(population),
                ReqBatch::new(),
            ));
        }
    }

    /// Returns an element in the input vec that occurs at least a given number
    /// of times, along with the number of times it occurs. If no element meets
    /// the threshold, returns `None` but with the max number of occurrences
    /// across elements.
    fn get_enough_identical<T: Eq>(
        mut v: Vec<T>,
        thresh: u8,
    ) -> (Option<T>, u8) {
        debug_assert_ne!(thresh, 0);

        let mut first = 0;
        let mut visited = vec![false; v.len()];
        visited[0] = true;
        let mut max_cnt = 1;

        while first < v.len() {
            let mut next_first = first;
            let mut same_cnt = 1;
            for i in (first + 1)..v.len() {
                if !visited[i] {
                    // not visited yet
                    if v[i] == v[first] {
                        visited[i] = true;
                        same_cnt += 1;
                    } else if next_first == first {
                        next_first = i;
                    }
                }
            }

            if same_cnt >= thresh {
                return (Some(v.remove(first)), same_cnt);
            }
            first = next_first;
            max_cnt = max_cnt.max(same_cnt);
        }

        (None, max_cnt)
    }
}
