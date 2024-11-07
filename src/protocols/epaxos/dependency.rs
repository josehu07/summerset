//! EPaxos -- dependencies set helpers.

use super::*;

impl fmt::Display for DepSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        let mut first_idx = true;
        for c in &self.0 {
            if !first_idx {
                write!(f, ", ")?;
            } else {
                first_idx = false;
            }
            if let Some(c) = c {
                write!(f, "{}", c)?;
            } else {
                write!(f, "-")?;
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
    /// Update the highest_cols tracking info given a new request batch about
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

    /// Identify the dependencies set of a request batch based my current
    /// instance space state.
    pub(super) fn identify_deps(&self, req_batch: &ReqBatch) -> DepSet {
        let mut deps = DepSet::empty(self.population);
        for (_, req) in req_batch {
            if let ApiRequest::Req {
                cmd: Command::Put { key, .. },
                ..
            } = req
            {
                if let Some(cols) = self.highest_cols.get(key) {
                    deps.union(cols);
                }
            }
        }
        deps
    }

    /// Compute the maximum sequence number of a dependencies set.
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
}
