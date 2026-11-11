//! `Crossword` -- tolerated failures, and the commit and recovery rules.

use std::collections::HashMap;

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError};

/// Crash sets that committed data must survive: any `fault_tolerance`
/// servers, plus each failure domain (e.g., a rack) as a whole, and their
/// subsets. Kept as the maximal sets.
#[derive(Debug, Clone)]
pub(super) struct Tolerance {
    /// Total number of servers.
    pub(super) population: u8,

    /// Maximal tolerated crash sets.
    maximal: Vec<Bitmap>,
}

impl Tolerance {
    /// Creates the tolerance of any `fault_tolerance` servers and the failure
    /// domains in `domains` (e.g., "3,4/1,2" for two of them).
    pub(super) fn new(
        population: u8,
        fault_tolerance: u8,
        domains: &str,
    ) -> Result<Self, SummersetError> {
        let mut sets: Vec<Bitmap> = (0..1u32 << population)
            .filter(|m| m.count_ones() == u32::from(fault_tolerance))
            .map(|m| {
                Bitmap::from((
                    population,
                    (0..population)
                        .filter(|&s| (m >> s) & 1 == 1)
                        .collect::<Vec<_>>(),
                ))
            })
            .collect();
        for domain in domains.split('/').filter(|d| !d.trim().is_empty()) {
            let mut set = Bitmap::new(population, false);
            for s in domain.split(',') {
                set.set(s.trim().parse()?, true)?;
            }
            sets.push(set);
        }
        if sets.iter().any(|x| x.count() >= population) {
            return Err(SummersetError::msg(format!(
                "no server survives some tolerated failure: f {fault_tolerance} domains '{domains}'"
            )));
        }

        // keep only the maximal ones (the first of equal ones)
        let maximal = sets
            .iter()
            .enumerate()
            .filter(|&(i, x)| {
                !sets
                    .iter()
                    .enumerate()
                    .any(|(j, y)| j != i && y.is_superset(x) && (y != x || j < i))
            })
            .map(|(_, x)| x.clone())
            .collect();
        Ok(Tolerance {
            population,
            maximal,
        })
    }

    /// The commit rule: the shards held by `rows` (of replicas that have
    /// confirmed them durable) still number at least `d` after crossing out
    /// the rows of any tolerated failure.
    pub(super) fn covers(&self, rows: &HashMap<ReplicaId, Bitmap>, d: u8) -> bool {
        let Some(mut coverage) = rows.values().next().cloned() else {
            return false;
        };
        self.maximal.iter().all(|x| {
            coverage.clear();
            for (&s, row) in rows {
                if !x.get(s).unwrap_or(false) && coverage.union(row).is_err() {
                    return false;
                }
            }
            coverage.count() >= d
        })
    }

    /// Whether the replicas `heard` include all survivors of some tolerated
    /// failure. Recovery then has heard from every committed value with at
    /// least `d` of its shards, so it may take that as a Prepare quorum and
    /// conclude that a value with fewer shards never committed (the discard
    /// rule).
    pub(super) fn survived_by(&self, heard: &Bitmap) -> bool {
        self.maximal.iter().any(|x| {
            (0..self.population).all(|s| heard.get(s).unwrap_or(false) || x.get(s).unwrap_or(false))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(rows: &[(ReplicaId, &[u8])], w: u8) -> HashMap<ReplicaId, Bitmap> {
        rows.iter()
            .map(|&(s, shards)| (s, Bitmap::from((w, shards.to_vec()))))
            .collect()
    }

    #[test]
    fn any_minority_is_majority_quorums() -> Result<(), SummersetError> {
        let tol = Tolerance::new(5, 2, "")?;
        assert_eq!(tol.maximal.len(), 10);
        assert!(tol.survived_by(&Bitmap::from((5, vec![0, 3, 4]))));
        assert!(!tol.survived_by(&Bitmap::from((5, vec![0, 4]))));
        // full copies commit on any majority, one shard each of a (5, 3) code
        // on all five only
        assert!(tol.covers(&rows(&[(0, &[0]), (1, &[0]), (2, &[0])], 1), 1));
        assert!(!tol.covers(&rows(&[(0, &[0]), (1, &[0])], 1), 1));
        let thirds: Vec<(ReplicaId, &[u8])> = vec![(0, &[0]), (1, &[1]), (2, &[2]), (3, &[3])];
        assert!(!tol.covers(&rows(&thirds, 5), 3));
        let mut all = thirds;
        all.push((4, &[4]));
        assert!(tol.covers(&rows(&all, 5), 3));
        Ok(())
    }

    #[test]
    fn rack_domain() -> Result<(), SummersetError> {
        // any one server, or servers 3 and 4 together
        let tol = Tolerance::new(5, 1, "3,4")?;
        assert_eq!(tol.maximal.len(), 4);
        assert!(tol.survived_by(&Bitmap::from((5, vec![0, 1, 2]))));
        assert!(!tol.survived_by(&Bitmap::from((5, vec![0, 3, 4]))));
        // a full leader row and halves on 1 and 2 commit with three replies
        let halves = rows(&[(0, &[0, 1]), (1, &[0]), (2, &[1])], 2);
        assert!(tol.covers(&halves, 2));
        assert!(!Tolerance::new(5, 2, "")?.covers(&halves, 2));
        assert!(Tolerance::new(3, 1, "0,1,2").is_err());
        Ok(())
    }
}
