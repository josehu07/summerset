//! `Crossword` -- reliability profiles of the servers, and the failures that
//! committed data must survive (with the commit and recovery rules).

use std::collections::HashMap;

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError};

/// Default probability above which crash sets are tolerated under `probs`.
const DEFAULT_PROBS_THRESHOLD: f64 = 1e-3;

/// What is known about how the servers fail, as mechanisms separated by ';':
///   - `domains:<ids>/<ids>/...`: failure domains (e.g., racks) that may fail
///     as a whole, with their server IDs comma-separated
///   - `probs:<p0>,<p1>,...[@<eps>]`: independent probability of each server
///     failing; sets of servers at least `eps` likely to all fail are
///     tolerated
///   - `runtime`: downtime observed at runtime
///
/// Domains and probabilities add crash sets that committed data must survive,
/// so all servers must be given the same ones; probabilities and runtime
/// downtime make assignments rely more on the more reliable servers.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct ReliabilityProfile {
    /// Failure domains that may fail as a whole.
    domains: Vec<Bitmap>,

    /// Independent failure probability of each server, and the probability
    /// above which crash sets are tolerated.
    probs: Option<(Vec<f64>, f64)>,

    /// Whether to estimate unreliability from downtime observed at runtime.
    runtime: bool,
}

impl ReliabilityProfile {
    /// Parses a profile of `population` servers from its string form.
    pub(super) fn parse(population: u8, profile: &str) -> Result<Self, SummersetError> {
        let mut parsed = Self::default();
        for mechanism in profile
            .split(';')
            .map(str::trim)
            .filter(|m| !m.is_empty() && *m != "none")
        {
            let (name, args) = mechanism.split_once(':').unwrap_or((mechanism, ""));
            match name.trim() {
                "domains" => {
                    for domain in args.split('/') {
                        let mut set = Bitmap::new(population, false);
                        for s in domain.split(',') {
                            set.set(s.trim().parse()?, true)?;
                        }
                        parsed.domains.push(set);
                    }
                }

                "probs" => {
                    let (probs, eps) = args.split_once('@').unwrap_or((args, ""));
                    let probs = probs
                        .split(',')
                        .map(|p| p.trim().parse::<f64>())
                        .collect::<Result<Vec<_>, _>>()?;
                    let eps = if eps.trim().is_empty() {
                        DEFAULT_PROBS_THRESHOLD
                    } else {
                        eps.trim().parse()?
                    };
                    if probs.len() != usize::from(population)
                        || probs.iter().any(|p| !(0.0..=1.0).contains(p))
                        || !(eps > 0.0 && eps <= 1.0)
                    {
                        return Err(SummersetError::msg(format!(
                            "invalid failure probabilities '{args}'"
                        )));
                    }
                    parsed.probs = Some((probs, eps));
                }

                "runtime" => parsed.runtime = true,

                _ => {
                    return Err(SummersetError::msg(format!(
                        "unknown reliability mechanism '{mechanism}'"
                    )));
                }
            }
        }
        Ok(parsed)
    }

    /// Whether it adds any crash sets to tolerate.
    pub(super) fn adds_crash_sets(&self) -> bool {
        !self.domains.is_empty() || self.probs.is_some()
    }

    /// Crash sets to tolerate on top of any `fault_tolerance` servers: the
    /// failure domains, and the sets of servers at least the threshold likely
    /// to all fail.
    pub(super) fn crash_sets(&self) -> Vec<Bitmap> {
        let mut sets = self.domains.clone();
        if let Some((probs, eps)) = &self.probs {
            // depth-first over servers in order, as adding a server to a set
            // only makes it less likely
            let population = u8::try_from(probs.len()).unwrap_or(u8::MAX);
            let mut stack: Vec<(u8, f64, Vec<u8>)> = vec![(0, 1.0, vec![])];
            while let Some((next, prob, members)) = stack.pop() {
                for s in next..population {
                    let prob = prob * probs[usize::from(s)];
                    if prob >= *eps {
                        let mut members = members.clone();
                        members.push(s);
                        sets.push(Bitmap::from((population, members.clone())));
                        stack.push((s + 1, prob, members));
                    }
                }
            }
        }
        sets
    }

    /// How likely `peer` seems to fail in the middle of an instance, given the
    /// fraction of recent time it was observed `down`.
    pub(super) fn unreliability(&self, peer: ReplicaId, down: f64) -> f64 {
        let given = self
            .probs
            .as_ref()
            .map_or(0.0, |(probs, _)| probs[usize::from(peer)]);
        if self.runtime { given.max(down) } else { given }
    }
}

/// Crash sets that committed data must survive: any `fault_tolerance`
/// servers, plus those of the reliability profile (e.g., a rack as a whole),
/// and their subsets. Kept as the maximal sets.
#[derive(Debug, Clone)]
pub(super) struct Tolerance {
    /// Total number of servers.
    pub(super) population: u8,

    /// List of maximal crash sets we need to tolerate.
    maximal_faults: Vec<Bitmap>,
}

impl Tolerance {
    /// Creates the tolerance of any `fault_tolerance` servers and the given
    /// extra crash sets.
    pub(super) fn new(
        population: u8,
        fault_tolerance: u8,
        extra: Vec<Bitmap>,
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
        sets.extend(extra);
        if sets.iter().any(|x| x.count() >= population) {
            return Err(SummersetError::msg(
                "no server survives some tolerated failure",
            ));
        }

        // keep only the maximal ones (the first of equal ones)
        let maximal_faults = sets
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
            maximal_faults,
        })
    }

    /// The commit rule: the shards held by `rows` (of replicas that have
    /// confirmed them durable) still number at least `d` after crossing out
    /// the rows of any tolerated failure.
    pub(super) fn covers(&self, rows: &HashMap<ReplicaId, Bitmap>, d: u8) -> bool {
        let Some(mut coverage) = rows.values().next().cloned() else {
            return false;
        };
        self.maximal_faults.iter().all(|x| {
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
        self.maximal_faults.iter().any(|x| {
            (0..self.population).all(|s| heard.get(s).unwrap_or(false) || x.get(s).unwrap_or(false))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mechanisms() -> Result<(), SummersetError> {
        let profile = ReliabilityProfile::parse(5, " domains:3,4/1,2 ; runtime ")?;
        assert_eq!(
            profile.crash_sets(),
            vec![Bitmap::from((5, vec![3, 4])), Bitmap::from((5, vec![1, 2]))]
        );
        assert!((profile.unreliability(3, 0.4) - 0.4).abs() < 1e-9);
        assert_eq!(
            ReliabilityProfile::parse(5, "none")?,
            ReliabilityProfile::default()
        );
        assert!(!ReliabilityProfile::parse(5, "runtime")?.adds_crash_sets());
        assert!(ReliabilityProfile::parse(5, "rumors").is_err());
        assert!(ReliabilityProfile::parse(5, "domains:3,7").is_err());
        assert!(ReliabilityProfile::parse(5, "probs:0.1,0.1").is_err());
        assert!(ReliabilityProfile::parse(3, "probs:0.1,0.1,1.5").is_err());
        Ok(())
    }

    #[test]
    fn likely_crash_sets() -> Result<(), SummersetError> {
        // a flaky pair among reliable servers: the pair, and any one of them
        // with a reliable one, are at least 1e-3 likely to fail together
        let profile = ReliabilityProfile::parse(5, "probs:0.01,0.01,0.01,0.3,0.3@1e-3")?;
        let sets = profile.crash_sets();
        let has = |ids: Vec<u8>| sets.contains(&Bitmap::from((5, ids)));
        assert!(has(vec![3, 4]) && has(vec![0, 3]) && has(vec![2, 4]));
        assert!(!has(vec![0, 1]) && !has(vec![0, 3, 4]));
        assert!((profile.unreliability(3, 0.9) - 0.3).abs() < 1e-9);
        Ok(())
    }

    fn rows(rows: &[(ReplicaId, &[u8])], w: u8) -> HashMap<ReplicaId, Bitmap> {
        rows.iter()
            .map(|&(s, shards)| (s, Bitmap::from((w, shards.to_vec()))))
            .collect()
    }

    #[test]
    fn any_minority_is_majority_quorums() -> Result<(), SummersetError> {
        let tol = Tolerance::new(5, 2, vec![])?;
        assert_eq!(tol.maximal_faults.len(), 10);
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
        let tol = Tolerance::new(5, 1, vec![Bitmap::from((5, vec![3, 4]))])?;
        assert_eq!(tol.maximal_faults.len(), 4);
        assert!(tol.survived_by(&Bitmap::from((5, vec![0, 1, 2]))));
        assert!(!tol.survived_by(&Bitmap::from((5, vec![0, 3, 4]))));
        // a full leader row and halves on 1 and 2 commit with three replies
        let halves = rows(&[(0, &[0, 1]), (1, &[0]), (2, &[1])], 2);
        assert!(tol.covers(&halves, 2));
        assert!(!Tolerance::new(5, 2, vec![])?.covers(&halves, 2));
        assert!(Tolerance::new(3, 1, vec![Bitmap::new(3, true)]).is_err());
        Ok(())
    }
}
