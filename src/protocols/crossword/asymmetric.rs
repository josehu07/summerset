//! `Crossword` -- asymmetric assignments with per-instance code schemes.

use std::collections::HashMap;

use super::tolerance::Tolerance;
use crate::server::ReplicaId;
use crate::utils::Bitmap;

/// Candidate numbers of data shards for asymmetric assignments.
const DATA_SHARDS_CHOICES: [u8; 11] = [1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 16];

/// An assignment with its code scheme, and its predicted commit time.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Assignment {
    /// Number of data shards.
    pub(super) d: u8,

    /// Total number of shards.
    pub(super) w: u8,

    /// Shards each replica is assigned to persist.
    pub(super) rows: Vec<Bitmap>,

    /// Predicted commit time in millisecs.
    pub(super) time: f64,
}

/// Lays out rows of the given numbers of shards (out of `d`) per replica:
/// full rows get the data shards, partial ones disjoint shards in order (of
/// `w` in total if given, wrapping around then).
fn layout_rows(d: u8, loads: &[u8], w: Option<u8>) -> (u8, Vec<Bitmap>) {
    let partial: usize = loads
        .iter()
        .filter(|&&k| k < d)
        .map(|&k| usize::from(k))
        .sum();
    let w = w.unwrap_or_else(|| u8::try_from(partial.max(usize::from(d))).unwrap_or(u8::MAX));
    let mut offset = 0;
    let rows = loads
        .iter()
        .map(|&k| {
            if k >= d {
                Bitmap::from((w, 0..d))
            } else {
                let shards = (offset..offset + usize::from(k))
                    .filter_map(|i| u8::try_from(i % usize::from(w)).ok())
                    .collect::<Vec<u8>>();
                offset += usize::from(k);
                Bitmap::from((w, shards))
            }
        })
        .collect();
    (w, rows)
}

/// Lays out the rows of the given loads, and checks whether replies from
/// those with `Some` load (possibly zero) commit: there are at least `quorum`
/// of them, and they pass the commit rule.
fn layout_commits(
    tolerance: &Tolerance,
    quorum: u8,
    d: u8,
    loads: &[Option<u8>],
    w: Option<u8>,
) -> (u8, Vec<Bitmap>, bool) {
    let sizes: Vec<u8> = loads.iter().map(|k| k.unwrap_or(0)).collect();
    let (w, rows) = layout_rows(d, &sizes, w);
    let acks: HashMap<ReplicaId, Bitmap> = (0..)
        .zip(&rows)
        .zip(loads)
        .filter(|(_, k)| k.is_some())
        .map(|((s, row), _)| (s, row.clone()))
        .collect();
    let ok = acks.len() >= usize::from(quorum) && tolerance.covers(&acks, d);
    (w, rows, ok)
}

/// Picks the assignment predicted to commit the soonest for a value of
/// `size` bytes, keeping the code scheme `(d, w)` if given (for a value
/// already encoded). Commits need replies from at least `quorum` replicas
/// that pass the commit rule. The leader `me` persists all data shards, and
/// `reply_time(peer, bytes)` predicts when a peer would reply to an Accept
/// carrying that many bytes, `None` if it is not expected to.
///
/// For each number of data shards, the smallest time by which the replies
/// commit is found by giving every peer as many shards as it can persist by
/// then; loads are then trimmed while still committing, to send fewer bytes.
/// Ties in time go to fewer replies carrying shards needed (sparing waits for
/// stragglers), then to fewer bytes.
pub(super) fn pick_assignment(
    me: ReplicaId,
    tolerance: &Tolerance,
    quorum: u8,
    reply_time: impl Fn(ReplicaId, usize) -> Option<f64>,
    size: usize,
    max_data_shards: u8,
    scheme: Option<(u8, u8)>,
) -> Option<Assignment> {
    let choices: Vec<u8> = scheme.map_or_else(
        || {
            DATA_SHARDS_CHOICES
                .iter()
                .copied()
                .filter(|&d| d <= max_data_shards)
                .collect()
        },
        |(d, _)| vec![d],
    );
    let fixed_w = scheme.map(|(_, w)| w);
    let population = tolerance.population;

    let mut best: Option<(Assignment, (usize, usize))> = None;
    for d in choices {
        let shard_len = size.div_ceil(usize::from(d)).max(1);
        // times[peer][k]: predicted reply time with k shards
        let times: Vec<Vec<Option<f64>>> = (0..population)
            .map(|p| {
                (0..=d)
                    .map(|k| {
                        if p == me {
                            Some(0.0)
                        } else {
                            reply_time(p, usize::from(k) * shard_len).filter(|t| t.is_finite())
                        }
                    })
                    .collect()
            })
            .collect();
        let mut candidates: Vec<f64> = times.iter().flatten().filter_map(|&t| t).collect();
        candidates.sort_by(f64::total_cmp);
        candidates.dedup();

        // the most shards each replica can persist by a time, if any
        let loads_by = |time: f64| -> Vec<Option<u8>> {
            (0..population)
                .map(|p| {
                    if p == me {
                        return Some(d);
                    }
                    let k = times[p as usize]
                        .iter()
                        .take_while(|t| t.is_some_and(|t| t <= time))
                        .count();
                    k.checked_sub(1).and_then(|k| u8::try_from(k).ok())
                })
                .collect()
        };
        let Some(&time) = candidates
            .iter()
            .find(|&&t| layout_commits(tolerance, quorum, d, &loads_by(t), fixed_w).2)
        else {
            continue;
        };

        // trim loads, slowest peers first, while still committing
        let mut loads = loads_by(time);
        let mut order: Vec<ReplicaId> = (0..population).filter(|&p| p != me).collect();
        order.sort_by(|&a, &b| {
            let ta = times[a as usize][usize::from(d)].unwrap_or(f64::INFINITY);
            let tb = times[b as usize][usize::from(d)].unwrap_or(f64::INFINITY);
            tb.total_cmp(&ta)
        });
        let mut trimmed = true;
        while trimmed {
            trimmed = false;
            for &p in &order {
                let Some(k) = loads[p as usize].filter(|&k| k > 0) else {
                    continue;
                };
                loads[p as usize] = Some(k - 1);
                if layout_commits(tolerance, quorum, d, &loads, fixed_w).2 {
                    trimmed = true;
                } else {
                    loads[p as usize] = Some(k);
                }
            }
        }

        let (w, rows, _) = layout_commits(tolerance, quorum, d, &loads, fixed_w);
        let sizes = (0..population)
            .filter(|&p| p != me)
            .filter_map(|p| loads[p as usize].filter(|&k| k > 0));
        let cost = (
            sizes.clone().count(),
            sizes.map(|k| usize::from(k) * shard_len).sum(),
        );
        let better = best.as_ref().is_none_or(|(b, b_cost)| {
            time < b.time * 0.99 - 0.01 || (time <= b.time * 1.01 + 0.01 && cost < *b_cost)
        });
        if better {
            best = Some((Assignment { d, w, rows, time }, cost));
        }
    }
    best.map(|(assignment, _)| assignment)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reply time in ms of `bytes` over a link of `gbps`, with 1 ms delays.
    fn link_time(bytes: usize, gbps: f64) -> f64 {
        1.0 + f64::from(u32::try_from(bytes).unwrap()) * 8.0 / (gbps * 1e6)
    }

    /// Reply times over links of the given Gbps.
    fn over_links(gbps: Vec<f64>) -> impl Fn(ReplicaId, usize) -> Option<f64> {
        move |p, bytes| Some(link_time(bytes, gbps[p as usize]))
    }

    fn loads(a: &Assignment) -> Vec<u8> {
        a.rows.iter().map(Bitmap::count).collect()
    }

    #[test]
    fn small_values_go_full_copies() {
        let tol = Tolerance::new(5, 2, "").unwrap();
        let reply = over_links(vec![1.0; 5]);
        let a = pick_assignment(0, &tol, 3, reply, 100, 16, None).unwrap();
        assert_eq!(a.d, 1);
        assert_eq!(loads(&a).iter().map(|&k| usize::from(k)).sum::<usize>(), 3);
    }

    #[test]
    fn uneven_links_get_uneven_rows() {
        // links of 1, 1, 0.4, 0.1 Gbps: faster peers persist more, and the
        // commit beats that of full copies (on the two 1 Gbps links)
        let tol = Tolerance::new(5, 2, "").unwrap();
        let reply = over_links(vec![1.0, 1.0, 1.0, 0.4, 0.1]);
        let size = 8 << 20;
        let a = pick_assignment(0, &tol, 3, &reply, size, 16, None).unwrap();
        let full_copies = reply(1, size).unwrap();
        assert!(a.time < 0.75 * full_copies, "{a:?} vs {full_copies}");
        let l = loads(&a);
        assert!(l[1] >= l[3] && l[3] >= l[4], "{l:?}");
    }

    #[test]
    fn rack_domain_spares_its_peers() {
        // tolerating any one server or the rack of 3 and 4, halves on 1 and
        // 2 commit without the rack
        let tol = Tolerance::new(5, 1, "3,4").unwrap();
        let reply = |p: ReplicaId, bytes: usize| {
            let t = link_time(bytes, 1.0);
            Some(if p >= 3 { t + 500.0 } else { t })
        };
        let a = pick_assignment(0, &tol, 3, reply, 8 << 20, 16, None).unwrap();
        let l = loads(&a);
        assert_eq!((l[3], l[4]), (0, 0), "{a:?}");
        assert_eq!(l[1] + l[2], a.d, "{a:?}");
    }

    #[test]
    fn waits_for_a_majority() {
        // tolerating any one failure, a full copy on 1 alone passes the
        // commit rule, but commits wait for a majority of (maybe empty) replies
        let tol = Tolerance::new(5, 1, "").unwrap();
        let reply = |p: ReplicaId, bytes: usize| {
            Some(link_time(bytes, 1.0) + if p >= 2 { 50.0 } else { 0.0 })
        };
        let size = 1 << 20;
        assert!(
            pick_assignment(0, &tol, 2, reply, size, 16, None)
                .unwrap()
                .time
                < 50.0
        );
        let a = pick_assignment(0, &tol, 3, reply, size, 16, None).unwrap();
        assert!(a.time >= 50.0, "{a:?}");
    }

    #[test]
    fn keeps_given_scheme() {
        let tol = Tolerance::new(5, 2, "").unwrap();
        let reply = over_links(vec![1.0; 5]);
        let a = pick_assignment(0, &tol, 3, reply, 1 << 20, 16, Some((3, 5))).unwrap();
        assert_eq!((a.d, a.w), (3, 5));
        assert!(a.rows.iter().all(|r| r.size() == 5));
        // none if the commit rule cannot pass, e.g., with too few replying
        let tol = Tolerance::new(5, 2, "").unwrap();
        let few = |p: ReplicaId, _| (p < 2).then_some(1.0);
        assert!(pick_assignment(0, &tol, 3, few, 1 << 20, 16, None).is_none());
    }
}
