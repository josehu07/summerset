//! `Crossword` -- general (possibly uneven) assignments of the shards of a
//! per-instance code scheme to replicas.

use std::collections::HashMap;

use super::reliability::Tolerance;
use crate::server::ReplicaId;
use crate::utils::Bitmap;

/// Number of data shards of the default code scheme: fine enough for rows to
/// follow uneven replicas, and few enough for cheap coding.
pub(super) const DEFAULT_DATA_SHARDS: u8 = 16;

/// Total number of shards to lay rows out in: exactly that of an existing
/// code scheme, or at most a bound for a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TotalShards {
    Exactly(u8),
    AtMost(u8),
}

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
/// full rows get the data shards, partial ones disjoint shards in order,
/// wrapping around once all `total` shards are used.
fn layout_rows(d: u8, loads: &[u8], total: TotalShards) -> (u8, Vec<Bitmap>) {
    let partial: usize = loads
        .iter()
        .filter(|&&k| k < d)
        .map(|&k| usize::from(k))
        .sum();
    let w = match total {
        TotalShards::Exactly(w) => w,
        TotalShards::AtMost(max) => {
            u8::try_from(partial.max(usize::from(d))).map_or(max, |w| w.min(max))
        }
    };
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
    total: TotalShards,
) -> (u8, Vec<Bitmap>, bool) {
    let sizes: Vec<u8> = loads.iter().map(|k| k.unwrap_or(0)).collect();
    let (w, rows) = layout_rows(d, &sizes, total);
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
/// `size` bytes under a code scheme of `d` data shards and `total` shards.
/// Commits need replies from at least `quorum` replicas that pass the commit
/// rule. The leader `me` persists all data shards, and `reply_time(peer,
/// bytes)` predicts when a peer would reply to an Accept carrying that many
/// bytes, `None` if it is not expected to.
///
/// The smallest time by which the replies commit is found by giving every
/// peer as many shards as it can persist by then; loads are then trimmed,
/// slowest peers first, while still committing, to send fewer bytes.
pub(super) fn pick_assignment(
    me: ReplicaId,
    tolerance: &Tolerance,
    quorum: u8,
    reply_time: impl Fn(ReplicaId, usize) -> Option<f64>,
    size: usize,
    d: u8,
    total: TotalShards,
) -> Option<Assignment> {
    let population = tolerance.population;
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
    let &time = candidates
        .iter()
        .find(|&&t| layout_commits(tolerance, quorum, d, &loads_by(t), total).2)?;

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
            if layout_commits(tolerance, quorum, d, &loads, total).2 {
                trimmed = true;
            } else {
                loads[p as usize] = Some(k);
            }
        }
    }

    let (w, rows, _) = layout_commits(tolerance, quorum, d, &loads, total);
    Some(Assignment { d, w, rows, time })
}

#[cfg(test)]
mod tests {
    use super::super::reliability::ReliabilityProfile;
    use super::*;

    /// Reply time in ms of `bytes` over a link of `gbps`, with 1 ms delays.
    fn time_over_link(bytes: usize, gbps: f64) -> f64 {
        1.0 + f64::from(u32::try_from(bytes).unwrap()) * 8.0 / (gbps * 1e6)
    }

    /// Reply times over links of the given Gbps.
    fn time_over_links(gbps: Vec<f64>) -> impl Fn(ReplicaId, usize) -> Option<f64> {
        move |p, bytes| Some(time_over_link(bytes, gbps[p as usize]))
    }

    fn row_loads(a: &Assignment) -> Vec<u8> {
        a.rows.iter().map(Bitmap::count).collect()
    }

    #[test]
    fn small_values_commit_at_link_delay() {
        // with equal links, small values commit as soon as the delay allows,
        // sending no more than full copies to two peers would
        let tol = Tolerance::new(5, 2, vec![]).unwrap();
        let a = pick_assignment(
            0,
            &tol,
            3,
            time_over_links(vec![1.0; 5]),
            100,
            DEFAULT_DATA_SHARDS,
            TotalShards::AtMost(u8::MAX),
        )
        .unwrap();
        assert!(a.time < 1.01, "{a:?}");
        let sent: u8 = row_loads(&a)[1..].iter().sum();
        assert!(sent <= 2 * DEFAULT_DATA_SHARDS, "{a:?}");
    }

    #[test]
    fn uneven_links_get_uneven_rows() {
        // links of 1, 1, 0.4, 0.1 Gbps: faster peers persist more, and the
        // commit beats that of full copies (on the two 1 Gbps links)
        let tol = Tolerance::new(5, 2, vec![]).unwrap();
        let reply = time_over_links(vec![1.0, 1.0, 1.0, 0.4, 0.1]);
        let size = 8 << 20;
        let a = pick_assignment(
            0,
            &tol,
            3,
            &reply,
            size,
            DEFAULT_DATA_SHARDS,
            TotalShards::AtMost(u8::MAX),
        )
        .unwrap();
        let full_copies = reply(1, size).unwrap();
        assert!(a.time < 0.75 * full_copies, "{a:?} vs {full_copies}");
        let l = row_loads(&a);
        assert!(l[1] >= l[3] && l[3] >= l[4], "{l:?}");
    }

    #[test]
    fn rack_domain_spares_its_peers() {
        // tolerating any one server or the rack of 3 and 4, halves on 1 and
        // 2 commit without the rack
        let rack = ReliabilityProfile::parse(5, "domains:3,4").unwrap();
        let tol = Tolerance::new(5, 1, rack.crash_sets()).unwrap();
        let reply = |p: ReplicaId, bytes: usize| {
            let t = time_over_link(bytes, 1.0);
            Some(if p >= 3 { t + 500.0 } else { t })
        };
        let a = pick_assignment(
            0,
            &tol,
            3,
            reply,
            8 << 20,
            DEFAULT_DATA_SHARDS,
            TotalShards::AtMost(u8::MAX),
        )
        .unwrap();
        let l = row_loads(&a);
        assert_eq!((l[3], l[4]), (0, 0), "{a:?}");
        assert_eq!(l[1] + l[2], a.d, "{a:?}");
    }

    #[test]
    fn waits_for_a_majority() {
        // tolerating any one failure, a full copy on 1 alone passes the
        // commit rule, but commits wait for a majority of (maybe empty) replies
        let tol = Tolerance::new(5, 1, vec![]).unwrap();
        let reply = |p: ReplicaId, bytes: usize| {
            Some(time_over_link(bytes, 1.0) + if p >= 2 { 50.0 } else { 0.0 })
        };
        let size = 1 << 20;
        let fewer = pick_assignment(
            0,
            &tol,
            2,
            reply,
            size,
            DEFAULT_DATA_SHARDS,
            TotalShards::AtMost(u8::MAX),
        )
        .unwrap();
        assert!(fewer.time < 50.0, "{fewer:?}");
        let a = pick_assignment(
            0,
            &tol,
            3,
            reply,
            size,
            DEFAULT_DATA_SHARDS,
            TotalShards::AtMost(u8::MAX),
        )
        .unwrap();
        assert!(a.time >= 50.0, "{a:?}");
    }

    #[test]
    fn keeps_scheme_bounds() {
        let tol = Tolerance::new(5, 2, vec![]).unwrap();
        let reply = time_over_links(vec![1.0, 1.0, 1.0, 0.4, 0.1]);
        let given = TotalShards::Exactly(5);
        let a = pick_assignment(0, &tol, 3, &reply, 1 << 20, 3, given).unwrap();
        assert_eq!((a.d, a.w), (3, 5));
        assert!(a.rows.iter().all(|r| r.size() == 5));
        // at most two parity shards
        let a = pick_assignment(
            0,
            &tol,
            3,
            &reply,
            1 << 20,
            DEFAULT_DATA_SHARDS,
            TotalShards::AtMost(DEFAULT_DATA_SHARDS + 2),
        );
        assert!(a.unwrap().w <= DEFAULT_DATA_SHARDS + 2);
        // none if the commit rule cannot pass, e.g., with too few replying
        let few = |p: ReplicaId, _| (p < 2).then_some(1.0);
        assert!(
            pick_assignment(
                0,
                &tol,
                3,
                few,
                1 << 20,
                DEFAULT_DATA_SHARDS,
                TotalShards::AtMost(u8::MAX)
            )
            .is_none()
        );
    }
}
