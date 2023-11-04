//! Crossword -- assignment adaptability.

use std::mem;

use super::*;

use crate::utils::SummersetError;

// CrosswordReplica linear regression perf monitoring
impl CrosswordReplica {
    /// Parse config string into initial shards assignment policy.
    pub fn parse_init_assignment(
        population: u8,
        s: &str,
    ) -> Result<Vec<Bitmap>, SummersetError> {
        let mut assignment = Vec::with_capacity(population as usize);
        let majority = (population / 2) + 1;
        if s.is_empty() {
            // default to start with bandwidth-optimal diagonal assignment
            for r in 0..population {
                assignment.push(Bitmap::from(population, vec![r]));
            }
        } else if let Ok(shards_per_replica) = s.parse::<u8>() {
            // a single number: the same #shards per replica round-robinly
            if shards_per_replica == 0 || shards_per_replica > majority {
                return Err(SummersetError(format!(
                    "invalid shards assignment string {}",
                    s
                )));
            }
            for r in 0..population {
                assignment.push(Bitmap::from(
                    population,
                    (r..r + shards_per_replica)
                        .map(|i| i % population)
                        .collect(),
                ));
            }
        } else {
            // string in format of something like 0:0,1/1:2/3:3,4 ...
            for _ in 0..population {
                assignment.push(Bitmap::new(population, false));
            }
            for seg in s.split('/') {
                if let Some(idx) = seg.find(':') {
                    let r = seg[..idx].parse::<ReplicaId>()?;
                    if r >= population {
                        return Err(SummersetError(format!(
                            "invalid shards assignment string {}",
                            s
                        )));
                    }
                    for shard in seg[idx + 1..].split(',') {
                        assignment[r as usize].set(shard.parse()?, true)?;
                    }
                } else {
                    return Err(SummersetError(format!(
                        "invalid shards assignment string {}",
                        s
                    )));
                }
            }
        }
        Ok(assignment)
    }

    /// Get the proper assignment policy given data size and peer_alive count.
    // NOTE: if data_size == exactly `usize::MAX` this will fail; won't bother
    //       to account for this rare case right now
    #[inline]
    pub fn pick_assignment_policy<'a>(
        assignment_balanced: bool,
        assignment_policies: &'a RangeMap<usize, Vec<Bitmap>>,
        good_rr_assignments: &'a HashMap<u8, Vec<Bitmap>>,
        majority: u8,
        fault_tolerance: u8,
        data_size: usize,
        peer_alive: &Bitmap,
    ) -> &'a Vec<Bitmap> {
        if assignment_balanced {
            let assignment = assignment_policies.get(&data_size).unwrap();
            let min_shards_per_replica =
                majority + fault_tolerance + 1 - peer_alive.count();
            if assignment[0].count() >= min_shards_per_replica {
                assignment
            } else {
                good_rr_assignments.get(&min_shards_per_replica).unwrap()
            }
        } else {
            // NOTE: skips the check of `alive_cnt` for unbalanced assignments;
            //       leaving this as future work
            assignment_policies.get(&data_size).unwrap()
        }
    }

    // Pretty-print assignment policies.
    #[inline]
    pub fn assignment_policies_str(
        assignment_policies: &RangeMap<usize, Vec<Bitmap>>,
    ) -> String {
        let mut s = String::new();
        for (range, policy) in assignment_policies.iter() {
            let ps = policy
                .iter()
                .enumerate()
                .map(|(i, a)| format!("{}:{}", i, a.compact_str()))
                .collect::<Vec<String>>()
                .join("/");
            s.push_str(&format!(
                "{}~{}-{{{}}} ",
                range.start,
                if range.end == usize::MAX {
                    "max".into()
                } else {
                    range.end.to_string()
                },
                ps
            ));
        }
        s
    }

    /// Records a new datapoint for Accept RTT time.
    pub fn record_accept_rtt(
        &mut self,
        peer: ReplicaId,
        tr: u128,
        slot: usize,
        size: usize,
    ) {
        // pop oldest heartbeats sent timestamps out until the corresponding
        // heartbeat ID is found. Records preceding the matching record will
        // be discarded forever
        while let Some((ts, s)) = self.pending_accepts.pop_front() {
            #[allow(clippy::comparison_chain)]
            if s == slot {
                debug_assert!(tr >= ts);
                // approximate size as the PeerMsg type's stack size + shards
                // payload size
                let mut size_mb: f64 = 2.0 * mem::size_of::<PeerMsg>() as f64;
                size_mb += size as f64;
                size_mb /= (1024 * 1024) as f64;
                let elapsed_ms: f64 = (tr - ts) as f64 / 1000.0;
                self.regressor
                    .get_mut(&peer)
                    .unwrap()
                    .append_sample(tr, size_mb, elapsed_ms);
                break;
            } else if slot < s {
                // larger slot seen, meaning the send record for slot is
                // probably lost. Do nothing
                self.pending_accepts.push_front((ts, s));
                break;
            }
        }
    }

    /// Records a new datapoint for heartbeat RTT time.
    pub fn record_heartbeat_rtt(
        &mut self,
        peer: ReplicaId,
        tr: u128,
        hb_id: HeartbeatId,
    ) {
        // pop oldest heartbeats sent timestamps out until the corresponding
        // heartbeat ID is found. Records preceding the matching record will
        // be discarded forever
        while let Some((ts, id)) = self.pending_heartbeats.pop_front() {
            #[allow(clippy::comparison_chain)]
            if id == hb_id {
                debug_assert!(tr >= ts);
                let size_mb: f64 = 2.0 * mem::size_of::<PeerMsg>() as f64
                    / (1024 * 1024) as f64;
                let elapsed_ms: f64 = (tr - ts) as f64 / 1000.0;
                self.regressor
                    .get_mut(&peer)
                    .unwrap()
                    .append_sample(tr, size_mb, elapsed_ms);
                break;
            } else if hb_id < id {
                // larger ID seen, meaning the send record for hb_id is
                // probably lost. Do nothing
                self.pending_heartbeats.push_front((ts, id));
                break;
            }
        }
    }

    /// Discards all datapoints older than one interval ago, then updates the
    /// linear regression perf monitoring model for each replica using the
    /// current window of datapoints.
    pub fn update_linreg_model(&mut self) -> Result<(), SummersetError> {
        let now_us = self.startup_time.elapsed().as_micros();
        let keep_us = now_us - 1000 * self.config.linreg_interval_ms as u128;

        for (peer, regressor) in self.regressor.iter_mut() {
            regressor.discard_before(keep_us);
            match regressor.calc_model() {
                Ok(mut model) => {
                    if model.0 < 0.0 {
                        model.0 = 0.0;
                    }
                    if model.1 < 0.0 {
                        model.1 = 0.0;
                    }
                    *self.linreg_model.get_mut(peer).unwrap() = model;
                }
                Err(_e) => {
                    // pf_trace!(self.id; "calc_model error: {}", e);
                }
            }
        }
        Ok(())
    }
}
