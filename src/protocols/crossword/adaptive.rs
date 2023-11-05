//! Crossword -- assignment adaptability.

use std::mem;

use super::*;

use crate::utils::SummersetError;

// CrosswordReplica linear regression perf monitoring
impl CrosswordReplica {
    /// Pretty-print a `Vec<Bitmap>` assignment policy
    #[inline]
    #[allow(clippy::ptr_arg)]
    pub fn assignment_to_string(assignment: &Vec<Bitmap>) -> String {
        assignment
            .iter()
            .enumerate()
            .map(|(r, a)| format!("{}:{{{}}}", r, a.compact_str()))
            .collect::<Vec<String>>()
            .join(" ")
    }

    /// Pretty-print linear regression models.
    #[inline]
    #[allow(clippy::ptr_arg)]
    fn linreg_models_to_string(
        models: &HashMap<ReplicaId, (f64, f64)>,
    ) -> String {
        models
            .iter()
            .map(|(r, model)| format!("{}:({:.2},{:.2})", r, model.0, model.1))
            .collect::<Vec<String>>()
            .join(" ")
    }

    /// Parse config string into initial shards assignment policy.
    pub fn parse_init_assignment(
        population: u8,
        rs_total_shards: u8,
        rs_data_shards: u8,
        s: &str,
    ) -> Result<Vec<Bitmap>, SummersetError> {
        debug_assert_eq!(rs_total_shards % population, 0);
        let dj_spr = rs_total_shards / population;
        let mut assignment = Vec::with_capacity(population as usize);
        if s.is_empty() {
            // default to start with bandwidth-optimal diagonal assignment
            for r in 0..population {
                assignment.push(Bitmap::from(
                    rs_total_shards,
                    ((r * dj_spr)..((r + 1) * dj_spr)).collect(),
                ));
            }
        } else if let Ok(spr) = s.parse::<u8>() {
            // a single number: the same #shards per replica round-robinly
            if spr < dj_spr || spr > rs_data_shards {
                return Err(SummersetError(format!(
                    "invalid shards assignment string {}",
                    s
                )));
            }
            for r in 0..population {
                assignment.push(Bitmap::from(
                    rs_total_shards,
                    ((r * dj_spr)..(r * dj_spr + spr))
                        .map(|i| i % rs_total_shards)
                        .collect(),
                ));
            }
        } else {
            // string in format of something like 0:0,1/1:2/3:3,4 ...
            for _ in 0..population {
                assignment.push(Bitmap::new(rs_total_shards, false));
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

    /// Compute minimum number of shards_per_replica (assuming balanced
    /// assignment) that is be responsive for a given peer_alive cnt.
    #[inline]
    pub fn min_shards_per_replica(
        rs_data_shards: u8,
        majority: u8,
        fault_tolerance: u8,
        alive_cnt: u8,
    ) -> u8 {
        (majority + fault_tolerance + 1 - alive_cnt)
            * (rs_data_shards / majority)
    }

    /// Get the proper assignment policy given data size and peer_alive count.
    // NOTE: if data_size == exactly `usize::MAX` this will fail; won't bother
    //       to account for this rare case right now
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn pick_assignment_policy<'a>(
        assignment_balanced: bool,
        init_assignment: &'a Vec<Bitmap>,
        brr_assignments: &'a HashMap<u8, Vec<Bitmap>>,
        rs_data_shards: u8,
        majority: u8,
        fault_tolerance: u8,
        data_size: usize,
        linreg_model: &HashMap<ReplicaId, (f64, f64)>,
        peer_alive: &Bitmap,
    ) -> &'a Vec<Bitmap> {
        // NOTE: skips the check of `alive_cnt` for unbalanced assignments;
        //       leaving this as future work
        if !assignment_balanced {
            return init_assignment;
        }

        // query the linear regression models and pick the best config of
        // (#shards_per_replica, quorum_size) pair along the constraint
        // boundary line
        let dj_spr = rs_data_shards / majority;
        let mut config_times =
            Vec::<(u8, f64)>::with_capacity(majority as usize);
        for (spr, q) in (dj_spr..=rs_data_shards)
            .step_by(dj_spr as usize)
            .enumerate()
            .map(|(i, spr)| (spr, majority + fault_tolerance - i as u8))
        {
            let load_size =
                (data_size / rs_data_shards as usize) * spr as usize;
            let mut peer_times: Vec<f64> = linreg_model
                .iter()
                .map(|(_, model)| model.0 + model.1 * load_size as f64)
                .collect();
            peer_times.sort_by(|x, y| x.partial_cmp(y).unwrap());
            config_times.push((spr, peer_times[q as usize - 2]));
        }
        let best_spr = config_times
            .iter()
            .min_by(|x, y| x.1.partial_cmp(&y.1).unwrap())
            .unwrap()
            .0;

        // if the best assignment according to models is not responsive enough
        // given the current peer_alive count, use the best possible one
        let min_spr = Self::min_shards_per_replica(
            rs_data_shards,
            majority,
            fault_tolerance,
            peer_alive.count(),
        );
        brr_assignments.get(&cmp::max(best_spr, min_spr)).unwrap()
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

        for (&peer, regressor) in self.regressor.iter_mut() {
            regressor.discard_before(keep_us);
            if !self.peer_alive.get(peer)? {
                // if peer not considered alive, use a very high delay
                *self.linreg_model.get_mut(&peer).unwrap() = (10000000.0, 1.0);
            } else {
                // otherwise, compute simple linear regression
                match regressor.calc_model() {
                    Ok(mut model) => {
                        if model.0 < 0.0 {
                            model.0 = 0.0;
                        }
                        if model.1 < 0.0 {
                            model.1 = 0.0;
                        }
                        *self.linreg_model.get_mut(&peer).unwrap() = model;
                    }
                    Err(_e) => {
                        // pf_trace!(self.id; "calc_model error: {}", e);
                    }
                }
            }
        }

        pf_info!(self.id; "linreg update {}",
                          Self::linreg_models_to_string(&self.linreg_model));
        Ok(())
    }
}
