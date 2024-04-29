//! Linear regression helpers for performance monitoring.

use std::fmt;
use std::collections::HashSet;

use crate::utils::SummersetError;

use rangemap::RangeMap;

use linreg::linear_regression_of;

/// Performance model of a peer target.
#[derive(Debug, PartialEq, Clone)]
pub struct PerfModel {
    /// Base bandwidth factor (slope) in ms/MiB.
    slope: f64,

    /// Base delay (interception) in ms.
    delay: f64,

    /// Average jitter in ms.
    jitter: f64,
}

impl fmt::Display for PerfModel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "({:.1},{:.1}+{:.1})",
            self.slope, self.delay, self.jitter
        )
    }
}

impl PerfModel {
    /// Creates a new perf model struct.
    #[inline]
    pub fn new(slope: f64, delay: f64, jitter: f64) -> Self {
        PerfModel {
            delay,
            jitter,
            slope,
        }
    }

    /// Updates the perf model numbers.
    #[inline]
    pub fn update(&mut self, slope: f64, delay: f64, jitter: f64) {
        self.slope = slope;
        self.delay = delay;
        self.jitter = jitter;
    }

    /// Calculate estimated response time given a data size.
    #[inline]
    pub fn predict(&self, x: usize) -> f64 {
        let size_mb = x as f64 / (1024 * 1024) as f64;
        self.slope * size_mb + self.delay + self.jitter
    }
}

/// Linear regression helper struct for maintaining time-tagged datapoints
/// and computing a linear regression model upon requested.
#[derive(Debug)]
pub struct LinearRegressor {
    /// Windows of currently held datapoints, divided into range buckets.
    buckets: Vec<Vec<(u128, usize, f64)>>,

    /// Map from x value range -> corresponding bucket index.
    rangemap: RangeMap<usize, usize>,

    /// Result of last calculated regression model; upon any update to the
    /// window of datapoints, this result will be invalidated to `None`.
    model: Option<PerfModel>,
}

impl LinearRegressor {
    /// Creates a new linear regressor helper struct.
    // NOTE: currently only using two buckets: size 0 and everything above.
    pub fn new() -> Self {
        let mut buckets = vec![];
        let mut rangemap = RangeMap::new();
        buckets.push(vec![]);
        rangemap.insert(0..1, 0);
        buckets.push(vec![]);
        rangemap.insert(1..usize::MAX, 1);

        LinearRegressor {
            buckets,
            rangemap,
            model: None,
        }
    }

    /// Injects a new datapoint into the window. It is assumed that all
    /// injections must have monotonically non-decreasing time tags.
    pub fn append_sample(&mut self, t: u128, x: usize, y: f64) {
        let bucket_idx = *self.rangemap.get(&x).unwrap();
        debug_assert!(bucket_idx < self.buckets.len());
        let bucket = &mut self.buckets[bucket_idx];
        debug_assert!(bucket.is_empty() || t >= bucket.last().unwrap().0);

        bucket.push((t, x, y));

        if self.model.is_some() {
            self.model = None;
        }
    }

    /// Discards everything with timestamp tag before given time.
    pub fn discard_before(&mut self, t: u128) {
        for bucket in self.buckets.iter_mut() {
            let mut keep = bucket.len();
            for (i, dp) in bucket.iter().enumerate() {
                if dp.0 >= t {
                    keep = i;
                    break;
                }
            }

            bucket.drain(0..keep);

            if self.model.is_some() {
                self.model = None;
            }
        }
    }

    /// Returns the result of linear regression model calculated on the
    /// current window of datapoints. If the model is not valid right now,
    /// compute it.
    pub fn calc_model(
        &mut self,
        outliers_ratio: f32,
    ) -> Result<PerfModel, SummersetError> {
        debug_assert!((0.0..1.0).contains(&outliers_ratio));

        if let Some(model) = self.model.as_ref() {
            // pf_trace!("linreg"; "calc ts {:?} dps {:?} {:?}",
            //                     self.timestamps, self.datapoints, model);
            Ok(model.clone())
        } else {
            // use all datapoints in the size 0 bucket (i.e., the heartbeat
            // messages) to estimate delay and jitter
            let bucket0 = &self.buckets[0];
            let mut delay = bucket0
                .iter()
                .min_by(|dpa, dpb| dpa.2.partial_cmp(&dpb.2).unwrap())
                .map(|dp| dp.2)
                .unwrap_or(0.0);
            let mut jitter = (bucket0.iter().map(|dp| dp.2).sum::<f64>()
                / bucket0.len() as f64)
                - delay;

            if delay < 0.0 {
                delay = 0.0;
            }
            if jitter < 0.0 {
                jitter = 0.0;
            }

            // compute model on current window of datapoints
            let mut xys: Vec<(f64, f64)> = vec![];
            for bucket in &self.buckets {
                xys.append(
                    &mut bucket.iter().map(|dp| (dp.1 as f64, dp.2)).collect(),
                );
            }
            let mut slope = linear_regression_of(&xys)?.0;
            if slope < 0.0 {
                slope = 0.0;
            }

            // remove potential outliers, where outliers are defined as the
            // points that are furthest away from computed model (but forcing
            // delay to be previously computed)
            if outliers_ratio > 0.0 && xys.len() as f32 * outliers_ratio >= 1.0
            {
                let mut distances: Vec<(usize, f64)> = xys
                    .iter()
                    .map(|(x, y)| (y - (x * slope + delay)).abs())
                    .enumerate()
                    .collect();
                distances.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                let to_remove: HashSet<usize> = distances
                    .into_iter()
                    .take((xys.len() as f32 * outliers_ratio).round() as usize)
                    .map(|(i, _)| i)
                    .collect();
                let new_xys: Vec<(f64, f64)> = xys
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, dp)| {
                        if to_remove.contains(&i) {
                            None
                        } else {
                            Some(dp)
                        }
                    })
                    .collect();
                slope = linear_regression_of(&new_xys)?.0;
                if slope < 0.0 {
                    slope = 0.0;
                }
            }

            // take the lowest percentage of datapoints from each bucket; do
            // linear regression on them to get an estimation of base delay
            // and base bandwidth factor
            // let mut xys: Vec<(f64, f64)> = vec![];
            // for bucket in &self.buckets {
            //     let min_cnt = cmp::max(
            //         (bucket.len() as f32 * (1.0 - outliers_ratio)) as usize,
            //         1,
            //     );

            //     if !bucket.is_empty() {
            //         let mut sorted = bucket.clone();
            //         sorted
            //             .sort_by(|dpa, dpb| dpa.2.partial_cmp(&dpb.2).unwrap());
            //         let mut min_dps: Vec<(f64, f64)> = sorted
            //             .iter()
            //             .take(min_cnt)
            //             .map(|dp| (dp.1 as f64, dp.2))
            //             .collect();

            //         xys.append(&mut min_dps);
            //     }
            // }
            // let (mut slope, mut delay) = linear_regression_of(&xys)?;

            slope *= (1024 * 1024) as f64;
            let model = PerfModel::new(slope, delay, jitter);
            // pf_warn!("linreg"; "calc ts {:?} dps {:?} {}",
            //                    self.timestamps, self.datapoints, model);
            self.model = Some(model.clone());
            Ok(model)
        }
    }
}

#[cfg(test)]
mod linreg_tests {
    use super::*;

    #[test]
    fn append_discard() {
        let mut lg = LinearRegressor::new();
        assert!(lg.model.is_none());
        lg.append_sample(0, 0, 0.5);
        lg.append_sample(2, 3, 1.1);
        lg.append_sample(6, 5, 2.3);
        assert_eq!(lg.buckets[*lg.rangemap.get(&0).unwrap()].len(), 1);
        assert_eq!(lg.buckets[*lg.rangemap.get(&3).unwrap()].len(), 2);
        lg.discard_before(4);
        assert_eq!(lg.buckets[*lg.rangemap.get(&0).unwrap()].len(), 0);
        assert_eq!(lg.buckets[*lg.rangemap.get(&3).unwrap()].len(), 1);
    }

    #[test]
    fn calc_model() -> Result<(), SummersetError> {
        let mut lg = LinearRegressor::new();
        assert!(lg.model.is_none());
        lg.append_sample(0, 0, 0.5);
        lg.append_sample(2, 1024 * 1024, 1.2);
        assert_eq!(lg.calc_model(0.0)?, PerfModel::new(0.7, 0.5, 0.0));
        lg.discard_before(2);
        assert!(lg.model.is_none());
        Ok(())
    }

    #[test]
    fn model_predict() {
        let model = PerfModel::new(1.0, 0.6, 1.2);
        let p0 = model.predict(0);
        assert!(p0 > 1.75 && p0 < 1.85);
        let p1 = model.predict(1024 * 1024);
        assert!(p1 > 2.75 && p1 < 2.85)
    }
}
