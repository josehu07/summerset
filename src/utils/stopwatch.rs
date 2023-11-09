//! Stopwatch utility useful for bookkeeping performance breakdown stats.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::utils::SummersetError;

use statistical::{mean, standard_deviation};

/// Using usize as ID type.
type RecordId = usize;

/// Stopwatch utility for bookkeeping timestamps for performance breakdown
/// statistics, etc.
///
/// Groups records by IDs and saves all timestamps of all records for future
/// summary reporting.
#[derive(Debug)]
pub struct Stopwatch {
    /// Map from ID -> vec of the record's step timestamps.
    records: HashMap<RecordId, Vec<SystemTime>>,
}

impl Stopwatch {
    /// Creates a new stopwatch utility.
    pub fn new() -> Self {
        Stopwatch {
            records: HashMap::new(),
        }
    }

    /// Save current timestamp for given record at given step.
    pub fn record_now(
        &mut self,
        id: RecordId,
        step: usize,
        // if Some, uses this timestamp instead of fetching current timestamp
        now: Option<SystemTime>,
    ) -> Result<(), SummersetError> {
        if !self.records.contains_key(&id) && step != 0 {
            return Err(SummersetError(format!("record {} not found", id)));
        }
        self.records.entry(id).or_default();
        let record = self.records.get_mut(&id).unwrap();

        if step != record.len() {
            Err(SummersetError(format!(
                "step mismatch: expect {} got {}",
                record.len(),
                step
            )))
        } else {
            record.push(if let Some(ts) = now {
                ts
            } else {
                SystemTime::now()
            });
            Ok(())
        }
    }

    /// Checks if a record ID exists.
    #[inline]
    #[allow(dead_code)]
    pub fn has_id(&self, id: RecordId) -> bool {
        self.records.contains_key(&id)
    }

    /// Removes the record of given ID.
    #[inline]
    #[allow(dead_code)]
    pub fn remove_id(&mut self, id: RecordId) {
        self.records.remove(&id);
    }

    /// Removes all current records of given ID.
    #[inline]
    pub fn remove_all(&mut self) {
        self.records.clear();
    }

    /// Gather a summary of #records as well as (mean_us, stdev_us) of times
    /// taken in each interval, up to step index, across all current records.
    pub fn summarize(&self, steps: usize) -> (usize, Vec<(f64, f64)>) {
        debug_assert!(steps > 0);
        let mut step_times: Vec<Vec<f64>> =
            vec![Vec::with_capacity(self.records.len()); steps];
        let mut cnt = 0;

        for record in self.records.values() {
            if record.len() > steps {
                cnt += 1;
                for i in 0..steps {
                    step_times[i].push(
                        record[i + 1]
                            .duration_since(record[i])
                            .unwrap_or(Duration::ZERO)
                            .as_micros() as f64,
                    );
                }
            }
        }

        let outliers = (cnt as f32 * 0.1) as usize;
        if cnt - outliers > 2 {
            let step_stats = step_times
                .into_iter()
                .map(|mut v| {
                    v.sort_by(|x, y| x.partial_cmp(y).unwrap());
                    v.truncate(v.len() - outliers);
                    v.drain(0..outliers);

                    let mean = mean(&v);
                    let stdev = standard_deviation(&v, Some(mean));

                    (mean, stdev)
                })
                .collect();
            (cnt - outliers, step_stats)
        } else {
            (0, vec![(0.0, 0.0); steps])
        }
    }
}

#[cfg(test)]
mod stopwatch_tests {
    use super::*;
    use tokio::time::{self, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopwatch_records() -> Result<(), SummersetError> {
        let mut sw = Stopwatch::new();
        sw.record_now(0, 0, None)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(0, 1, None)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(1, 0, Some(SystemTime::now()))?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(0, 2, Some(SystemTime::now()))?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(1, 1, None)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(2, 0, None)?;
        assert!(sw.record_now(0, 4, None).is_err());
        assert!(sw.record_now(3, 1, None).is_err());
        assert!(sw.has_id(1));
        assert!(!sw.has_id(3));
        assert_eq!(sw.records.len(), 3);
        sw.remove_id(1);
        assert_eq!(sw.records.len(), 2);
        sw.remove_all();
        assert_eq!(sw.records.len(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopwatch_summary() -> Result<(), SummersetError> {
        let mut sw = Stopwatch::new();
        for id in 0..3 {
            for step in 0..4 {
                sw.record_now(id, step, None)?;
                time::sleep(Duration::from_micros(100)).await;
            }
        }
        sw.record_now(3, 0, None)?;
        time::sleep(Duration::from_micros(100)).await;
        sw.record_now(3, 1, None)?;
        let (cnt, stats) = sw.summarize(3); // should not trigger outliers
        assert_eq!(cnt, 3);
        assert_eq!(stats.len(), 3);
        Ok(())
    }
}
