//! Stopwatch utility useful for bookkeeping performance breakdown stats.

use std::collections::HashMap;

use crate::utils::SummersetError;

use tokio::time::Instant;

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
    records: HashMap<RecordId, Vec<Instant>>,
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
            record.push(Instant::now());
            Ok(())
        }
    }

    /// Removes the record of given ID.
    pub fn remove_id(&mut self, id: RecordId) {
        self.records.remove(&id);
    }

    /// Removes all current records of given ID.
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
                        record[i + 1].duration_since(record[i]).as_micros()
                            as f64,
                    );
                }
            }
        }

        let step_stats = step_times
            .into_iter()
            .map(|mut v| {
                v.sort_by(|x, y| x.partial_cmp(y).unwrap());
                let outliers = (v.len() as f32 * 0.1) as usize;
                v.truncate(v.len() - outliers);
                v.drain(0..outliers);

                let mean = mean(&v);
                let stdev = standard_deviation(&v, Some(mean));

                (mean, stdev)
            })
            .collect();

        (cnt, step_stats)
    }
}

#[cfg(test)]
mod stopwatch_tests {
    use super::*;
    use tokio::time::{self, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopwatch_records() -> Result<(), SummersetError> {
        let mut sw = Stopwatch::new();
        sw.record_now(0, 0)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(0, 1)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(1, 0)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(0, 2)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(1, 1)?;
        time::sleep(Duration::from_micros(200)).await;
        sw.record_now(2, 0)?;
        assert!(sw.record_now(0, 4).is_err());
        assert!(sw.record_now(3, 1).is_err());
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
                sw.record_now(id, step)?;
                time::sleep(Duration::from_micros(100)).await;
            }
        }
        sw.record_now(3, 0)?;
        time::sleep(Duration::from_micros(100)).await;
        sw.record_now(3, 1)?;
        let (cnt, stats) = sw.summarize(3);
        assert_eq!(cnt, 3);
        assert_eq!(stats.len(), 3);
        Ok(())
    }
}
