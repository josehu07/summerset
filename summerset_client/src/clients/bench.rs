//! Benchmarking client using open-loop driver.

use std::collections::HashSet;

use crate::drivers::DriverOpenLoop;

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;

use serde::Deserialize;

use tokio::time::{Duration, Instant};

use summerset::{
    GenericEndpoint, ClientId, RequestId, SummersetError, pf_info, pf_error,
    logged_err, parsed_config,
};

lazy_static! {
    /// Pool of keys to choose from.
    // TODO: enable using a dynamic pool of keys
    static ref KEYS_POOL: Vec<String> = {
        let mut pool = vec![];
        for _ in 0..5 {
            let key = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(8)
                .map(char::from)
                .collect();
            pool.push(key)
        }
        pool
    };

    /// Target batch duration to approach.
    static ref TARGET_BATCH_DUR: Duration = Duration::from_millis(5);

    /// Statistics printing interval.
    static ref PRINT_INTERVAL: Duration = Duration::from_millis(500);
}

/// Mode parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsBench {
    /// Initial number of requests per batch.
    pub init_batch_size: u64,

    /// Time length to benchmark in seconds.
    pub length_s: u64,

    /// Percentage of put requests.
    pub put_ratio: u8,

    /// Value size in bytes.
    pub value_size: usize,

    /// Whether to adaptively adjust batch size.
    pub adaptive: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsBench {
    fn default() -> Self {
        ModeParamsBench {
            init_batch_size: 1,
            length_s: 30,
            put_ratio: 50,
            value_size: 1024,
            adaptive: false,
        }
    }
}

/// Benchmarking client struct.
pub struct ClientBench {
    /// Client ID.
    id: ClientId,

    /// Open-loop request driver.
    driver: DriverOpenLoop,

    /// Mode parameters struct.
    params: ModeParamsBench,

    /// Random number generator.
    rng: ThreadRng,

    /// Fixed value generated according to specified size.
    value: String,

    /// Set of pending request IDs.
    pending_reqs: HashSet<RequestId>,

    /// If true, there is request to be retried.
    should_retry: bool,
}

impl ClientBench {
    /// Creates a new benchmarking client.
    pub fn new(
        id: ClientId,
        stub: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsBench;
                                     init_batch_size, length_s, put_ratio,
                                     value_size, adaptive)?;
        if params.init_batch_size == 0 {
            return logged_err!(id; "invalid params.init_batch_size '{}'",
                                   params.init_batch_size);
        }
        if params.length_s == 0 {
            return logged_err!(id; "invalid params.length_s '{}'",
                                   params.length_s);
        }
        if params.put_ratio > 100 {
            return logged_err!(id; "invalid params.put_ratio '{}'",
                                   params.put_ratio);
        }
        if params.value_size == 0 {
            return logged_err!(id; "invalid params.value_size '{}'",
                                   params.value_size);
        }

        let value = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(params.value_size)
            .map(char::from)
            .collect();

        Ok(ClientBench {
            id,
            driver: DriverOpenLoop::new(id, stub, timeout),
            params,
            rng: rand::thread_rng(),
            value,
            pending_reqs: HashSet::new(),
            should_retry: false,
        })
    }

    /// Issues a random request.
    async fn issue_rand_cmd(
        &mut self,
    ) -> Result<Option<RequestId>, SummersetError> {
        let key = KEYS_POOL[self.rng.gen_range(0..KEYS_POOL.len())].clone();
        if self.rng.gen_range(0..=100) <= self.params.put_ratio {
            self.driver.issue_put(&key, &self.value).await
        } else {
            self.driver.issue_get(&key).await
        }
    }

    /// Issues a batch of requests and waits for all of their replies.
    /// Returns the number of successful replies received and a latency sample
    /// in microseconds if sampled.
    async fn do_batch(
        &mut self,
        num_reqs: u64,
    ) -> Result<(u64, Option<f64>), SummersetError> {
        assert!(num_reqs > 0);
        let (mut first_issue_ts, mut last_issue_ts) = (None, None);
        let (mut first_reply_ts, mut last_reply_ts) = (None, None);

        let mut issue_cnt = 0;
        let mut batch_pending_reqs = HashSet::new();
        while issue_cnt < num_reqs {
            let req_id = if self.should_retry {
                self.driver.issue_retry().await?
            } else {
                self.issue_rand_cmd().await?
            };

            if let Some(id) = req_id {
                batch_pending_reqs.insert(id);
                issue_cnt += 1;
                self.should_retry = false;
            } else {
                // got `WouldBlock` failure
                self.should_retry = true;
                break;
            }

            if issue_cnt == 0 {
                first_issue_ts = Some(Instant::now());
            }
            if issue_cnt == num_reqs - 1 {
                last_issue_ts = Some(Instant::now());
            }
        }

        let (mut reply_cnt, mut ok_cnt) = (0, 0);
        while reply_cnt < issue_cnt {
            let result = self.driver.wait_reply().await?;
            reply_cnt += 1;

            if let Some((req_id, _)) = result {
                ok_cnt += 1;
                batch_pending_reqs.remove(&req_id);

                if reply_cnt == 0 {
                    first_reply_ts = Some(Instant::now());
                }
                if reply_cnt == issue_cnt - 1 {
                    last_reply_ts = Some(Instant::now());
                }
            }
        }

        for req_id in batch_pending_reqs {
            self.pending_reqs.insert(req_id);
        }

        // calculate latency sample
        let first_lat = if first_issue_ts.is_some() && first_reply_ts.is_some()
        {
            Some(
                (first_reply_ts
                    .unwrap()
                    .duration_since(first_issue_ts.unwrap())
                    .as_nanos() as f64)
                    / 1000.0,
            )
        } else {
            None
        };
        let last_lat = if last_issue_ts.is_some() && last_reply_ts.is_some() {
            Some(
                (last_reply_ts
                    .unwrap()
                    .duration_since(last_issue_ts.unwrap())
                    .as_nanos() as f64)
                    / 1000.0,
            )
        } else {
            None
        };
        let lat_sample = if first_lat.is_some() && last_lat.is_some() {
            Some((first_lat.unwrap() + last_lat.unwrap()) / 2.0)
        } else if first_lat.is_some() {
            first_lat
        } else if last_lat.is_some() {
            last_lat
        } else {
            None
        };

        Ok((ok_cnt, lat_sample))
    }

    /// Runs the adaptive benchmark for given time length.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        let start = Instant::now();
        let mut now = start;
        let length = Duration::from_secs(self.params.length_s);

        let mut batch_size = self.params.init_batch_size;
        let mut ok_cnt = 0;
        let mut total_cnt = 0;

        let mut chunk_cnt = 0;
        let mut chunk_lats: Vec<f64> = vec![];

        println!(
            "{:^11} | {:^12} | {:^12} | {:>8} / {:<8}",
            "Elapsed (s)", "Tpt (reqs/s)", "Lat (us)", "OK", "Total"
        );
        let mut last_print = now;

        while now.duration_since(start) < length {
            let batch_start = now;

            let (batch_ok_cnt, lat_sample) = self.do_batch(batch_size).await?;
            ok_cnt += batch_ok_cnt;
            total_cnt += batch_size;

            chunk_cnt += batch_ok_cnt;
            if let Some(lat) = lat_sample {
                chunk_lats.push(lat);
            }

            now = Instant::now();

            // print statistics if print interval passed
            let elapsed = now.duration_since(start);
            let print_elapsed = now.duration_since(last_print);
            if print_elapsed >= *PRINT_INTERVAL {
                let tpt = (chunk_cnt as f64) / print_elapsed.as_secs_f64();
                let lat =
                    chunk_lats.iter().sum::<f64>() / (chunk_lats.len() as f64);
                println!(
                    "{:>11.2} | {:>12.2} | {:>12.2} | {:>8} / {:<8}",
                    elapsed.as_secs_f64(),
                    tpt,
                    lat,
                    ok_cnt,
                    total_cnt
                );
                last_print = now;
                chunk_cnt = 0;
                chunk_lats.clear();
            }

            if self.params.adaptive {
                // adaptively adjust number of requests per batch
                let batch_dur = now.duration_since(batch_start);
                assert!(!batch_dur.is_zero());
                let adjust =
                    TARGET_BATCH_DUR.as_secs_f64() / batch_dur.as_secs_f64();
                batch_size = ((batch_size as f64) * adjust) as u64;
                if batch_size == 0 {
                    batch_size = 1;
                }
            }
        }

        if !self.pending_reqs.is_empty() {
            pf_info!(self.id; "there are {} pending requests",
                              self.pending_reqs.len());
        }

        self.driver.leave().await?;
        Ok(())
    }
}
