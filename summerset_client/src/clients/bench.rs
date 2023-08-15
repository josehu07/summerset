//! Benchmarking client using open-loop driver.

use crate::drivers::DriverOpenLoop;

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;

use serde::Deserialize;

use tokio::time::{self, Duration, Instant, MissedTickBehavior};

use summerset::{
    GenericEndpoint, ClientId, RequestId, SummersetError, pf_error, logged_err,
    parsed_config,
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

    /// Statistics printing interval.
    static ref PRINT_INTERVAL: Duration = Duration::from_millis(500);
}

/// Mode parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsBench {
    /// Target frequency of issuing requests per second.
    pub freq_target: u64,

    /// Time length to benchmark in seconds.
    pub length_s: u64,

    /// Percentage of put requests.
    pub put_ratio: u8,

    /// Value size in bytes.
    pub value_size: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsBench {
    fn default() -> Self {
        ModeParamsBench {
            freq_target: 200000,
            length_s: 30,
            put_ratio: 50,
            value_size: 1024,
        }
    }
}

/// Benchmarking client struct.
pub struct ClientBench {
    /// Client ID.
    _id: ClientId,

    /// Open-loop request driver.
    driver: DriverOpenLoop,

    /// Mode parameters struct.
    params: ModeParamsBench,

    /// Random number generator.
    rng: ThreadRng,

    /// Fixed value generated according to specified size.
    value: String,
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
                                     freq_target, length_s, put_ratio,
                                     value_size)?;
        if params.freq_target > 10000000 {
            return logged_err!(id; "invalid params.freq_target '{}'",
                                   params.freq_target);
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
            _id: id,
            driver: DriverOpenLoop::new(id, stub, timeout),
            params,
            rng: rand::thread_rng(),
            value,
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

    /// Runs closed-loop style benchmark.
    async fn run_closed_loop(&mut self) -> Result<(), SummersetError> {
        let start = Instant::now();
        let (mut now, mut last_print) = (start, start);
        let length = Duration::from_secs(self.params.length_s);

        let (mut reply_cnt, mut total_cnt) = (0, 0);
        let mut chunk_cnt = 0;
        let mut chunk_lats: Vec<f64> = vec![];

        // run for specified length
        let mut retrying = false;
        while now.duration_since(start) < length {
            // send next request
            let req_id = if retrying {
                self.driver.issue_retry().await?
            } else {
                self.issue_rand_cmd().await?
            };

            retrying = req_id.is_none();
            if !retrying {
                total_cnt += 1;
            }

            // wait for the next reply
            if total_cnt > reply_cnt {
                let result = self.driver.wait_reply().await?;

                if let Some((_, _, lat)) = result {
                    reply_cnt += 1;
                    chunk_cnt += 1;
                    let lat_us = lat.as_secs_f64() * 1000000.0;
                    chunk_lats.push(lat_us);
                }
            }

            now = Instant::now();

            // print statistics if print interval passed
            let elapsed = now.duration_since(start);
            let print_elapsed = now.duration_since(last_print);
            if print_elapsed >= *PRINT_INTERVAL {
                let tpt = (chunk_cnt as f64) / print_elapsed.as_secs_f64();
                let lat = if chunk_lats.is_empty() {
                    0.0
                } else {
                    chunk_lats.iter().sum::<f64>() / (chunk_lats.len() as f64)
                };
                println!(
                    "{:>11.2} | {:>12.2} | {:>12.2} | {:>8} / {:<8}",
                    elapsed.as_secs_f64(),
                    tpt,
                    lat,
                    reply_cnt,
                    total_cnt
                );
                last_print = now;
                chunk_cnt = 0;
                chunk_lats.clear();
            }
        }

        Ok(())
    }

    /// Runs open-loop style benchmark.
    async fn run_open_loop(&mut self) -> Result<(), SummersetError> {
        let start = Instant::now();
        let (mut now, mut last_print) = (start, start);
        let length = Duration::from_secs(self.params.length_s);

        let (mut total_cnt, mut reply_cnt) = (0, 0);
        let mut chunk_cnt = 0;
        let mut chunk_lats: Vec<f64> = vec![];

        // kick off frequency interval ticker
        let delay = Duration::from_nanos(1000000000 / self.params.freq_target);
        let mut ticker = time::interval(delay);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // run for specified length
        let (mut slowdown, mut retrying) = (false, false);
        while now.duration_since(start) < length {
            tokio::select! {
                // prioritize receiving reply
                biased;

                // receive next reply
                result = self.driver.wait_reply() => {
                    let result = result?;

                    if let Some((_, _, lat)) = result {
                        reply_cnt += 1;
                        chunk_cnt += 1;
                        let lat_us = lat.as_secs_f64() * 1000000.0;
                        chunk_lats.push(lat_us);

                        if slowdown {
                            slowdown = false;
                        }
                    }
                }

                // send next request
                _ = ticker.tick(), if !slowdown => {
                    let req_id = if retrying {
                        self.driver.issue_retry().await?
                    } else {
                        self.issue_rand_cmd().await?
                    };

                    retrying = req_id.is_none();
                    slowdown = retrying && (total_cnt > reply_cnt);
                    if !retrying {
                        total_cnt += 1;
                    }
                }
            }

            now = Instant::now();

            // print statistics if print interval passed
            let elapsed = now.duration_since(start);
            let print_elapsed = now.duration_since(last_print);
            if print_elapsed >= *PRINT_INTERVAL {
                let tpt = (chunk_cnt as f64) / print_elapsed.as_secs_f64();
                let lat = if chunk_lats.is_empty() {
                    0.0
                } else {
                    chunk_lats.iter().sum::<f64>() / (chunk_lats.len() as f64)
                };
                println!(
                    "{:>11.2} | {:>12.2} | {:>12.2} | {:>8} / {:<8}",
                    elapsed.as_secs_f64(),
                    tpt,
                    lat,
                    reply_cnt,
                    total_cnt
                );
                last_print = now;
                chunk_cnt = 0;
                chunk_lats.clear();
            }
        }

        Ok(())
    }

    /// Runs the adaptive benchmark for given time length.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;
        println!(
            "{:^11} | {:^12} | {:^12} | {:>8} / {:<8}",
            "Elapsed (s)", "Tpt (reqs/s)", "Lat (us)", "Reply", "Total"
        );

        if self.params.freq_target == 0 {
            self.run_closed_loop().await?;
        } else {
            self.run_open_loop().await?;
        }

        self.driver.leave().await?;
        Ok(())
    }
}
