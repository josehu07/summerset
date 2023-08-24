//! Benchmarking client using open-loop driver.

use crate::drivers::DriverOpenLoop;

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;

use serde::Deserialize;

use tokio::time::{self, Duration, Instant, Interval, MissedTickBehavior};

use summerset::{
    GenericEndpoint, RequestId, SummersetError, pf_error, logged_err,
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
    /// Open-loop request driver.
    driver: DriverOpenLoop,

    /// Mode parameters struct.
    params: ModeParamsBench,

    /// Random number generator.
    rng: ThreadRng,

    /// Fixed value generated according to specified size.
    value: String,

    /// Total number of requests issued.
    total_cnt: u64,

    /// Total number of replies received.
    reply_cnt: u64,

    /// Total number of replies received in last print interval.
    chunk_cnt: u64,

    /// Latencies of requests in last print interval.
    chunk_lats: Vec<f64>,

    /// True if the next issue should be a retry.
    retrying: bool,

    /// Number of replies to wait for before allowing the next issue.
    slowdown: u64,

    /// Current timestamp.
    now: Instant,

    /// Interval ticker for open-loop.
    ticker: Interval,

    /// Current frequency in use.
    curr_freq: u64,
}

impl ClientBench {
    /// Creates a new benchmarking client.
    pub fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsBench;
                                     freq_target, length_s, put_ratio,
                                     value_size)?;
        if params.freq_target > 1000000 {
            return logged_err!("c"; "invalid params.freq_target '{}'",
                                   params.freq_target);
        }
        if params.length_s == 0 {
            return logged_err!("c"; "invalid params.length_s '{}'",
                                   params.length_s);
        }
        if params.put_ratio > 100 {
            return logged_err!("c"; "invalid params.put_ratio '{}'",
                                   params.put_ratio);
        }
        if params.value_size == 0 {
            return logged_err!("c"; "invalid params.value_size '{}'",
                                   params.value_size);
        }

        let value = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(params.value_size)
            .map(char::from)
            .collect();

        Ok(ClientBench {
            driver: DriverOpenLoop::new(endpoint, timeout),
            params,
            rng: rand::thread_rng(),
            value,
            total_cnt: 0,
            reply_cnt: 0,
            chunk_cnt: 0,
            chunk_lats: vec![],
            retrying: false,
            slowdown: 0,
            now: Instant::now(),
            ticker: time::interval(Duration::MAX),
            curr_freq: 0,
        })
    }

    /// Issues a random request.
    fn issue_rand_cmd(&mut self) -> Result<Option<RequestId>, SummersetError> {
        let key = KEYS_POOL[self.rng.gen_range(0..KEYS_POOL.len())].clone();
        if self.rng.gen_range(0..=100) <= self.params.put_ratio {
            self.driver.issue_put(&key, &self.value)
        } else {
            self.driver.issue_get(&key)
        }
    }

    /// Runs one iteration action of closed-loop style benchmark.
    #[allow(clippy::too_many_arguments)]
    async fn closed_loop_iter(&mut self) -> Result<(), SummersetError> {
        // send next request
        let req_id = if self.retrying {
            self.driver.issue_retry()?
        } else {
            self.issue_rand_cmd()?
        };

        self.retrying = req_id.is_none();
        if !self.retrying {
            self.total_cnt += 1;
        }

        // wait for the next reply
        if self.total_cnt > self.reply_cnt {
            let result = self.driver.wait_reply().await?;

            if let Some((_, _, lat)) = result {
                self.reply_cnt += 1;
                self.chunk_cnt += 1;
                let lat_us = lat.as_secs_f64() * 1000000.0;
                self.chunk_lats.push(lat_us);
            }
        }

        Ok(())
    }

    /// Runs one iteration action of open-loop style benchmark.
    #[allow(clippy::too_many_arguments)]
    async fn open_loop_iter(&mut self) -> Result<(), SummersetError> {
        tokio::select! {
            // prioritize receiving reply
            biased;

            // receive next reply
            result = self.driver.wait_reply() => {
                if let Some((_, _, lat)) = result? {
                    self.reply_cnt += 1;
                    self.chunk_cnt += 1;
                    let lat_us = lat.as_secs_f64() * 1000000.0;
                    self.chunk_lats.push(lat_us);

                    if self.slowdown > 0 {
                        self.slowdown -= 1;
                    }
                }
            }

            // send next request
            _ = self.ticker.tick(), if self.slowdown == 0 => {
                let req_id = if self.retrying {
                    self.driver.issue_retry()?
                } else {
                    self.issue_rand_cmd()?
                };

                self.retrying = req_id.is_none();
                if self.retrying && (self.total_cnt > self.reply_cnt) {
                    // too many pending requests, pause issuing for a while
                    self.slowdown = (self.total_cnt - self.reply_cnt) / 2;
                }
                if !self.retrying {
                    self.total_cnt += 1;
                }
            }
        }

        Ok(())
    }

    /// Drops the current interval ticker and create a new one using the
    /// current frequency.
    fn reset_ticker(&mut self) {
        if self.curr_freq == 0 {
            self.curr_freq = 1; // avoid division-by-zero
        } else if self.curr_freq > 1000000 {
            self.curr_freq = 1000000; // avoid going too high
        }

        let period = Duration::from_nanos(1000000000 / self.curr_freq);
        self.ticker = time::interval(period);
        self.ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);
    }

    /// Runs the adaptive benchmark for given time length.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;
        println!(
            "{:^11} | {:^12} | {:^12} | {:^8} : {:>8} / {:<8}",
            "Elapsed (s)", "Tpt (reqs/s)", "Lat (us)", "Freq", "Reply", "Total"
        );

        let start = Instant::now();
        self.now = start;
        let length = Duration::from_secs(self.params.length_s);

        let mut last_print = start;
        let (mut printed_1_100, mut printed_1_10) = (false, false);

        if self.params.freq_target > 0 {
            // is open-loop, set up interval ticker
            self.curr_freq = self.params.freq_target;
            self.reset_ticker();
        }

        self.total_cnt = 0;
        self.reply_cnt = 0;
        self.chunk_cnt = 0;
        self.chunk_lats.clear();
        self.retrying = false;
        self.slowdown = 0;

        // run for specified length
        while self.now.duration_since(start) < length {
            if self.params.freq_target == 0 {
                self.closed_loop_iter().await?;
            } else {
                self.open_loop_iter().await?;
            }

            self.now = Instant::now();

            // print statistics if print interval passed
            let elapsed = self.now.duration_since(start);
            let print_elapsed = self.now.duration_since(last_print);
            if print_elapsed >= *PRINT_INTERVAL
                || (!printed_1_100 && elapsed >= *PRINT_INTERVAL / 100)
                || (!printed_1_10 && elapsed >= *PRINT_INTERVAL / 10)
            {
                let tpt = (self.chunk_cnt as f64) / print_elapsed.as_secs_f64();
                let lat = if self.chunk_lats.is_empty() {
                    0.0
                } else {
                    self.chunk_lats.iter().sum::<f64>()
                        / (self.chunk_lats.len() as f64)
                };
                println!(
                    "{:>11.2} | {:>12.2} | {:>12.2} | {:>8} : {:>8} / {:<8}",
                    elapsed.as_secs_f64(),
                    tpt,
                    lat,
                    self.curr_freq,
                    self.reply_cnt,
                    self.total_cnt
                );
                last_print = self.now;
                self.chunk_cnt = 0;
                self.chunk_lats.clear();

                // adaptively adjust issuing frequency according to number of
                // pending requests; we try to maintain two ranges:
                //   - curr_freq in (1 ~ 1.25) * tpt
                //   - total_cnt in reply_cnt + (0.001 ~ 0.1) * tpt
                if self.params.freq_target > 0 {
                    let freq_changed = if self.slowdown > 0
                        || self.curr_freq as f64 > 1.25 * tpt
                        || self.reply_cnt as f64 + 0.1 * tpt
                            < self.total_cnt as f64
                    {
                        // frequency too high, ramp down
                        self.curr_freq -= (0.01 * tpt) as u64;
                        if self.curr_freq as f64 > 2.0 * tpt {
                            self.curr_freq /= 2;
                        } else if self.curr_freq as f64 > 1.5 * tpt {
                            self.curr_freq =
                                (0.67 * self.curr_freq as f64) as u64;
                        } else if self.curr_freq as f64 > 1.25 * tpt {
                            self.curr_freq =
                                (0.8 * self.curr_freq as f64) as u64;
                        }
                        true
                    } else if self.curr_freq as f64 <= tpt
                        || self.reply_cnt as f64 + 0.001 * tpt
                            >= self.total_cnt as f64
                    {
                        // frequency too conservative, ramp up a bit
                        self.curr_freq += (0.01 * tpt) as u64;
                        if self.curr_freq as f64 <= tpt {
                            self.curr_freq = tpt as u64;
                        }
                        if self.curr_freq > self.params.freq_target {
                            self.curr_freq = self.params.freq_target;
                        }
                        true
                    } else {
                        // roughly appropriate
                        false
                    };
                    if freq_changed {
                        self.reset_ticker();
                    }
                }

                // these two print triggers are for more effecitve adaptive
                // adjustment of frequency
                if !printed_1_100 && elapsed >= *PRINT_INTERVAL / 100 {
                    printed_1_100 = true;
                }
                if !printed_1_10 && elapsed >= *PRINT_INTERVAL / 10 {
                    printed_1_10 = true;
                }
            }
        }

        self.driver.leave(true).await?;
        Ok(())
    }
}
