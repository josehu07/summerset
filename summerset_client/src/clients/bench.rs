//! Benchmarking client using open-loop driver.

use std::collections::HashMap;

use crate::drivers::{DriverReply, DriverOpenLoop};

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;

use rand_distr::{Distribution, Normal, Uniform};

use rangemap::RangeMap;

use serde::Deserialize;

use tokio::time::{self, Duration, Instant, Interval, MissedTickBehavior};

use summerset::{
    GenericEndpoint, RequestId, SummersetError, pf_debug, pf_error, logged_err,
    parsed_config,
};

lazy_static! {
    /// Pool of keys to choose from.
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

    /// A very long pre-generated value string to get values from.
    static ref MOM_VALUE: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10 * 1024 * 1024)
        .map(char::from)
        .collect();

    /// Value size standard deviation to mean ratio.
    static ref STDEV_RATIO: f32 = 0.1;

    /// Statistics printing interval.
    static ref PRINT_INTERVAL: Duration = Duration::from_millis(100);
}

/// Mode parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsBench {
    /// Target frequency of issuing requests per second. Zero means closed-loop
    /// style client.
    pub freq_target: u64,

    /// Time length to benchmark in seconds.
    pub length_s: u64,

    /// Percentage of put requests.
    pub put_ratio: u8,

    /// String in one of the following formats:
    ///   - a single number: fixed value size in bytes
    ///   - "0:v0/t1:v1/t2:v2 ...": use value size v0 in timespan 0~t1, change
    ///                             to v1 at t1, then change to v2 at t2, etc.
    ///     This format also turns on using normal distributions.
    pub value_size: String,

    /// Do a uniformly distributed value size write for every some interval.
    /// Zero to turn off.
    pub unif_interval_ms: u64,

    /// Upper bound of uniform distribution to sample value size from.
    pub unif_upper_bound: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsBench {
    fn default() -> Self {
        ModeParamsBench {
            freq_target: 0,
            length_s: 30,
            put_ratio: 50,
            value_size: "1024".into(),
            unif_interval_ms: 500,
            unif_upper_bound: 256 * 1024,
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

    /// Map from time range (secs) -> specified value size mean value.
    value_size: RangeMap<u64, usize>,

    /// Using a normal distribution to choose a more dynamic value size?
    using_dist: bool,

    /// Map from value size mean -> normal distribution generator.
    normal_dist: HashMap<usize, Normal<f32>>,

    /// Fixed uniform distribution.
    unif_dist: Uniform<usize>,

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

    /// Start timestamp.
    start: Instant,

    /// Current timestamp.
    now: Instant,

    /// Last timestamp when a uniform distribution gets used.
    last_unif: Instant,

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
                                    value_size, unif_interval_ms, unif_upper_bound)?;
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
        if params.unif_interval_ms > 0 && params.unif_interval_ms < 100 {
            return logged_err!("c"; "invalid params.unif_interval_ms '{}'",
                                    params.unif_interval_ms);
        }
        if params.unif_upper_bound < 1024
            || params.unif_upper_bound > 1024 * 1024
        {
            return logged_err!("c"; "invalid params.unif_upper_bound '{}'",
                                    params.unif_upper_bound);
        }

        let (using_dist, value_size) =
            Self::parse_value_sizes(params.length_s, &params.value_size)?;
        let normal_dist = value_size
            .iter()
            .map(|(_, &vs)| {
                (
                    vs,
                    Normal::new(vs as f32, vs as f32 * *STDEV_RATIO).unwrap(),
                )
            })
            .collect();
        let unif_dist = Uniform::from(1..=params.unif_upper_bound);

        Ok(ClientBench {
            driver: DriverOpenLoop::new(endpoint, timeout),
            params,
            rng: rand::thread_rng(),
            value_size,
            using_dist,
            normal_dist,
            unif_dist,
            total_cnt: 0,
            reply_cnt: 0,
            chunk_cnt: 0,
            chunk_lats: vec![],
            retrying: false,
            slowdown: 0,
            start: Instant::now(),
            now: Instant::now(),
            last_unif: Instant::now(),
            ticker: time::interval(Duration::MAX),
            curr_freq: 0,
        })
    }

    /// Parses values sizes over time parameter into a rangemap of
    /// pre-generated values.
    fn parse_value_sizes(
        length_s: u64,
        s: &str,
    ) -> Result<(bool, RangeMap<u64, usize>), SummersetError> {
        let mut using_dist = false;
        let mut value_size = RangeMap::new();

        if s.is_empty() {
            return logged_err!("c"; "invalid params.value_size '{}'", s);
        } else if let Ok(size) = s.parse::<usize>() {
            // a single number
            if size == 0 {
                return logged_err!("c"; "size 0 found in params.value_size");
            }
            value_size.insert(0..u64::MAX, size);
        } else {
            // changes over time
            if !s.starts_with("0:") {
                return logged_err!("c"; "params.value_size must start with '0:'");
            }
            let (mut seg_start, mut seg_size) = (0, 0);
            for (i, seg) in s.split('/').enumerate() {
                if let Some(idx) = seg.find(':') {
                    let time = seg[..idx].parse::<u64>()?;
                    let size = seg[idx + 1..].parse::<usize>()?;
                    if size == 0 {
                        return logged_err!("c"; "size 0 found in params.value_size");
                    } else if time >= length_s {
                        return logged_err!("c"; "sec {} >= length_s {} in params.value_size",
                                                time, length_s);
                    }
                    if i == 0 {
                        debug_assert_eq!(time, 0);
                        seg_size = size;
                    } else {
                        debug_assert!(time > seg_start);
                        debug_assert!(seg_size > 0);
                        value_size.insert(seg_start..time, seg_size);
                        seg_start = time;
                        seg_size = size;
                    }
                } else {
                    return logged_err!("c"; "invalid params.value_size '{}'", s);
                }
            }
            debug_assert!(seg_size > 0);
            value_size.insert(seg_start..u64::MAX, seg_size);
            using_dist = true;
        }

        Ok((using_dist, value_size))
    }

    /// Pick a value of given size. If `using_dist` is on, uses that as mean
    /// size and goes through a normal distribution to sample a size.
    #[inline]
    fn gen_value_at_now(&mut self) -> Result<&'static str, SummersetError> {
        let curr_sec = self.now.duration_since(self.start).as_secs();
        let mut size = *self.value_size.get(&curr_sec).unwrap();
        if size > MOM_VALUE.len() {
            return logged_err!(self.driver.id; "value size {} too big", size);
        }

        if self.using_dist {
            loop {
                let f32_size =
                    self.normal_dist[&size].sample(&mut rand::thread_rng());
                size = f32_size.round() as usize;
                if size > 0 && size <= MOM_VALUE.len() {
                    break;
                }
            }
        }

        if self.params.unif_interval_ms > 0
            && self.now.duration_since(self.last_unif).as_millis()
                >= self.params.unif_interval_ms as u128
        {
            size = self.unif_dist.sample(&mut rand::thread_rng());
            self.last_unif = self.now;
        }

        Ok(&MOM_VALUE[..size])
    }

    /// Issues a random request.
    fn issue_rand_cmd(&mut self) -> Result<Option<RequestId>, SummersetError> {
        // randomly choose a key from key pool; key does not matter too much
        let key = KEYS_POOL[self.rng.gen_range(0..KEYS_POOL.len())].clone();
        if self.rng.gen_range(0..=100) <= self.params.put_ratio {
            // query the value to use for current timestamp
            let val = self.gen_value_at_now()?;
            self.driver.issue_put(&key, val)
        } else {
            self.driver.issue_get(&key)
        }
    }

    /// Leaves and reconnects to the service in case the previous server fails.
    async fn leave_reconnect(&mut self) -> Result<(), SummersetError> {
        pf_debug!(self.driver.id; "leave and reconnecting...");
        self.driver.leave(false).await?;
        self.driver.connect().await?;
        Ok(())
    }

    /// Runs one iteration action of closed-loop style benchmark.
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
            match result {
                DriverReply::Success { latency, .. } => {
                    self.reply_cnt += 1;
                    self.chunk_cnt += 1;
                    let lat_us = latency.as_secs_f64() * 1000000.0;
                    self.chunk_lats.push(lat_us);
                }

                DriverReply::Timeout => {
                    self.leave_reconnect().await?;
                }

                _ => {}
            }
        }

        Ok(())
    }

    /// Runs one iteration action of open-loop style benchmark.
    async fn open_loop_iter(&mut self) -> Result<(), SummersetError> {
        tokio::select! {
            // prioritize receiving reply
            biased;

            // receive next reply
            result = self.driver.wait_reply() => {
                match result? {
                    DriverReply::Success { latency, .. } => {
                        self.reply_cnt += 1;
                        self.chunk_cnt += 1;
                        let lat_us = latency.as_secs_f64() * 1000000.0;
                        self.chunk_lats.push(lat_us);

                        if self.slowdown > 0 {
                            self.slowdown -= 1;
                        }
                    }

                    DriverReply::Timeout => {
                        self.leave_reconnect().await?;
                    }

                    _ => {}
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

        self.start = Instant::now();
        self.now = self.start;
        let length = Duration::from_secs(self.params.length_s);

        let mut last_print = self.start;
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
        let mut elapsed = self.now.duration_since(self.start);
        while elapsed < length {
            if self.params.freq_target == 0 {
                self.closed_loop_iter().await?;
            } else {
                self.open_loop_iter().await?;
            }

            self.now = Instant::now();
            elapsed = self.now.duration_since(self.start);

            // print statistics if print interval passed
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
