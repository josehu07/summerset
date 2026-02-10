//! Benchmarking client using open-loop driver.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::LazyLock;

use rand::Rng;
use rand::distr::Alphanumeric;
use rand_distr::{Distribution, Normal, Uniform};
use rangemap::RangeMap;
use serde::Deserialize;
use summerset::{
    CommandResult, GenericEndpoint, RequestId, SummersetError, logged_err,
    parsed_config, pf_debug, pf_error, pf_info, pf_warn,
};
use tokio::time::{self, Duration, Instant, Interval, MissedTickBehavior};

use crate::drivers::{DriverOpenLoop, DriverReply};

/// Fixed length in bytes of key.
const KEY_LEN: usize = 8;

/// Max length in bytes of value.
const MAX_VAL_LEN: usize = 16 * 1024 * 1024; // 16 MB

/// Statistics printing interval.
const PRINT_INTERVAL: Duration = Duration::from_millis(100);
// const PRINT_INTERVAL: Duration = Duration::from_millis(250);

/// Finer-grained statistics printing interval.
const FINE_PRINT_INTERVAL: Duration = Duration::from_millis(5);

/// A very long pre-generated value string to get values from.
static MOM_VALUE: LazyLock<String> = LazyLock::new(|| {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(MAX_VAL_LEN)
        .map(char::from)
        .collect()
});

/// Mode parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsBench {
    /// If non empty, path to write outputs to.
    pub output_path: String,

    /// If non empty, use finer-grained printing time intervals. If so, then
    /// `output_path` Should be a memory-backed path (e.g. tmpfs) to avoid
    /// disturbing exper accuracy.
    pub fine_output: bool,

    /// Target frequency of issuing requests per second. Zero means closed-loop
    /// style client.
    pub freq_target: u64,

    /// Time length to benchmark in seconds. If zero and `ycsb_trace` is valid,
    /// will run the trace exactly once (else if trace is given, will repeat it
    /// indefinitely).
    pub length_s: u64,

    /// Percentage of put requests.
    pub put_ratio: u8,

    /// Path to cleaned YCSB trace file.
    /// Having a valid path here overwrites the `put_ratio` setting.
    pub ycsb_trace: String,

    /// String in one of the following formats:
    ///   - a single number: fixed value size in bytes
    ///   - "0:v0/t1:v1/t2:v2 ...": use value size v0 in timespan 0~t1, change
    ///     to v1 at t1, then change to v2 at t2, etc.
    pub value_size: String,

    /// Number of keys to choose from.
    pub num_keys: usize,

    /// Whether to generate keys randomly or use predetermined sequence from 0.
    pub use_random_keys: bool,

    /// Whether to skip the preloading phase that loads values for all keys.
    pub skip_preloading: bool,

    /// If non-zero, use a normal distribution of this standard deviation
    /// ratio for every write command.
    pub norm_stdev_ratio: f32,

    /// Do a uniformly distributed value size write for every some interval.
    /// Set to 0 to turn off. Set to 1 to let every request size go through a
    /// uniform distribution (overwriting the normal dist param).
    pub unif_interval_ms: u64,

    /// Upper bound of uniform distribution to sample value size from.
    pub unif_upper_bound: usize,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsBench {
    fn default() -> Self {
        ModeParamsBench {
            output_path: "".into(),
            fine_output: false,
            freq_target: 0,
            length_s: 30,
            put_ratio: 50,
            ycsb_trace: "".into(),
            value_size: "1024".into(),
            num_keys: 5,
            use_random_keys: false,
            skip_preloading: false,
            norm_stdev_ratio: 0.0,
            unif_interval_ms: 0,
            unif_upper_bound: 128 * 1024,
        }
    }
}

/// Benchmarking client struct.
pub(crate) struct ClientBench {
    /// Open-loop request driver.
    driver: DriverOpenLoop,

    /// Mode parameters struct.
    params: ModeParamsBench,

    /// Output file.
    output_file: Option<File>,

    /// List of randomly generated keys if in synthetic mode.
    keys_pool: Option<Vec<String>>,

    /// Trace of (op, key, vlen) pairs to (repeatedly) replay.
    trace_vec: Option<Vec<(bool, String, usize)>>,

    /// Trace operation index to play next.
    trace_idx: usize,

    /// Map from time range (secs) -> specified value size mean value.
    value_size: RangeMap<u64, usize>,

    /// Map from value size mean -> normal distribution generator.
    norm_dist: Option<HashMap<usize, Normal<f32>>>,

    /// Fixed uniform distribution.
    unif_dist: Option<Uniform<usize>>,

    /// Total number of requests issued.
    total_cnt: u64,

    /// Total number of replies received.
    reply_cnt: u64,

    /// Total number of replies received in last print interval.
    chunk_cnt: u64,

    /// Latencies of Put requests in last print interval.
    chunk_wlats: Vec<f64>,

    /// Latencies of Get requests in last print interval.
    chunk_rlats: Vec<f64>,

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
    pub(crate) fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsBench;
                                    output_path, fine_output,
                                    freq_target, length_s,
                                    put_ratio, ycsb_trace,
                                    value_size, num_keys,
                                    use_random_keys, skip_preloading,
                                    norm_stdev_ratio, unif_interval_ms,
                                    unif_upper_bound)?;
        if params.freq_target > 1_000_000 {
            return logged_err!(
                "invalid params.freq_target '{}'",
                params.freq_target
            );
        }
        if params.length_s == 0 && params.ycsb_trace.is_empty() {
            return logged_err!(
                "invalid params.length_s '{}'",
                params.length_s
            );
        }
        if params.put_ratio > 100 {
            return logged_err!(
                "invalid params.put_ratio '{}'",
                params.put_ratio
            );
        }
        if params.num_keys == 0 {
            return logged_err!(
                "invalid params.num_keys '{}'",
                params.num_keys
            );
        }
        if params.norm_stdev_ratio < 0.0 {
            return logged_err!(
                "invalid params.norm_stdev_ratio '{}'",
                params.norm_stdev_ratio
            );
        }
        if params.unif_interval_ms > 1 && params.unif_interval_ms < 100 {
            return logged_err!(
                "invalid params.unif_interval_ms '{}'",
                params.unif_interval_ms
            );
        }
        if params.unif_upper_bound < 1024
            || params.unif_upper_bound > MAX_VAL_LEN
        {
            return logged_err!(
                "invalid params.unif_upper_bound '{}'",
                params.unif_upper_bound
            );
        }

        let output_file = if params.output_path.is_empty() {
            None
        } else {
            let output_path = Path::new(&params.output_path);
            if fs::exists(output_path)? {
                pf_warn!(
                    "overwriting existing output file '{}'",
                    params.output_path
                );
            } else {
                fs::create_dir_all(
                    output_path
                        .parent()
                        .expect("output_path should have parent dir"),
                )?;
            }
            Some(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(output_path)?,
            )
        };

        let keys_pool = if params.ycsb_trace.is_empty() {
            let mut pool = Vec::with_capacity(params.num_keys);
            for i in 0..params.num_keys {
                pool.push(Self::compose_ith_key(i, params.use_random_keys));
            }
            Some(pool)
        } else {
            None
        };

        let trace_vec = if !params.ycsb_trace.is_empty() {
            Some(Self::load_trace_file(&params.ycsb_trace)?)
        } else {
            None
        };

        let value_size =
            Self::parse_value_sizes(params.length_s, &params.value_size)?;
        let norm_dist = if params.norm_stdev_ratio > 0.0 {
            Some(
                value_size
                    .iter()
                    .map(|(_, &vs)| {
                        (
                            vs,
                            Normal::new(
                                vs as f32,
                                vs as f32 * params.norm_stdev_ratio,
                            )
                            .unwrap(),
                        )
                    })
                    .collect(),
            )
        } else {
            None
        };
        let unif_dist = if params.unif_interval_ms > 0 {
            Some(Uniform::new_inclusive(8, params.unif_upper_bound)?)
        } else {
            None
        };

        Ok(ClientBench {
            driver: DriverOpenLoop::new(endpoint, timeout),
            params,
            output_file,
            keys_pool,
            trace_vec,
            trace_idx: 0,
            value_size,
            norm_dist,
            unif_dist,
            total_cnt: 0,
            reply_cnt: 0,
            chunk_cnt: 0,
            chunk_wlats: vec![],
            chunk_rlats: vec![],
            retrying: false,
            slowdown: 0,
            start: Instant::now(),
            now: Instant::now(),
            last_unif: Instant::now(),
            ticker: time::interval(Duration::MAX),
            curr_freq: 0,
        })
    }

    /// Loads in given input trace file. Expects it to be a cleaned YCSB trace.
    pub(crate) fn load_trace_file(
        path: &str,
    ) -> Result<Vec<(bool, String, usize)>, SummersetError> {
        let file = File::open(path)?;
        let mut trace_vec = vec![];
        for line in BufReader::new(file).lines() {
            let line = line?;
            if !(line.is_empty()) {
                let mut seg_idx = 0;
                let mut read = false;
                let mut key = String::new();
                let mut vlen = 0;
                for seg in line.split(' ') {
                    if seg.is_empty() {
                        continue;
                    }
                    if seg_idx == 0 {
                        // opcode
                        read = if seg == "READ" || seg == "SCAN" {
                            true
                        } else if seg == "UPDATE" || seg == "INSERT" {
                            false
                        } else {
                            return logged_err!(
                                "unrecognized trace op '{}'",
                                seg
                            );
                        }
                    } else if seg_idx == 1 {
                        // key
                        key = seg.into();
                    } else if seg_idx == 2 {
                        // vlen; if not present, will store 0 and the runner
                        // will use the value size params instead
                        vlen = seg.parse::<usize>()?;
                        break;
                    }
                    seg_idx += 1;
                }
                trace_vec.push((read, key, vlen));
            }
        }
        Ok(trace_vec)
    }

    /// Returns the key of index `i`.
    pub(crate) fn compose_ith_key(i: usize, random_keys: bool) -> String {
        if random_keys {
            rand::rng()
                .sample_iter(&Alphanumeric)
                .take(KEY_LEN)
                .map(char::from)
                .collect()
        } else {
            // the format recognizable by key-range-based utilities
            format!("k{:0w$}", i, w = KEY_LEN - 1)
        }
    }

    /// Parses values sizes over time parameter into a rangemap of
    /// pre-generated values.
    pub(crate) fn parse_value_sizes(
        length_s: u64,
        s: &str,
    ) -> Result<RangeMap<u64, usize>, SummersetError> {
        let mut value_size = RangeMap::new();
        if s.is_empty() {
            return logged_err!("invalid params.value_size '{}'", s);
        } else if let Ok(size) = s.parse::<usize>() {
            // a single number
            if size == 0 {
                return logged_err!("size 0 found in params.value_size");
            }
            value_size.insert(0..u64::MAX, size);
        } else {
            // changes over time
            if !s.starts_with("0:") {
                return logged_err!("params.value_size must start with '0:'");
            }
            let (mut seg_start, mut seg_size) = (0, 0);
            for (i, seg) in s.split('/').enumerate() {
                if let Some(idx) = seg.find(':') {
                    let time = seg[..idx].parse::<u64>()?;
                    let size = seg[idx + 1..].parse::<usize>()?;
                    if size == 0 {
                        return logged_err!(
                            "size 0 found in params.value_size"
                        );
                    } else if time >= length_s {
                        return logged_err!(
                            "sec {} >= length_s {} in params.value_size",
                            time,
                            length_s
                        );
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
                    return logged_err!("invalid params.value_size '{}'", s);
                }
            }
            debug_assert!(seg_size > 0);
            value_size.insert(seg_start..u64::MAX, seg_size);
        }
        Ok(value_size)
    }

    /// Pick a value of given size. If `using_dist` is on, uses that as mean
    /// size and goes through a normal distribution to sample a size.
    fn gen_value_at_now(&mut self) -> Result<&'static str, SummersetError> {
        let curr_sec = self.now.duration_since(self.start).as_secs();
        let mut size = *self.value_size.get(&curr_sec).unwrap();

        // go through a probability distribution?
        if let Some(unif_dist) = self.unif_dist.as_ref()
            && self.params.unif_interval_ms > 1
            && self.now.duration_since(self.last_unif).as_millis()
                >= self.params.unif_interval_ms as u128
        {
            // an injected uniform distribution sample
            size = unif_dist.sample(&mut rand::rng());
            self.last_unif = self.now;
        } else if let Some(unif_dist) = self.unif_dist.as_ref()
            && self.params.unif_interval_ms == 1
        {
            // forcing a uniform distribution, ignoring all other settings
            size = unif_dist.sample(&mut rand::rng());
        } else if let Some(norm_dist) = self.norm_dist.as_ref() {
            // a normal distribution with current size as mean
            loop {
                let f32_size = norm_dist[&size].sample(&mut rand::rng());
                size = f32_size.round() as usize;
                if size > 0 && size <= MAX_VAL_LEN {
                    break;
                }
            }
        }

        debug_assert!(size <= MAX_VAL_LEN);
        Ok(&MOM_VALUE[..size])
    }

    /// Issues a random request.
    fn issue_rand_cmd(&mut self) -> Result<Option<RequestId>, SummersetError> {
        debug_assert!(self.keys_pool.is_some());
        let key = self
            .keys_pool
            .as_ref()
            .unwrap()
            .get(rand::rng().random_range(0..self.params.num_keys))
            .unwrap()
            .clone();
        if rand::rng().random_range(0..100) < self.params.put_ratio {
            // query the value to use for current timestamp
            let val = self.gen_value_at_now()?;
            self.driver.issue_put(&key, val)
        } else {
            self.driver.issue_get(&key)
        }
    }

    /// Issues a request following the trace vec.
    fn issue_trace_cmd(&mut self) -> Result<Option<RequestId>, SummersetError> {
        debug_assert!(self.trace_vec.is_some());
        let (read, key, vlen) = self
            .trace_vec
            .as_ref()
            .unwrap()
            .get(self.trace_idx)
            .unwrap()
            .clone();
        // update trace_idx, rounding back to 0 if reaching end
        self.trace_idx += 1;
        if self.trace_idx == self.trace_vec.as_ref().unwrap().len() {
            self.trace_idx = 0;
        }
        if read {
            self.driver.issue_get(&key)
        } else if vlen == 0 {
            // query the value to use for current timestamp
            let val = self.gen_value_at_now()?;
            self.driver.issue_put(&key, val)
        } else {
            // use vlen in trace
            let val = &MOM_VALUE[..vlen];
            self.driver.issue_put(&key, val)
        }
    }

    /// Leaves and reconnects to the service in case the previous server fails.
    async fn leave_reconnect(&mut self) -> Result<(), SummersetError> {
        pf_debug!("leave and reconnecting...");
        self.driver.leave(false).await?;
        self.driver.connect().await?;
        Ok(())
    }

    /// Runs one iteration action of closed-loop style benchmark.
    async fn closed_loop_iter(&mut self) -> Result<(), SummersetError> {
        // send next request
        let req_id = if self.retrying {
            self.driver.issue_retry()?
        } else if self.trace_vec.is_some() {
            self.issue_trace_cmd()?
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
                DriverReply::Success {
                    latency,
                    cmd_result,
                    ..
                } => {
                    self.reply_cnt += 1;
                    self.chunk_cnt += 1;

                    let lat_us = latency.as_secs_f64() * 1_000_000.0;
                    match cmd_result {
                        CommandResult::Put { .. } => {
                            self.chunk_wlats.push(lat_us);
                        }
                        CommandResult::Get { .. } => {
                            self.chunk_rlats.push(lat_us);
                        }
                    }
                }

                DriverReply::Timeout | DriverReply::Failure => {
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
                    DriverReply::Success { latency, cmd_result, .. } => {
                        self.reply_cnt += 1;
                        self.chunk_cnt += 1;

                        let lat_us = latency.as_secs_f64() * 1_000_000.0;
                        match cmd_result {
                            CommandResult::Put { .. } => {
                                self.chunk_wlats.push(lat_us);
                            }
                            CommandResult::Get { .. } => {
                                self.chunk_rlats.push(lat_us);
                            }
                        }

                        if self.slowdown > 0 {
                            self.slowdown -= 1;
                        }
                    }

                    DriverReply::Timeout | DriverReply::Failure => {
                        self.leave_reconnect().await?;
                    }

                    _ => {}
                }
            }

            // send next request
            _ = self.ticker.tick(), if self.slowdown == 0 => {
                let req_id = if self.retrying {
                    self.driver.issue_retry()?
                } else if self.trace_vec.is_some() {
                    self.issue_trace_cmd()?
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

    /// Runs through the loaded trace exactly once.
    async fn trace_once(&mut self) -> Result<(), SummersetError> {
        debug_assert!(self.trace_vec.is_some());
        loop {
            self.closed_loop_iter().await?;
            if self.trace_idx == 0 {
                return Ok(());
            }
        }
    }

    /// If not using trace, preload values for all keys. Allowing a few retries
    /// in cases of redirections and if servers weren't fully launched up.
    async fn do_preload(&mut self) -> Result<(), SummersetError> {
        let val = self.gen_value_at_now()?;

        if let Some(keys_pool) = &self.keys_pool {
            pf_info!("preloading all keys...");

            let mut retries = 10; // hardcoded
            for key in keys_pool {
                loop {
                    while self.driver.issue_put(key, val)?.is_none() {}

                    match self.driver.wait_reply().await? {
                        DriverReply::Success { .. } => {
                            break;
                        }
                        _ => {
                            retries -= 1;
                            if retries == 0 {
                                return logged_err!(
                                    "unsuccessful preload reply, no retries left"
                                );
                            }
                            time::sleep(Duration::from_millis(200)).await;
                        }
                    }
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
        }

        let period = Duration::from_nanos(1_000_000_000 / self.curr_freq);
        self.ticker = time::interval(period);
        self.ticker
            .set_missed_tick_behavior(MissedTickBehavior::Skip);
    }

    /// Runs the adaptive benchmark for given time length.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;

        self.start = Instant::now();
        self.now = self.start;
        let length = Duration::from_secs(self.params.length_s);

        // if length is 0, do the special action of running the trace exactly
        // once; this is useful for e.g. loading YCSB
        if self.params.length_s == 0 && self.trace_vec.is_some() {
            pf_info!("feeding trace once...");
            self.trace_once().await?;
            return Ok(());
        }

        // if not using trace, preload values for all keys
        if self.trace_vec.is_none() && !self.params.skip_preloading {
            self.do_preload().await?;
        }

        if self.params.freq_target > 0 {
            // is open-loop, set up interval ticker
            self.curr_freq = self.params.freq_target;
            self.reset_ticker();
        }

        let mut last_print = self.start;
        // let (mut printed_1_100, mut printed_1_10) = (false, false);
        self.total_cnt = 0;
        self.reply_cnt = 0;
        self.chunk_cnt = 0;
        self.chunk_wlats.clear();
        self.chunk_rlats.clear();
        self.retrying = false;
        self.slowdown = 0;

        let header = format!(
            "{:^11} | {:^12} | {:^12} : {:^12} ~ {:^12} | {:^8} : {:>8} / {:<8}",
            "Elapsed (s)",
            "Tput (ops/s)",
            "Lat (us)",
            "WLat (us)",
            "RLat (us)",
            "Freq",
            "Reply",
            "Total"
        );
        if let Some(output_file) = self.output_file.as_mut() {
            writeln!(output_file, "{}", header)?;
        } else {
            println!("{}", header);
        }
        pf_info!("starting benchmark...");

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
            let print_interval = if self.params.fine_output {
                &FINE_PRINT_INTERVAL
            } else {
                &PRINT_INTERVAL
            };
            if &print_elapsed >= print_interval
            // || (!printed_1_100 && elapsed >= PRINT_INTERVAL / 100)
            // || (!printed_1_10 && elapsed >= PRINT_INTERVAL / 10)
            {
                let tput =
                    (self.chunk_cnt as f64) / print_elapsed.as_secs_f64();
                let wlat = if self.chunk_wlats.is_empty() {
                    0.0
                } else {
                    self.chunk_wlats.iter().sum::<f64>()
                        / self.chunk_wlats.len() as f64
                };
                let rlat = if self.chunk_rlats.is_empty() {
                    0.0
                } else {
                    self.chunk_rlats.iter().sum::<f64>()
                        / self.chunk_rlats.len() as f64
                };
                let lat =
                    if self.chunk_wlats.len() + self.chunk_rlats.len() == 0 {
                        0.0
                    } else {
                        (wlat * self.chunk_wlats.len() as f64
                            + rlat * self.chunk_rlats.len() as f64)
                            / (self.chunk_wlats.len() + self.chunk_rlats.len())
                                as f64
                    };

                let line = format!(
                    "{:>11.2} | {:>12.2} | {:>12.2} : {:>12.2} ~ {:>12.2} | {:>8} : {:>8} / {:<8}",
                    elapsed.as_secs_f64(),
                    tput,
                    lat,
                    wlat,
                    rlat,
                    self.curr_freq,
                    self.reply_cnt,
                    self.total_cnt
                );
                if let Some(output_file) = self.output_file.as_mut() {
                    writeln!(output_file, "{}", line)?;
                } else {
                    println!("{}", line);
                }

                last_print = self.now;
                self.chunk_cnt = 0;
                self.chunk_wlats.clear();
                self.chunk_rlats.clear();

                // THE FOLLOWING IS EXPERIMENTAL:
                // adaptively adjust issuing frequency according to number of
                // pending requests; we try to maintain two ranges:
                //   - curr_freq in (1 ~ 1.25) * tput
                //   - total_cnt in reply_cnt + (0.001 ~ 0.1) * tput
                // if self.params.freq_target > 0 {
                //     let freq_changed = if self.slowdown > 0
                //         || self.curr_freq as f64 > 1.25 * tput
                //         || self.reply_cnt as f64 + 0.1 * tput
                //             < self.total_cnt as f64
                //     {
                //         // frequency too high, ramp down
                //         self.curr_freq -= (0.01 * tput) as u64;
                //         if self.curr_freq as f64 > 2.0 * tput {
                //             self.curr_freq /= 2;
                //         } else if self.curr_freq as f64 > 1.5 * tput {
                //             self.curr_freq =
                //                 (0.67 * self.curr_freq as f64) as u64;
                //         } else if self.curr_freq as f64 > 1.25 * tput {
                //             self.curr_freq =
                //                 (0.8 * self.curr_freq as f64) as u64;
                //         }
                //         true
                //     } else if self.curr_freq as f64 <= tput
                //         || self.reply_cnt as f64 + 0.001 * tput
                //             >= self.total_cnt as f64
                //     {
                //         // frequency too conservative, ramp up a bit
                //         self.curr_freq += (0.01 * tput) as u64;
                //         if self.curr_freq as f64 <= tput {
                //             self.curr_freq = tput as u64;
                //         }
                //         if self.curr_freq > self.params.freq_target {
                //             self.curr_freq = self.params.freq_target;
                //         }
                //         true
                //     } else {
                //         // roughly appropriate
                //         false
                //     };
                //     if freq_changed {
                //         self.reset_ticker();
                //     }
                // }

                // these two print triggers are for more effecitve adaptive
                // adjustment of frequency
                // if !printed_1_100 && elapsed >= PRINT_INTERVAL / 100 {
                //     printed_1_100 = true;
                // }
                // if !printed_1_10 && elapsed >= PRINT_INTERVAL / 10 {
                //     printed_1_10 = true;
                // }
            }
        }

        // self.driver.leave(true).await?;
        Ok(())
    }
}
