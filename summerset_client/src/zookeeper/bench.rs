//! Benchmarking client for ZooKeeper.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;

use rand::Rng;
use rand::distr::Alphanumeric;
use rangemap::RangeMap;
use summerset::{
    SummersetError, logged_err, parsed_config, pf_error, pf_info, pf_warn,
};
use tokio::time::{Duration, Instant};

use crate::{ClientBench, ModeParamsBench, ZooKeeperSession};

/// Max length in bytes of value.
const MAX_VAL_LEN: usize = 1024 * 1024; // 1 MB as of ZooKeeper limit

/// Statistics printing interval.
const PRINT_INTERVAL: Duration = Duration::from_millis(100);

/// A very long pre-generated value string to get values from.
static MOM_VALUE: LazyLock<String> = LazyLock::new(|| {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(MAX_VAL_LEN)
        .map(char::from)
        .collect()
});

/// Benchmarking client struct (only supports closed-loop).
pub(crate) struct ZooKeeperBench {
    /// ZooKeeper session client.
    session: ZooKeeperSession,

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

    /// Map from time range (secs) -> specified value size.
    value_size: RangeMap<u64, usize>,

    /// Total number of requests made.
    total_cnt: u64,

    /// Total number of successful replies received.
    valid_cnt: u64,

    /// Total number of replies received in last print interval.
    chunk_cnt: u64,

    /// Latencies of Put requests in last print interval.
    chunk_wlats: Vec<f64>,

    /// Latencies of Get requests in last print interval.
    chunk_rlats: Vec<f64>,

    /// Start timestamp.
    start: Instant,

    /// Current timestamp.
    now: Instant,
}

impl ZooKeeperBench {
    /// Create a new benchmarking client.
    pub fn new(
        session: ZooKeeperSession,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsBench;
                                    output_path, freq_target, length_s,
                                    put_ratio, ycsb_trace, value_size,
                                    num_keys, use_random_keys,
                                    norm_stdev_ratio, unif_interval_ms,
                                    unif_upper_bound)?;
        if params.freq_target > 0 {
            return logged_err!(
                "params.freq_target '{}' > 0; ZooKeeperBench only supports closed-loop",
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
                pool.push(ClientBench::compose_ith_key(
                    i,
                    params.use_random_keys,
                ));
            }
            Some(pool)
        } else {
            None
        };

        let trace_vec = if !params.ycsb_trace.is_empty() {
            Some(ClientBench::load_trace_file(&params.ycsb_trace)?)
        } else {
            None
        };

        let value_size = ClientBench::parse_value_sizes(
            params.length_s,
            &params.value_size,
        )?;

        Ok(ZooKeeperBench {
            session,
            params,
            output_file,
            keys_pool,
            trace_vec,
            trace_idx: 0,
            value_size,
            total_cnt: 0,
            valid_cnt: 0,
            chunk_cnt: 0,
            chunk_wlats: vec![],
            chunk_rlats: vec![],
            start: Instant::now(),
            now: Instant::now(),
        })
    }

    /// Pick a value of given size. If `using_dist` is on, uses that as mean
    /// size and goes through a normal distribution to sample a size.
    fn gen_value_at_now(&mut self) -> Result<&'static str, SummersetError> {
        let curr_sec = self.now.duration_since(self.start).as_secs();
        let size = *self.value_size.get(&curr_sec).unwrap();
        debug_assert!(size <= MAX_VAL_LEN);
        Ok(&MOM_VALUE[..size])
    }

    /// Make a random request. Returns `true` if a read command was done or
    /// `false` if a write.
    async fn do_rand_cmd(&mut self) -> Result<bool, SummersetError> {
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
            self.session.set(&key, val).await?;
            Ok(false)
        } else {
            self.session.get(&key).await?;
            Ok(true)
        }
    }

    /// Make a request following the trace vec. Returns `true` if a read
    /// command was done or `false` if a write.
    async fn do_trace_cmd(&mut self) -> Result<bool, SummersetError> {
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
            self.session.get(&key).await?;
            Ok(true)
        } else if vlen == 0 {
            // query the value to use for current timestamp
            let val = self.gen_value_at_now()?;
            self.session.set(&key, val).await?;
            Ok(false)
        } else {
            // use vlen in trace
            let val = &MOM_VALUE[..vlen];
            self.session.set(&key, val).await?;
            Ok(false)
        }
    }

    /// Runs one iteration action of closed-loop style benchmark.
    async fn closed_loop_iter(&mut self) -> Result<(), SummersetError> {
        let ts = Instant::now();

        // do the next request
        self.total_cnt += 1;
        let is_read = if self.trace_vec.is_some() {
            self.do_trace_cmd().await?
        } else {
            self.do_rand_cmd().await?
        };

        self.valid_cnt += 1;
        self.chunk_cnt += 1;

        let lat_us =
            Instant::now().duration_since(ts).as_secs_f64() * 1_000_000.0;
        if is_read {
            self.chunk_rlats.push(lat_us);
        } else {
            self.chunk_wlats.push(lat_us);
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

    /// Run the benchmark for given time length.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        self.session.connect().await?;

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

        let mut last_print = self.start;

        self.total_cnt = 0;
        self.valid_cnt = 0;
        self.chunk_cnt = 0;
        self.chunk_wlats.clear();
        self.chunk_rlats.clear();

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
            assert_eq!(self.params.freq_target, 0);
            self.closed_loop_iter().await?;

            self.now = Instant::now();
            elapsed = self.now.duration_since(self.start);

            // print statistics if print interval passed
            let print_elapsed = self.now.duration_since(last_print);
            if print_elapsed >= PRINT_INTERVAL {
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
                    0,
                    self.valid_cnt,
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
            }
        }

        self.session.leave();
        Ok(())
    }
}
