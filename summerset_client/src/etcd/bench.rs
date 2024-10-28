//! Benchmarking client for etcd.

use crate::{ClientBench, EtcdKvClient, ModeParamsBench};

use lazy_static::lazy_static;

use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;
use rand::Rng;

use rangemap::RangeMap;

use tokio::time::{Duration, Instant};

use summerset::{logged_err, parsed_config, pf_error, SummersetError};

/// Fixed length in bytes of key.
const KEY_LEN: usize = 8;

/// Max length in bytes of value.
const MAX_VAL_LEN: usize = 1536 * 1024; // 1.5 MB as of etcd limit

/// Statistics printing interval.
const PRINT_INTERVAL: Duration = Duration::from_millis(100);

lazy_static! {
    /// A very long pre-generated value string to get values from.
    static ref MOM_VALUE: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(MAX_VAL_LEN)
        .map(char::from)
        .collect();
}

/// Benchmarking client struct (only supports closed-loop).
pub(crate) struct EtcdBench {
    /// etcd KV client.
    kv_client: EtcdKvClient,

    /// Mode parameters struct.
    params: ModeParamsBench,

    /// Random number generator.
    rng: ThreadRng,

    /// List of randomly generated keys if in synthetic mode.
    keys_pool: Option<Vec<String>>,

    /// Trace of (op, key) pairs to (repeatedly) replay.
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

    /// Latencies of requests in last print interval.
    chunk_lats: Vec<f64>,

    /// Start timestamp.
    start: Instant,

    /// Current timestamp.
    now: Instant,
}

impl EtcdBench {
    /// Create a new benchmarking client.
    pub fn new(
        kv_client: EtcdKvClient,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsBench;
                                    freq_target, length_s,
                                    put_ratio, ycsb_trace,
                                    value_size, num_keys,
                                    norm_stdev_ratio,
                                    unif_interval_ms, unif_upper_bound)?;
        if params.freq_target > 0 {
            return logged_err!(
                "params.freq_target '{}' > 0; EtcdBench only supports closed-loop",
                params.freq_target
            );
        }
        if params.length_s == 0 {
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

        let keys_pool = if params.ycsb_trace.is_empty() {
            let mut pool = Vec::with_capacity(params.num_keys);
            for _ in 0..params.num_keys {
                pool.push(
                    rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(KEY_LEN)
                        .map(char::from)
                        .collect(),
                );
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

        Ok(EtcdBench {
            kv_client,
            params,
            rng: rand::thread_rng(),
            keys_pool,
            trace_vec,
            trace_idx: 0,
            value_size,
            total_cnt: 0,
            valid_cnt: 0,
            chunk_cnt: 0,
            chunk_lats: vec![],
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

    /// Make a random request.
    async fn do_rand_cmd(&mut self) -> Result<(), SummersetError> {
        debug_assert!(self.keys_pool.is_some());
        let key = self
            .keys_pool
            .as_ref()
            .unwrap()
            .get(self.rng.gen_range(0..self.params.num_keys))
            .unwrap()
            .clone();
        if self.rng.gen_range(0..=100) <= self.params.put_ratio {
            // query the value to use for current timestamp
            let val = self.gen_value_at_now()?;
            self.kv_client.put(&key, val).await.map(|_| ())
        } else {
            self.kv_client.get(&key).await.map(|_| ())
        }
    }

    /// Make a request following the trace vec.
    async fn do_trace_cmd(&mut self) -> Result<(), SummersetError> {
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
            self.kv_client.get(&key).await.map(|_| ())
        } else if vlen == 0 {
            // query the value to use for current timestamp
            let val = self.gen_value_at_now()?;
            self.kv_client.put(&key, val).await.map(|_| ())
        } else {
            // use vlen in trace
            let val = &MOM_VALUE[..vlen];
            self.kv_client.put(&key, val).await.map(|_| ())
        }
    }

    /// Runs one iteration action of closed-loop style benchmark.
    async fn closed_loop_iter(&mut self) -> Result<(), SummersetError> {
        let ts = Instant::now();

        // do the next request
        self.total_cnt += 1;
        if self.trace_vec.is_some() {
            self.do_trace_cmd().await?;
        } else {
            self.do_rand_cmd().await?;
        };

        self.valid_cnt += 1;
        self.chunk_cnt += 1;

        let lat_us =
            Instant::now().duration_since(ts).as_secs_f64() * 1_000_000.0;
        self.chunk_lats.push(lat_us);

        Ok(())
    }

    /// Run the benchmark for given time length.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        self.kv_client.connect().await?;
        println!(
            "{:^11} | {:^12} | {:^12} | {:>8}",
            "Elapsed (s)", "Tpt (reqs/s)", "Lat (us)", "Total"
        );

        self.start = Instant::now();
        self.now = self.start;
        let length = Duration::from_secs(self.params.length_s);
        let mut last_print = self.start;

        self.total_cnt = 0;
        self.chunk_cnt = 0;
        self.chunk_lats.clear();

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
                let tpt = (self.chunk_cnt as f64) / print_elapsed.as_secs_f64();
                let lat = if self.chunk_lats.is_empty() {
                    0.0
                } else {
                    self.chunk_lats.iter().sum::<f64>()
                        / (self.chunk_lats.len() as f64)
                };
                println!(
                    "{:>11.2} | {:>12.2} | {:>12.2} | {:>8}",
                    elapsed.as_secs_f64(),
                    tpt,
                    lat,
                    self.total_cnt,
                );
                last_print = self.now;
                self.chunk_cnt = 0;
                self.chunk_lats.clear();
            }
        }

        self.kv_client.leave();
        Ok(())
    }
}
