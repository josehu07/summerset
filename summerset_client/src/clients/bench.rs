//! Benchmarking client using open-loop driver.

use crate::drivers::DriverOpenLoop;

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::ThreadRng;

use serde::Deserialize;

use tokio::time::{Duration, Instant};

use summerset::{
    GenericClient, ClientId, Command, SummersetError, pf_error, logged_err,
    parsed_config,
};

lazy_static! {
    /// Pool of keys to choose from.
    // TODO: maybe use a dynamic pool of keys
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
    static ref TARGET_BATCH_DUR: Duration = Duration::from_millis(100);
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
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsBench {
    fn default() -> Self {
        ModeParamsBench {
            init_batch_size: 100,
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
}

impl ClientBench {
    /// Creates a new benchmarking client.
    pub fn new(
        id: ClientId,
        stub: Box<dyn GenericClient>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsBench;
                                     init_batch_size, length_s, put_ratio,
                                     value_size)?;
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
            driver: DriverOpenLoop::new(id, stub, timeout),
            params,
            rng: rand::thread_rng(),
            value,
        })
    }

    /// Generates a random request.
    fn gen_rand_cmd(&mut self) -> Command {
        let key = KEYS_POOL[self.rng.gen_range(0..KEYS_POOL.len())].clone();
        if self.rng.gen_range(0..=100) <= self.params.put_ratio {
            // Put
            Command::Put {
                key,
                value: self.value.clone(),
            }
        } else {
            // Get
            Command::Get { key }
        }
    }

    /// Issues a batch of requests and waits for all of their replies.
    /// Returns the number of successful replies received.
    async fn do_batch(&mut self, num_reqs: u64) -> Result<u64, SummersetError> {
        assert!(num_reqs > 0);

        for _ in 0..num_reqs {
            let cmd = self.gen_rand_cmd();
            match cmd {
                Command::Get { key } => {
                    self.driver.issue_get(&key).await?;
                }
                Command::Put { key, value } => {
                    self.driver.issue_put(&key, &value).await?;
                }
            }
        }

        let mut ok_cnt = 0;
        for _ in 0..num_reqs {
            let result = self.driver.wait_reply().await?;
            if result.is_some() {
                ok_cnt += 1;
            }
        }

        Ok(ok_cnt)
    }

    /// Runs the adaptive benchmark for given time length.
    // TODO: add latency information
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        let start = Instant::now();
        let mut now = start;
        let length = Duration::from_secs(self.params.length_s);

        let mut batch_size = self.params.init_batch_size;
        let mut ok_cnt = 0;
        let mut total_cnt = 0;

        println!(
            "{:^11} | {:^7} | {:^12} | {:>7} / {:<7}",
            "Elapsed (s)", "Batch", "Tpt (reqs/s)", "OK", "Total"
        );
        while now.duration_since(start) < length {
            let batch_start = now;

            let batch_ok_cnt = self.do_batch(batch_size).await?;
            ok_cnt += batch_ok_cnt;
            total_cnt += batch_size;

            // print at the end of every batch
            now = Instant::now();
            let elapsed = now.duration_since(start);
            println!(
                "{:>11.2} | {:>7} | {:>12.2} | {:>7} / {:<7}",
                elapsed.as_secs_f64(),
                batch_size,
                (ok_cnt as f64) / elapsed.as_secs_f64(),
                ok_cnt,
                total_cnt
            );

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

        self.driver.leave().await?;
        Ok(())
    }
}
