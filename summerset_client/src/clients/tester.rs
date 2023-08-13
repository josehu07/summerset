//! Correctness testing client using open-loop driver.

use crate::drivers::DriverOpenLoop;

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;

use serde::Deserialize;

use tokio::time::Duration;

use summerset::{
    GenericEndpoint, ClientId, RequestId, SummersetError, pf_error, logged_err,
    parsed_config,
};

lazy_static! {
    /// List of all tests. If the flag is true, the test is marked as basic.
    static ref ALL_TESTS: Vec<(&'static str, bool)> = vec![
        ("primitives", true),
        ("reconnect", true),
    ];
}

/// Mod parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsTester {
    /// Name of individual test to run, or 'basic' to run the basic set of
    /// tests, or 'all' to run all tests.
    pub test_name: String,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsTester {
    fn default() -> Self {
        ModeParamsTester {
            test_name: "basic".into(),
        }
    }
}

/// Correctness testing client struct.
pub struct ClientTester {
    /// Client ID.
    id: ClientId,

    /// Open-loop request driver.
    driver: DriverOpenLoop,

    /// Mode parameters struct.
    params: ModeParamsTester,
}

impl ClientTester {
    /// Creates a new testing client.
    pub fn new(
        id: ClientId,
        stub: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsTester;
                                     test_name)?;

        Ok(ClientTester {
            id,
            driver: DriverOpenLoop::new(id, stub, timeout),
            params,
        })
    }

    /// Generates a random string.
    fn gen_rand_string(length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    /// Issues a Get request, retrying immediately on `WouldBlock` failures.
    async fn issue_get(
        &mut self,
        key: &str,
    ) -> Result<RequestId, SummersetError> {
        let mut req_id = self.driver.issue_get(key).await?;
        while req_id.is_none() {
            req_id = self.driver.issue_retry().await?;
        }
        Ok(req_id.unwrap())
    }

    /// Issues a Put request, retrying immediately on `WouldBlock` failures.
    async fn issue_put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<RequestId, SummersetError> {
        let mut req_id = self.driver.issue_put(key, value).await?;
        while req_id.is_none() {
            req_id = self.driver.issue_retry().await?;
        }
        Ok(req_id.unwrap())
    }

    /// Runs the individual correctness test.
    async fn do_test_by_name(
        &mut self,
        name: &str,
    ) -> Result<(), SummersetError> {
        match name {
            "primitives" => self.test_primitives(),
            "reconnect" => self.test_reconnect(),
            _ => logged_err!(self.id; "unrecognized test name '{}'", name),
        }
    }

    /// Runs the specified correctness test.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        let test_name = self.params.test_name.clone();
        match &test_name[..] {
            "basic" => {
                for (name, basic) in ALL_TESTS.iter() {
                    if *basic {
                        self.do_test_by_name(*name).await?;
                    }
                }
            }
            "all" => {
                for (name, _) in ALL_TESTS.iter() {
                    self.do_test_by_name(*name).await?;
                }
            }
            _ => self.do_test_by_name(&test_name).await?,
        }
        Ok(())
    }
}
