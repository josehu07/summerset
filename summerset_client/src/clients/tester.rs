//! Correctness testing client using open-loop driver.
//!
//! TODO: add replica management commands: reset, shutdown, restart, etc.

use std::collections::HashMap;

use crate::drivers::DriverOpenLoop;

use log::{self, LevelFilter};

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;

use serde::Deserialize;

use tokio::time::Duration;

use summerset::{
    GenericEndpoint, ClientId, CommandResult, RequestId, SummersetError,
    pf_error, logged_err, parsed_config,
};

lazy_static! {
    /// List of all tests. If the flag is true, the test is marked as basic.
    static ref ALL_TESTS: Vec<(&'static str, bool)> = vec![
        ("primitives", true),
        ("reconnect", true),
        ("crash_restart", false),
    ];
}

/// Mod parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsTester {
    /// Name of individual test to run, or 'basic' to run the basic set of
    /// tests, or 'all' to run all tests.
    pub test_name: String,

    /// Whether to continue next test upon failed test.
    pub keep_going: bool,

    /// Do not suppress logger output.
    pub logger_on: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsTester {
    fn default() -> Self {
        ModeParamsTester {
            test_name: "basic".into(),
            keep_going: false,
            logger_on: false,
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

    /// Replies received but not yet used.
    cached_replies: HashMap<RequestId, CommandResult>,
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
                                     test_name, keep_going, logger_on)?;

        // suppress all logger levels if not logger_on
        if !params.logger_on {
            log::set_max_level(LevelFilter::Error);
        }

        Ok(ClientTester {
            id,
            driver: DriverOpenLoop::new(id, stub, timeout),
            params,
            cached_replies: HashMap::new(),
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

    /// Returns whether an `Option<String>` matches an `Option<&str>`.
    fn strings_match(s: &Option<String>, expect: &Option<&str>) -> bool {
        s.as_deref() == *expect
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

    /// Waits for the next reply from service with the given request ID. If
    /// non-match replies received, cache them up for future references.
    async fn wait_reply(
        &mut self,
        req_id: RequestId,
        // maximum number of tries if repeatedly getting `Ok(None)` reply
        max_tries: u8,
    ) -> Result<CommandResult, SummersetError> {
        assert!(max_tries > 0);
        let mut num_tries = 0;

        // look up cached_replies first
        if let Some(cmd_result) = self.cached_replies.remove(&req_id) {
            return Ok(cmd_result);
        }

        let mut result = self.driver.wait_reply().await?;
        while result.is_none() || result.as_ref().unwrap().0 != req_id {
            if let Some((id, cmd_result)) = result {
                self.cached_replies.insert(id, cmd_result);
            } else {
                num_tries += 1;
                if num_tries == max_tries {
                    return Err(SummersetError(format!(
                        "exhausted {} tries expecting req {}",
                        max_tries, req_id,
                    )));
                }
            }
            result = self.driver.wait_reply().await?;
        }

        Ok(result.unwrap().1)
    }

    /// Waits for the reply of given request ID, expecting the given Get value
    /// if not `None`.
    async fn expect_get_reply(
        &mut self,
        req_id: RequestId,
        expect_value: Option<Option<&str>>,
        // maximum number of tries if repeatedly getting `WouldBlock` failure
        max_tries: u8,
    ) -> Result<(), SummersetError> {
        let cmd_result = self.wait_reply(req_id, max_tries).await?;
        if let CommandResult::Get { ref value } = cmd_result {
            if let Some(ref expect_value) = expect_value {
                if !Self::strings_match(value, expect_value) {
                    return Err(SummersetError(format!(
                        "Get value mismatch: expect {:?}, got {:?}",
                        expect_value, value
                    )));
                }
            }
            Ok(())
        } else {
            Err(SummersetError(
                "CommandResult type mismatch: expect Get".into(),
            ))
        }
    }

    /// Waits for the reply of given request ID, expecting the given Put
    /// old_value if not `None`.
    async fn expect_put_reply(
        &mut self,
        req_id: RequestId,
        expect_old_value: Option<Option<&str>>,
        // maximum number of tries if repeatedly getting `WouldBlock` failure
        max_tries: u8,
    ) -> Result<(), SummersetError> {
        let cmd_result = self.wait_reply(req_id, max_tries).await?;
        if let CommandResult::Put { ref old_value } = cmd_result {
            if let Some(ref expect_old_value) = expect_old_value {
                if !Self::strings_match(old_value, expect_old_value) {
                    return Err(SummersetError(format!(
                        "Put old_value mismatch: expect {:?}, got {:?}",
                        expect_old_value, old_value
                    )));
                }
            }
            Ok(())
        } else {
            Err(SummersetError(
                "CommandResult type mismatch: expect Put".into(),
            ))
        }
    }

    /// Runs the individual correctness test.
    async fn do_test_by_name(
        &mut self,
        name: &str,
    ) -> Result<(), SummersetError> {
        // TODO: reset service state
        self.cached_replies.clear();

        let result = match name {
            "primitives" => self.test_primitives().await,
            "reconnect" => self.test_reconnect().await,
            "crash_restart" => self.test_crash_restart().await,
            _ => {
                return logged_err!(self.id; "unrecognized test name '{}'", name)
            }
        };

        self.driver.leave().await?;

        if let Err(ref e) = result {
            println!("{:>16} | {:^6} | {}", name, "FAIL", e);
        } else {
            println!("{:>16} | {:^6} | --", name, "PASS");
        }
        result
    }

    /// Runs the specified correctness test.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        let test_name = self.params.test_name.clone();
        let mut all_pass = true;

        println!("{:^16} | {:^6} | Notes", "Test Case", "Result");
        match &test_name[..] {
            "basic" => {
                for (name, basic) in ALL_TESTS.iter() {
                    if *basic {
                        let result = self.do_test_by_name(name).await;
                        if result.is_err() {
                            all_pass = false;
                            if !self.params.keep_going {
                                return result;
                            }
                        }
                    }
                }
            }
            "all" => {
                for (name, _) in ALL_TESTS.iter() {
                    let result = self.do_test_by_name(name).await;
                    if result.is_err() {
                        all_pass = false;
                        if !self.params.keep_going {
                            return result;
                        }
                    }
                }
            }
            _ => return self.do_test_by_name(&test_name).await,
        }

        if all_pass {
            Ok(())
        } else {
            Err(SummersetError("some test(s) failed".into()))
        }
    }
}

// List of tests:
impl ClientTester {
    /// Basic primitive operations.
    async fn test_primitives(&mut self) -> Result<(), SummersetError> {
        let mut req_id = self.issue_get("Jose").await?;
        self.expect_get_reply(req_id, Some(None), 1).await?;
        let v0 = Self::gen_rand_string(8);
        req_id = self.issue_put("Jose", &v0).await?;
        self.expect_put_reply(req_id, Some(None), 1).await?;
        req_id = self.issue_get("Jose").await?;
        self.expect_get_reply(req_id, Some(Some(&v0)), 1).await?;
        let v1 = Self::gen_rand_string(16);
        req_id = self.issue_put("Jose", &v1).await?;
        self.expect_put_reply(req_id, Some(Some(&v0)), 1).await?;
        req_id = self.issue_get("Jose").await?;
        self.expect_get_reply(req_id, Some(Some(&v1)), 1).await?;
        Ok(())
    }

    /// Client leaves and reconnects.
    async fn test_reconnect(&mut self) -> Result<(), SummersetError> {
        Ok(())
    }

    /// Replica node crashes and restarts.
    async fn test_crash_restart(&mut self) -> Result<(), SummersetError> {
        Ok(())
    }
}
