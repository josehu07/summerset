//! Correctness testing client using open-loop driver.

use std::collections::{HashMap, HashSet};

use crate::drivers::DriverOpenLoop;

use color_print::cprintln;

use log::{self, LevelFilter};

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;

use serde::Deserialize;

use tokio::time::Duration;

use summerset::{
    ReplicaId, GenericEndpoint, CommandResult, RequestId, CtrlRequest,
    CtrlReply, SummersetError, pf_error, logged_err, parsed_config,
};

lazy_static! {
    /// List of all tests. If the flag is true, the test is marked as basic.
    static ref ALL_TESTS: Vec<(&'static str, bool)> = vec![
        ("primitive_ops", true),
        ("client_reconnect", true),
        ("node_1_crash", true),
        ("node_0_crash", true),
        ("two_nodes_crash", false)
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
        endpoint: Box<dyn GenericEndpoint>,
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
            driver: DriverOpenLoop::new(endpoint, timeout),
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
    fn issue_get(&mut self, key: &str) -> Result<RequestId, SummersetError> {
        let mut req_id = self.driver.issue_get(key)?;
        while req_id.is_none() {
            req_id = self.driver.issue_retry()?;
        }
        Ok(req_id.unwrap())
    }

    /// Issues a Put request, retrying immediately on `WouldBlock` failures.
    fn issue_put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<RequestId, SummersetError> {
        let mut req_id = self.driver.issue_put(key, value)?;
        while req_id.is_none() {
            req_id = self.driver.issue_retry()?;
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
            if let Some((id, cmd_result, _)) = result {
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

    /// Resets some server(s) in the cluster.
    async fn reset_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
        durable: bool,
    ) -> Result<(), SummersetError> {
        let ctrl_stub = self.driver.ctrl_stub();

        // send ResetServer request to manager
        let req = CtrlRequest::ResetServers { servers, durable };
        let mut sent = ctrl_stub.send_req(Some(&req))?;
        while !sent {
            sent = ctrl_stub.send_req(None)?;
        }

        // wait for reply from manager
        let reply = ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::ResetServers { .. } => Ok(()),
            _ => logged_err!("c"; "unexpected control reply type"),
        }
    }

    /// Runs the individual correctness test.
    async fn do_test_by_name(
        &mut self,
        name: &str,
    ) -> Result<(), SummersetError> {
        // reset everything to initial state at the start of each test
        self.reset_servers(HashSet::new(), false).await?;
        self.driver.connect().await?;
        self.cached_replies.clear();

        let result = match name {
            "primitive_ops" => self.test_primitive_ops().await,
            "client_reconnect" => self.test_client_reconnect().await,
            "node_1_crash" => self.test_node_1_crash().await,
            "node_0_crash" => self.test_node_0_crash().await,
            "two_nodes_crash" => self.test_two_nodes_crash().await,
            _ => return logged_err!("c"; "unrecognized test name '{}'", name),
        };

        if let Err(ref e) = result {
            cprintln!("{:>20} | <red>{:^6}</> | {}", name, "FAIL", e);
        } else {
            cprintln!("{:>20} | <green>{:^6}</> | --", name, "PASS");
        }

        // send leave notification and forget about the TCP connections at the
        // end of each test
        self.driver.leave(false).await?;
        result
    }

    /// Runs the specified correctness test.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        let test_name = self.params.test_name.clone();
        let mut all_pass = true;

        println!("{:^20} | {:^6} | Notes", "Test Case", "Result");
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

        self.driver.leave(true).await?;
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
    async fn test_primitive_ops(&mut self) -> Result<(), SummersetError> {
        let mut req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(None), 1).await?;
        let v0 = Self::gen_rand_string(8);
        req_id = self.issue_put("Jose", &v0)?;
        self.expect_put_reply(req_id, Some(None), 1).await?;
        req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(Some(&v0)), 1).await?;
        let v1 = Self::gen_rand_string(16);
        req_id = self.issue_put("Jose", &v1)?;
        self.expect_put_reply(req_id, Some(Some(&v0)), 1).await?;
        req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(Some(&v1)), 1).await?;
        Ok(())
    }

    /// Client leaves and reconnects.
    async fn test_client_reconnect(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        let mut req_id = self.issue_put("Jose", &v)?;
        self.expect_put_reply(req_id, Some(None), 1).await?;
        self.driver.leave(false).await?;
        self.driver.connect().await?;
        req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(Some(&v)), 1).await?;
        Ok(())
    }

    /// Replica node 1 crashes and restarts.
    async fn test_node_1_crash(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        let mut req_id = self.issue_put("Jose", &v)?;
        self.expect_put_reply(req_id, Some(None), 1).await?;
        self.driver.leave(false).await?;
        self.reset_servers(HashSet::from([1]), true).await?;
        self.driver.connect().await?;
        req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(Some(&v)), 1).await?;
        Ok(())
    }

    /// Replica node 0 crashes and restarts.
    async fn test_node_0_crash(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        let mut req_id = self.issue_put("Jose", &v)?;
        self.expect_put_reply(req_id, Some(None), 1).await?;
        self.driver.leave(false).await?;
        self.reset_servers(HashSet::from([0]), true).await?;
        self.driver.connect().await?;
        req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(Some(&v)), 1).await?;
        Ok(())
    }

    /// Two replica nodes crashes and restarts.
    async fn test_two_nodes_crash(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        let mut req_id = self.issue_put("Jose", &v)?;
        self.expect_put_reply(req_id, Some(None), 1).await?;
        self.driver.leave(false).await?;
        self.reset_servers(HashSet::from([0, 1]), true).await?;
        self.driver.connect().await?;
        req_id = self.issue_get("Jose")?;
        self.expect_get_reply(req_id, Some(Some(&v)), 1).await?;
        Ok(())
    }
}
