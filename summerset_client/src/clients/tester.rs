//! Correctness testing client using closed-loop driver.

use std::collections::{HashMap, HashSet};

use crate::drivers::{DriverReply, DriverClosedLoop};

use color_print::cprintln;

use log::{self, LevelFilter};

use lazy_static::lazy_static;

use rand::Rng;
use rand::distributions::Alphanumeric;

use serde::Deserialize;

use tokio::time::{self, Duration};

use summerset::{
    ReplicaId, GenericEndpoint, CommandResult, CtrlRequest, CtrlReply,
    SummersetError, pf_error, logged_err, parsed_config,
};

lazy_static! {
    /// List of all tests. If the flag is true, the test is marked as basic.
    static ref ALL_TESTS: Vec<(&'static str, bool)> = vec![
        ("primitive_ops", true),
        ("client_reconnect", true),
        ("non_leader_reset", true),
        ("leader_node_reset", true),
        ("two_nodes_reset", false),
        ("non_leader_pause", false),
        ("leader_node_pause", false),
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
    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// Timeout duration setting.
    timeout: Duration,

    /// Mode parameters struct.
    params: ModeParamsTester,
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
            driver: DriverClosedLoop::new(endpoint, timeout),
            timeout,
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

    /// Returns whether an `Option<String>` matches an `Option<&str>`.
    fn strings_match(s: &Option<String>, expect: &Option<&str>) -> bool {
        s.as_deref() == *expect
    }

    /// Issues a Get request and checks its reply value against given one if
    /// not `None`. Retries in-place upon getting redirection error.
    async fn checked_get(
        &mut self,
        key: &str,
        expect_value: Option<Option<&str>>,
    ) -> Result<(), SummersetError> {
        loop {
            let result = self.driver.get(key).await?;
            match result {
                DriverReply::Success { cmd_result, .. } => {
                    if let CommandResult::Get { ref value } = cmd_result {
                        if let Some(ref expect_value) = expect_value {
                            if !Self::strings_match(value, expect_value) {
                                return logged_err!(
                                    self.driver.id;
                                    "Get value mismatch: expect {:?}, got {:?}",
                                    expect_value, value
                                );
                            }
                        }
                        return Ok(());
                    } else {
                        return logged_err!(
                            self.driver.id;
                            "CommandResult type mismatch: expect Get"
                        );
                    }
                }

                DriverReply::Failure => {
                    return logged_err!(
                        self.driver.id;
                        "service replied unknown error"
                    );
                }

                DriverReply::Redirect { .. } => {
                    time::sleep(Duration::from_millis(500)).await;
                    // retry
                }

                DriverReply::Timeout => {
                    return logged_err!(
                        self.driver.id;
                        "client-side timeout {} ms",
                        self.timeout.as_millis()
                    )
                }
            }
        }
    }

    /// Issues a Put request and checks its reply old_value against given one
    /// if not `None`. Retries in-place upon getting redirection error.
    async fn checked_put(
        &mut self,
        key: &str,
        value: &str,
        expect_old_value: Option<Option<&str>>,
    ) -> Result<(), SummersetError> {
        loop {
            let result = self.driver.put(key, value).await?;
            match result {
                DriverReply::Success { cmd_result, .. } => {
                    if let CommandResult::Put { ref old_value } = cmd_result {
                        if let Some(ref expect_old_value) = expect_old_value {
                            if !Self::strings_match(old_value, expect_old_value)
                            {
                                return logged_err!(
                                    self.driver.id;
                                    "Put old_value mismatch: expect {:?}, got {:?}",
                                    expect_old_value, old_value
                                );
                            }
                        }
                        return Ok(());
                    } else {
                        return logged_err!(
                            self.driver.id;
                            "CommandResult type mismatch: expect Put"
                        );
                    }
                }

                DriverReply::Failure => {
                    return logged_err!(
                        self.driver.id;
                        "service replied unknown error"
                    );
                }

                DriverReply::Redirect { .. } => {
                    time::sleep(Duration::from_millis(500)).await;
                    // retry
                }

                DriverReply::Timeout => {
                    return logged_err!(
                        self.driver.id;
                        "client-side timeout {} ms",
                        self.timeout.as_millis()
                    )
                }
            }
        }
    }

    /// Query the list of servers in the cluster. Returns a map from replica ID
    /// -> is_leader status.
    async fn query_servers(
        &mut self,
    ) -> Result<HashMap<ReplicaId, bool>, SummersetError> {
        let ctrl_stub = self.driver.ctrl_stub();

        // send QueryInfo request to manager
        let req = CtrlRequest::QueryInfo;
        let mut sent = ctrl_stub.send_req(Some(&req))?;
        while !sent {
            sent = ctrl_stub.send_req(None)?;
        }

        // wait for reply from manager
        let reply = ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::QueryInfo { servers } => {
                Ok(servers.into_iter().map(|(id, info)| (id, info.1)).collect())
            }
            _ => logged_err!(self.driver.id; "unexpected control reply type"),
        }
    }

    /// Resets some server(s) in the cluster.
    async fn reset_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
        durable: bool,
    ) -> Result<(), SummersetError> {
        let ctrl_stub = self.driver.ctrl_stub();

        // send ResetServers request to manager
        let req = CtrlRequest::ResetServers { servers, durable };
        let mut sent = ctrl_stub.send_req(Some(&req))?;
        while !sent {
            sent = ctrl_stub.send_req(None)?;
        }

        // wait for reply from manager
        let reply = ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::ResetServers { .. } => Ok(()),
            _ => logged_err!(self.driver.id; "unexpected control reply type"),
        }
    }

    /// Pauses some server(s) in the cluster.
    async fn pause_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let ctrl_stub = self.driver.ctrl_stub();

        // send PauseServers request to manager
        let req = CtrlRequest::PauseServers { servers };
        let mut sent = ctrl_stub.send_req(Some(&req))?;
        while !sent {
            sent = ctrl_stub.send_req(None)?;
        }

        // wait for reply from manager
        let reply = ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::PauseServers { .. } => Ok(()),
            _ => logged_err!(self.driver.id; "unexpected control reply type"),
        }
    }

    /// Resume some server(s) in the cluster.
    #[allow(dead_code)]
    async fn resume_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let ctrl_stub = self.driver.ctrl_stub();

        // send ResumeServers request to manager
        let req = CtrlRequest::ResumeServers { servers };
        let mut sent = ctrl_stub.send_req(Some(&req))?;
        while !sent {
            sent = ctrl_stub.send_req(None)?;
        }

        // wait for reply from manager
        let reply = ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::ResumeServers { .. } => Ok(()),
            _ => logged_err!(self.driver.id; "unexpected control reply type"),
        }
    }

    /// Runs the individual correctness test.
    async fn do_test_by_name(
        &mut self,
        name: &str,
    ) -> Result<(), SummersetError> {
        // reset everything to initial state at the start of each test
        // self.reset_servers(HashSet::new(), false).await?;
        // time::sleep(Duration::from_secs(1)).await;
        self.driver.connect().await?;

        let result = match name {
            "primitive_ops" => self.test_primitive_ops().await,
            "client_reconnect" => self.test_client_reconnect().await,
            "non_leader_reset" => self.test_non_leader_reset().await,
            "leader_node_reset" => self.test_leader_node_reset().await,
            "two_nodes_reset" => self.test_two_nodes_reset().await,
            "non_leader_pause" => self.test_non_leader_pause().await,
            "leader_node_pause" => self.test_leader_node_pause().await,
            _ => {
                return logged_err!(self.driver.id; "unrecognized test name '{}'",
                                                   name);
            }
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
        self.checked_get("Jose", Some(None)).await?;
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None)).await?;
        self.checked_get("Jose", Some(Some(&v0))).await?;
        let v1 = Self::gen_rand_string(16);
        self.checked_put("Jose", &v1, Some(Some(&v0))).await?;
        self.checked_get("Jose", Some(Some(&v1))).await?;
        Ok(())
    }

    /// Client leaves and reconnects.
    async fn test_client_reconnect(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None)).await?;
        self.driver.leave(false).await?;
        self.driver.connect().await?;
        self.checked_get("Jose", Some(Some(&v))).await?;
        Ok(())
    }

    /// Single non-leader replica node crashes and restarts.
    async fn test_non_leader_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None)).await?;
        for (s, is_leader) in self.query_servers().await? {
            if !is_leader {
                self.driver.leave(false).await?;
                self.reset_servers(HashSet::from([s]), true).await?;
                time::sleep(Duration::from_millis(500)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v))).await?;
                break;
            }
        }
        Ok(())
    }

    /// Single leader replica node crashes and restarts.
    async fn test_leader_node_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None)).await?;
        for (s, is_leader) in self.query_servers().await? {
            if is_leader {
                self.driver.leave(false).await?;
                self.reset_servers(HashSet::from([s]), true).await?;
                time::sleep(Duration::from_millis(500)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v))).await?;
                break;
            }
        }
        Ok(())
    }

    /// Two replica nodes (leader + non-leader) crash and restart.
    async fn test_two_nodes_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None)).await?;
        let mut resets = HashSet::new();
        let (mut l, mut nl) = (false, false);
        for (s, is_leader) in self.query_servers().await? {
            if !l && is_leader {
                resets.insert(s);
                l = true;
            }
            if !nl && !is_leader {
                resets.insert(s);
                nl = true;
            }
            if l && nl {
                break;
            }
        }
        if resets.len() == 2 {
            self.driver.leave(false).await?;
            self.reset_servers(resets, true).await?;
            time::sleep(Duration::from_millis(500)).await;
            self.driver.connect().await?;
            self.checked_get("Jose", Some(Some(&v))).await?;
        }
        Ok(())
    }

    /// Single non-leader replica node paused.
    async fn test_non_leader_pause(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None)).await?;
        time::sleep(Duration::from_millis(300)).await;
        for (s, is_leader) in self.query_servers().await? {
            if !is_leader {
                self.driver.leave(false).await?;
                self.pause_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(1)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v0))).await?;
                let v1 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v1, Some(Some(&v0))).await?;
                break;
            }
        }
        Ok(())
    }

    /// Single leader replica node paused.
    async fn test_leader_node_pause(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None)).await?;
        time::sleep(Duration::from_millis(300)).await;
        for (s, is_leader) in self.query_servers().await? {
            if is_leader {
                self.driver.leave(false).await?;
                self.pause_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(1)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v0))).await?;
                let v1 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v1, Some(Some(&v0))).await?;
                break;
            }
        }
        Ok(())
    }
}
