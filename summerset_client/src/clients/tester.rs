//! Correctness testing client using closed-loop driver.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use color_print::cprintln;
use log::{self, LevelFilter};
use rand::RngExt;
use rand::distr::Alphanumeric;
use serde::Deserialize;
use summerset::{
    CommandResult, ConfChange, CtrlReply, CtrlRequest, Faults, GenericEndpoint, ReplicaId,
    SmrProtocol, SummersetError, logged_err, parsed_config, pf_debug, pf_error,
};
use tokio::time::{self, Duration};

use crate::drivers::{DriverClosedLoop, DriverReply};

/// List of all tests. If the flag is true, the test is marked as basic.
static ALL_TESTS: LazyLock<Vec<(&'static str, bool)>> = LazyLock::new(|| {
    vec![
        ("primitive_ops", true),
        ("client_reconnect", true),
        ("non_leader_reset", true),
        ("leader_node_reset", false),
        ("two_nodes_reset", false),
        ("all_nodes_reset", false),
        ("non_leader_pause", false),
        ("leader_node_pause", false),
        ("node_pause_resume", false),
        ("snapshot_reset", false),
        ("snapshot_progress", false),
        ("lossy_network", false),
        ("minority_partition", false),
        ("leader_partition", false),
        ("non_leader_crash", false),
        ("leader_node_crash", false),
        ("clock_drift", false),
        ("duplicate_failover", false),
        ("b2b_leader_crashes", false),
    ]
});

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

    /// Seed of injected faults, to replay a run; random per test if 0.
    pub fault_seed: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsTester {
    fn default() -> Self {
        ModeParamsTester {
            test_name: "basic".into(),
            keep_going: false,
            logger_on: false,
            fault_seed: 0,
        }
    }
}

/// Correctness testing client struct.
pub(crate) struct ClientTester {
    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// Timeout duration setting.
    timeout: Duration,

    /// Mode parameters struct.
    params: ModeParamsTester,

    /// Protocol under test.
    protocol: SmrProtocol,

    /// Seed of the faults injected by the current test, if any.
    fault_seed: Option<u64>,
}

impl ClientTester {
    /// Creates a new testing client.
    pub(crate) fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
        protocol: SmrProtocol,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsTester;
                                    test_name, keep_going, logger_on, fault_seed)?;

        // suppress all logger levels if not logger_on
        if !params.logger_on {
            log::set_max_level(LevelFilter::Error);
        }

        Ok(ClientTester {
            driver: DriverClosedLoop::new(endpoint, timeout),
            timeout,
            params,
            protocol,
            fault_seed: None,
        })
    }

    /// Generates a random string.
    fn gen_rand_string(length: usize) -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    /// Returns whether an `Option<String>` matches an `Option<&str>`.
    #[allow(clippy::ref_option, clippy::ref_option_ref)]
    fn strings_match(s: &Option<String>, expect: &Option<&str>) -> bool {
        s.as_deref() == *expect
    }

    /// Issues a Get request and checks its reply value against given one if
    /// not `None`. Retries in-place upon getting redirection error. Retries
    /// at most `max_timeouts` times upon getting timeouts.
    #[allow(clippy::option_option)]
    async fn checked_get(
        &mut self,
        key: &str,
        expect_value: Option<Option<&str>>,
        max_timeouts: u8,
    ) -> Result<(), SummersetError> {
        let mut timeouts = 0;
        while timeouts <= max_timeouts {
            let result = self.driver.get(key).await?;
            match result {
                DriverReply::Success { cmd_result, .. } => {
                    if let CommandResult::Get { ref value } = cmd_result {
                        if let Some(ref expect_value) = expect_value
                            && !Self::strings_match(value, expect_value)
                        {
                            return logged_err!(
                                "Get value mismatch: expect {:?}, got {:?}",
                                expect_value,
                                value
                            );
                        }
                        return Ok(());
                    }
                    return logged_err!("CommandResult type mismatch: expect Get");
                }

                DriverReply::Conf { .. } => {
                    return logged_err!("DriverReply type mismatch: expect normal command");
                }

                DriverReply::Failure => {
                    return logged_err!("service replied unknown error");
                }

                DriverReply::Redirect { .. } => {
                    time::sleep(Duration::from_millis(500)).await;
                    // retry
                }

                DriverReply::Timeout => {
                    timeouts += 1;
                    pf_debug!("client-side timeout {} ms", self.timeout.as_millis());
                }
            }
        }

        logged_err!(
            "client-side timeout {} ms {} times",
            self.timeout.as_millis(),
            max_timeouts + 1
        )
    }

    /// Issues a Put request and checks its reply `old_value` against given one
    /// if not `None`. Retries in-place upon getting redirection error. Retries
    /// at most `max_timeouts` times upon getting timeouts.
    #[allow(clippy::option_option)]
    async fn checked_put(
        &mut self,
        key: &str,
        value: &str,
        expect_old_value: Option<Option<&str>>,
        max_timeouts: u8,
    ) -> Result<(), SummersetError> {
        let mut timeouts = 0;
        while timeouts <= max_timeouts {
            let result = self.driver.put(key, value).await?;
            match result {
                DriverReply::Success { cmd_result, .. } => {
                    if let CommandResult::Put { ref old_value } = cmd_result {
                        if let Some(ref expect_old_value) = expect_old_value
                            && !Self::strings_match(old_value, expect_old_value)
                        {
                            return logged_err!(
                                "Put old_value mismatch: expect {:?}, got {:?}",
                                expect_old_value,
                                old_value
                            );
                        }
                        return Ok(());
                    }
                    return logged_err!("CommandResult type mismatch: expect Put");
                }

                DriverReply::Conf { .. } => {
                    return logged_err!("DriverReply type mismatch: expect normal command");
                }

                DriverReply::Failure => {
                    return logged_err!("service replied unknown error");
                }

                DriverReply::Redirect { .. } => {
                    time::sleep(Duration::from_millis(500)).await;
                    // retry
                }

                DriverReply::Timeout => {
                    timeouts += 1;
                    pf_debug!("client-side timeout {} ms", self.timeout.as_millis());
                }
            }
        }

        logged_err!(
            "client-side timeout {} ms {} times",
            self.timeout.as_millis(),
            max_timeouts + 1
        )
    }

    /// Sends a control request to the manager and waits for its reply, failing
    /// if there is none within a generous bound (e.g., as a server died).
    async fn ctrl(&mut self, req: CtrlRequest) -> Result<CtrlReply, SummersetError> {
        self.driver.ctrl_stub().send_req_insist(&req).await?;
        time::timeout(
            Duration::from_secs(30),
            self.driver.ctrl_stub().recv_reply(),
        )
        .await
        .map_err(|_| SummersetError::msg(format!("no reply to {req:?}")))?
    }

    /// Query the list of servers in the cluster. Returns a map from replica ID
    /// -> `is_leader` status.
    async fn query_servers(&mut self) -> Result<HashMap<ReplicaId, bool>, SummersetError> {
        let req = CtrlRequest::QueryInfo;
        let reply = self.ctrl(req).await?;
        if let CtrlReply::QueryInfo { servers_info, .. } = reply {
            Ok(servers_info
                .into_iter()
                .map(|(id, info)| (id, info.is_leader))
                .collect())
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Resets some server(s) in the cluster.
    async fn reset_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
        durable: bool,
    ) -> Result<(), SummersetError> {
        let req = CtrlRequest::ResetServers { servers, durable };
        let reply = self.ctrl(req).await?;
        if let CtrlReply::ResetServers { .. } = reply {
            Ok(())
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Pauses some server(s) in the cluster.
    async fn pause_servers(&mut self, servers: HashSet<ReplicaId>) -> Result<(), SummersetError> {
        let req = CtrlRequest::PauseServers { servers };
        let reply = self.ctrl(req).await?;
        if let CtrlReply::PauseServers { .. } = reply {
            Ok(())
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Force some server(s) to take a new snapshot.
    async fn force_snapshot(
        &mut self,
        servers: HashSet<ReplicaId>,
    ) -> Result<HashMap<ReplicaId, usize>, SummersetError> {
        let req = CtrlRequest::TakeSnapshot { servers };
        let reply = self.ctrl(req).await?;
        if let CtrlReply::TakeSnapshot { snapshot_up_to } = reply {
            Ok(snapshot_up_to)
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Resume some server(s) in the cluster.
    #[allow(dead_code)]
    async fn resume_servers(&mut self, servers: HashSet<ReplicaId>) -> Result<(), SummersetError> {
        let req = CtrlRequest::ResumeServers { servers };
        let reply = self.ctrl(req).await?;
        if let CtrlReply::ResumeServers { .. } = reply {
            Ok(())
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Injects faults into some server(s) in the cluster, replacing any
    /// injected before.
    async fn inject_faults(
        &mut self,
        servers: HashSet<ReplicaId>,
        faults: Faults,
    ) -> Result<(), SummersetError> {
        let req = CtrlRequest::InjectFaults { servers, faults };
        let reply = self.ctrl(req).await?;
        if let CtrlReply::InjectFaults { .. } = reply {
            Ok(())
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Crashes some server(s) in the cluster as a process crash, which skips
    /// the graceful leave but keeps all storage writes.
    async fn crash_servers(&mut self, servers: HashSet<ReplicaId>) -> Result<(), SummersetError> {
        let req = CtrlRequest::CrashServers {
            servers,
            power_loss: false,
        };
        let reply = self.ctrl(req).await?;
        if let CtrlReply::CrashServers { .. } = reply {
            Ok(())
        } else {
            logged_err!("unexpected control reply type")
        }
    }

    /// Returns empty faults seeded for the current test.
    fn seeded_faults(&mut self) -> Faults {
        let param = self.params.fault_seed;
        let seed = *self.fault_seed.get_or_insert_with(|| {
            if param == 0 {
                // within the range of TOML integers, so it can be passed back
                rand::rng().random_range(1..=(u64::MAX >> 1))
            } else {
                param
            }
        });
        Faults {
            seed,
            ..Faults::default()
        }
    }

    /// Cuts off all links between a server and the rest of the cluster.
    async fn cut_off_links(&mut self, server: ReplicaId) -> Result<(), SummersetError> {
        let others: HashSet<ReplicaId> = self
            .query_servers()
            .await?
            .into_keys()
            .filter(|&s| s != server)
            .collect();
        let faults = self.seeded_faults();
        self.inject_faults(
            HashSet::from([server]),
            Faults {
                cut_peers: others.clone(),
                ..faults.clone()
            },
        )
        .await?;
        self.inject_faults(
            others,
            Faults {
                cut_peers: HashSet::from([server]),
                ..faults
            },
        )
        .await
    }

    /// Picks the leader replica (server 0 if there is none), or else the
    /// non-leader with the highest ID, which clients connect to the least.
    async fn pick_server(&mut self, leader: bool) -> Result<ReplicaId, SummersetError> {
        let servers = self.query_servers().await?;
        let pick = if leader {
            servers
                .iter()
                .find(|&(_, &l)| l)
                .map_or(Some(0), |(&s, _)| Some(s))
        } else {
            servers.iter().filter(|&(_, &l)| !l).map(|(&s, _)| s).max()
        };
        pick.ok_or_else(|| SummersetError::msg("no server to pick"))
    }

    /// Runs the individual correctness test.
    async fn do_test_by_name(&mut self, name: &str) -> Result<(), SummersetError> {
        const NODE_PAUSE_TESTS: [&str; 3] =
            ["non_leader_pause", "leader_node_pause", "node_pause_resume"];
        const NODE_FAULT_TESTS: [&str; 8] = [
            "lossy_network",
            "minority_partition",
            "leader_partition",
            "non_leader_crash",
            "leader_node_crash",
            "clock_drift",
            "duplicate_failover",
            "b2b_leader_crashes",
        ];
        if self.protocol.skips_faulty_tests() && NODE_PAUSE_TESTS.contains(&name) {
            cprintln!(
                "{:>20} | <yellow>{:^6}</> | protocol can't handle pause tests",
                name,
                "SKIP"
            );
            return Ok(());
        }
        // protocols that keep no log to compact, and EPaxos, whose snapshot
        // point is held back by idle rows
        if (self.protocol.skips_faulty_tests() || self.protocol == SmrProtocol::EPaxos)
            && name == "snapshot_progress"
        {
            cprintln!(
                "{:>20} | <yellow>{:^6}</> | protocol can't advance snapshots",
                name,
                "SKIP"
            );
            return Ok(());
        }
        if self.protocol.skips_faulty_tests() && NODE_FAULT_TESTS.contains(&name) {
            cprintln!(
                "{:>20} | <yellow>{:^6}</> | protocol can't handle faults",
                name,
                "SKIP"
            );
            return Ok(());
        }

        // reset everything to initial state at the start of each test, with
        // no faults injected
        self.fault_seed = None;
        self.inject_faults(HashSet::new(), Faults::default())
            .await?;
        self.reset_servers(HashSet::new(), false).await?;
        time::sleep(Duration::from_secs(1)).await;

        // bound each test, as a stuck service could stall it forever (e.g.,
        // with requests redirected endlessly)
        let test = async {
            self.driver.connect().await?;
            match name {
                "primitive_ops" => self.test_primitive_ops().await,
                "client_reconnect" => self.test_client_reconnect().await,
                "non_leader_reset" => self.test_non_leader_reset().await,
                "leader_node_reset" => self.test_leader_node_reset().await,
                "two_nodes_reset" => self.test_two_nodes_reset().await,
                "all_nodes_reset" => self.test_all_nodes_reset().await,
                "non_leader_pause" => self.test_non_leader_pause().await,
                "leader_node_pause" => self.test_leader_node_pause().await,
                "node_pause_resume" => self.test_node_pause_resume().await,
                "snapshot_reset" => self.test_snapshot_reset().await,
                "snapshot_progress" => self.test_snapshot_progress().await,
                "lossy_network" => self.test_lossy_network().await,
                "minority_partition" => self.test_minority_partition().await,
                "leader_partition" => self.test_leader_partition().await,
                "non_leader_crash" => self.test_non_leader_crash().await,
                "leader_node_crash" => self.test_leader_node_crash().await,
                "clock_drift" => self.test_clock_drift().await,
                "duplicate_failover" => self.test_duplicate_failover().await,
                "b2b_leader_crashes" => self.test_b2b_leader_crashes().await,
                _ => logged_err!("unrecognized test name '{}'", name),
            }
        };
        let result = time::timeout(Duration::from_secs(60), test)
            .await
            .unwrap_or_else(|_| logged_err!("test timed out"));

        // the seed of injected faults, if any, allows replaying the test
        let seed = self.fault_seed.map(|seed| format!("seed {seed}"));
        if let Err(ref e) = result {
            let seed = seed.map(|seed| format!(" ({seed})")).unwrap_or_default();
            cprintln!("{:>20} | <red>{:^6}</> | {}{}", name, "FAIL", e, seed);
        } else {
            let note = seed.unwrap_or_else(|| "--".into());
            cprintln!("{:>20} | <green>{:^6}</> | {}", name, "PASS", note);
        }

        // never leave faults injected beyond a test
        self.inject_faults(HashSet::new(), Faults::default())
            .await?;

        // send leave notification and forget about the TCP connections at the
        // end of each test
        self.driver.leave(false).await?;
        result
    }

    /// Runs the specified correctness test.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        let test_name = self.params.test_name.clone();
        let mut all_pass = true;

        println!("{:^20} | {:^6} | Notes", "Test Case", "Result");
        let names: Vec<&str> = match &test_name[..] {
            "basic" => ALL_TESTS
                .iter()
                .filter(|(_, basic)| *basic)
                .map(|(name, _)| *name)
                .collect(),
            "all" => ALL_TESTS.iter().map(|(name, _)| *name).collect(),
            _ => return self.do_test_by_name(&test_name).await,
        };
        for name in names {
            let result = self.do_test_by_name(name).await;
            if result.is_err() {
                all_pass = false;
                if !self.params.keep_going {
                    return result;
                }
            }
        }

        self.driver.leave(true).await?;
        if all_pass {
            Ok(())
        } else {
            Err(SummersetError::msg("some test(s) failed"))
        }
    }
}

// List of tests:
impl ClientTester {
    /// Basic primitive operations.
    async fn test_primitive_ops(&mut self) -> Result<(), SummersetError> {
        self.checked_get("Jose", Some(None), 0).await?;
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        self.checked_get("Jose", Some(Some(&v0)), 0).await?;
        let v1 = Self::gen_rand_string(16);
        self.checked_put("Jose", &v1, Some(Some(&v0)), 0).await?;
        self.checked_get("Jose", Some(Some(&v1)), 0).await?;
        Ok(())
    }

    /// Client leaves and reconnects.
    async fn test_client_reconnect(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None), 0).await?;
        self.driver.leave(false).await?;
        self.driver.connect().await?;
        self.checked_get("Jose", Some(Some(&v)), 0).await?;
        Ok(())
    }

    /// Single non-leader replica node crashes and restarts.
    async fn test_non_leader_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None), 0).await?;
        for (s, is_leader) in self.query_servers().await? {
            if !is_leader {
                // picked a non-leader replica
                self.driver.leave(false).await?;
                self.reset_servers(HashSet::from([s]), true).await?;
                time::sleep(Duration::from_secs(1)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v)), 0).await?;
                break;
            }
        }
        Ok(())
    }

    /// Single leader replica node crashes and restarts.
    async fn test_leader_node_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None), 0).await?;
        for (s, is_leader) in self.query_servers().await? {
            if is_leader {
                // picked a leader replica
                self.driver.leave(false).await?;
                self.reset_servers(HashSet::from([s]), true).await?;
                time::sleep(Duration::from_secs(3)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v)), 0).await?;
                break;
            }
        }
        Ok(())
    }

    /// Two replica nodes (leader + non-leader) crash and restart.
    async fn test_two_nodes_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None), 0).await?;
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
            // picked two replicas, one leader and one non-leader
            self.driver.leave(false).await?;
            self.reset_servers(resets, true).await?;
            time::sleep(Duration::from_secs(3)).await;
            self.driver.connect().await?;
            self.checked_get("Jose", Some(Some(&v)), 0).await?;
        }
        Ok(())
    }

    /// All replica nodes crash and restart at the same time.
    async fn test_all_nodes_reset(&mut self) -> Result<(), SummersetError> {
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(None), 0).await?;
        self.driver.leave(false).await?;
        self.reset_servers(HashSet::new(), true).await?;
        time::sleep(Duration::from_secs(3)).await;
        self.driver.connect().await?;
        self.checked_get("Jose", Some(Some(&v)), 0).await?;
        Ok(())
    }

    /// Single non-leader replica node paused.
    async fn test_non_leader_pause(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        time::sleep(Duration::from_millis(500)).await;
        for (s, is_leader) in self.query_servers().await? {
            if !is_leader {
                // picked a non-leader replica
                self.driver.leave(false).await?;
                self.pause_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(3)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v0)), 0).await?;
                let v1 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v1, Some(Some(&v0)), 0).await?;
                break;
            }
        }
        Ok(())
    }

    /// Single leader replica node paused.
    async fn test_leader_node_pause(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        time::sleep(Duration::from_millis(500)).await;
        for (s, is_leader) in self.query_servers().await? {
            if is_leader {
                // picked a leader replica
                self.driver.leave(false).await?;
                self.pause_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(3)).await;
                self.driver.connect().await?;
                self.checked_get("Jose", Some(Some(&v0)), 0).await?;
                let v1 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v1, Some(Some(&v0)), 0).await?;
                break;
            }
        }
        Ok(())
    }

    /// Leader replica node paused and then resumed, twice.
    async fn test_node_pause_resume(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        time::sleep(Duration::from_millis(500)).await;
        for (s, is_leader) in self.query_servers().await? {
            if is_leader {
                // picked a leader replica
                self.driver.leave(false).await?;
                self.pause_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(3)).await;
                self.driver.connect().await?;
                let v1 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v1, Some(Some(&v0)), 0).await?;
                // resuming old leader replica
                self.driver.leave(false).await?;
                self.resume_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(1)).await;
                self.driver.connect().await?;
                let v2 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v2, Some(Some(&v1)), 1).await?;
                // pausing that replica again
                self.driver.leave(false).await?;
                self.pause_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(1)).await;
                self.driver.connect().await?;
                let v3 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v3, Some(Some(&v2)), 0).await?;
                // resuming that replica again
                self.driver.leave(false).await?;
                self.resume_servers(HashSet::from([s])).await?;
                time::sleep(Duration::from_secs(1)).await;
                self.driver.connect().await?;
                let v4 = Self::gen_rand_string(8);
                self.checked_put("Jose", &v4, Some(Some(&v3)), 1).await?;
                break;
            }
        }
        Ok(())
    }

    /// Take snapshot and reset, check previously put key-value.
    async fn test_snapshot_reset(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        let v1 = Self::gen_rand_string(8);
        self.checked_put("Shawn", &v1, Some(None), 0).await?;
        // forcing all nodes to take snapshot
        time::sleep(Duration::from_millis(500)).await;
        self.force_snapshot(HashSet::new()).await?;
        self.checked_put("Jose", &v1, Some(Some(&v0)), 0).await?;
        // resetting all nodes and see if things are there
        self.driver.leave(false).await?;
        self.reset_servers(HashSet::new(), true).await?;
        time::sleep(Duration::from_secs(3)).await;
        self.driver.connect().await?;
        self.checked_get("Shawn", Some(Some(&v1)), 0).await?;
        self.checked_get("Jose", Some(Some(&v1)), 0).await?;
        // forcing all nodes to take snapshot again
        time::sleep(Duration::from_millis(500)).await;
        self.force_snapshot(HashSet::new()).await?;
        // resetting all nodes again and check again
        self.driver.leave(false).await?;
        self.reset_servers(HashSet::new(), true).await?;
        time::sleep(Duration::from_secs(3)).await;
        self.driver.connect().await?;
        self.checked_get("Shawn", Some(Some(&v1)), 0).await?;
        self.checked_get("Jose", Some(Some(&v1)), 0).await?;
        Ok(())
    }

    /// Snapshots keep advancing past instances with nothing to execute, such
    /// as ones of only a config change.
    async fn test_snapshot_progress(&mut self) -> Result<(), SummersetError> {
        if self.protocol == SmrProtocol::QuorumLeases {
            // a config change of nothing, replicated in a batch of its own
            let delta = ConfChange::default();
            loop {
                match self.driver.conf(delta.clone()).await? {
                    DriverReply::Conf { .. } => break,
                    DriverReply::Redirect { .. } => {
                        time::sleep(Duration::from_millis(500)).await;
                        // retry
                    }
                    reply => return logged_err!("config change failed: {:?}", reply),
                }
            }
        }
        let mut starts = vec![];
        for _ in 0..2 {
            self.checked_put("Jose", &Self::gen_rand_string(8), None, 0)
                .await?;
            time::sleep(Duration::from_millis(500)).await;
            starts.push(self.force_snapshot(HashSet::new()).await?);
        }
        for (server, start) in &starts[0] {
            if starts[1].get(server).is_none_or(|next| next <= start) {
                return logged_err!("server {} snapshot stuck at {}", server, start);
            }
        }
        Ok(())
    }

    /// Puts and gets keys while all replicas drop, duplicate, delay, and
    /// reorder peer messages.
    async fn test_lossy_network(&mut self) -> Result<(), SummersetError> {
        let faults = Faults {
            msg_drop_pct: 5,
            msg_dup_pct: 5,
            msg_delay_ms: 1,
            msg_jitter_ms: 4,
            ..self.seeded_faults()
        };
        self.inject_faults(HashSet::new(), faults).await?;
        for i in 0..10 {
            // old values are not checked, as a timed-out put may be retried
            let (key, v) = (format!("Key{i}"), Self::gen_rand_string(8));
            self.checked_put(&key, &v, None, 2).await?;
            self.checked_get(&key, Some(Some(&v)), 2).await?;
        }
        Ok(())
    }

    /// Single non-leader replica gets cut off from the others, then heals.
    async fn test_minority_partition(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        let s = self.pick_server(false).await?;
        self.cut_off_links(s).await?;
        let v1 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v1, Some(Some(&v0)), 0).await?;
        self.checked_get("Jose", Some(Some(&v1)), 0).await?;
        // keep it cut off past its hearing timeout, so that it steps up with a
        // higher ballot and disrupts the others once healed
        time::sleep(Duration::from_millis(2500)).await;
        // heal and give the replica time to catch up
        self.inject_faults(HashSet::new(), Faults::default())
            .await?;
        time::sleep(Duration::from_secs(1)).await;
        self.checked_get("Jose", Some(Some(&v1)), 1).await?;
        Ok(())
    }

    /// Leader replica gets cut off from the others for long enough for them to
    /// pick a new leader, then heals.
    async fn test_leader_partition(&mut self) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        time::sleep(Duration::from_millis(500)).await;
        let s = self.pick_server(true).await?;
        self.driver.leave(false).await?;
        self.cut_off_links(s).await?;
        time::sleep(Duration::from_secs(3)).await;
        self.inject_faults(HashSet::new(), Faults::default())
            .await?;
        time::sleep(Duration::from_secs(1)).await;
        self.driver.connect().await?;
        self.checked_get("Jose", Some(Some(&v0)), 1).await?;
        let v1 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v1, Some(Some(&v0)), 1).await?;
        Ok(())
    }

    /// Crashes a replica and checks that the cluster still serves the right
    /// values once it restarts.
    async fn crash_and_check(&mut self, leader: bool) -> Result<(), SummersetError> {
        let v0 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v0, Some(None), 0).await?;
        let s = self.pick_server(leader).await?;
        self.driver.leave(false).await?;
        self.crash_servers(HashSet::from([s])).await?;
        time::sleep(Duration::from_secs(if leader { 3 } else { 1 })).await;
        self.driver.connect().await?;
        self.checked_get("Jose", Some(Some(&v0)), 1).await?;
        let v1 = Self::gen_rand_string(8);
        self.checked_put("Jose", &v1, Some(Some(&v0)), 1).await?;
        Ok(())
    }

    /// Single non-leader replica crashes and restarts.
    async fn test_non_leader_crash(&mut self) -> Result<(), SummersetError> {
        self.crash_and_check(false).await
    }

    /// Leader replica crashes and restarts.
    async fn test_leader_node_crash(&mut self) -> Result<(), SummersetError> {
        self.crash_and_check(true).await
    }

    /// Puts and gets keys while replicas' clocks run fast or slow.
    async fn test_clock_drift(&mut self) -> Result<(), SummersetError> {
        let faults = self.seeded_faults();
        for s in self.query_servers().await?.into_keys() {
            let drift = if s % 2 == 0 { -40 } else { 40 };
            let faults = Faults {
                clock_drift_pct: drift,
                ..faults.clone()
            };
            self.inject_faults(HashSet::from([s]), faults).await?;
        }
        for i in 0..5 {
            let (key, v) = (format!("Key{i}"), Self::gen_rand_string(8));
            self.checked_put(&key, &v, Some(None), 1).await?;
            self.checked_get(&key, Some(Some(&v)), 1).await?;
        }
        Ok(())
    }

    /// Puts a key repeatedly while every peer message gets duplicated and
    /// reordered; after the leader then crashes, the next leader must hold
    /// exactly the latest value.
    async fn test_duplicate_failover(&mut self) -> Result<(), SummersetError> {
        let faults = Faults {
            msg_dup_pct: 100,
            msg_delay_ms: 1,
            msg_jitter_ms: 50,
            ..self.seeded_faults()
        };
        self.inject_faults(HashSet::new(), faults).await?;
        let mut last: Option<String> = None;
        for _ in 0..8 {
            let v = Self::gen_rand_string(8);
            self.checked_put("Jose", &v, Some(last.as_deref()), 1)
                .await?;
            last = Some(v);
        }
        self.inject_faults(HashSet::new(), Faults::default())
            .await?;

        let leader = self.pick_server(true).await?;
        self.driver.leave(false).await?;
        self.crash_servers(HashSet::from([leader])).await?;
        time::sleep(Duration::from_secs(3)).await;
        self.driver.connect().await?;
        self.checked_get("Jose", Some(last.as_deref()), 1).await?;
        let v = Self::gen_rand_string(8);
        self.checked_put("Jose", &v, Some(last.as_deref()), 1)
            .await?;
        Ok(())
    }

    /// Leader replica crashes twice in a row, each next leader having to carry
    /// over what the previous ones accepted.
    async fn test_b2b_leader_crashes(&mut self) -> Result<(), SummersetError> {
        let mut last: Option<String> = None;
        for _ in 0..2 {
            let v = Self::gen_rand_string(8);
            self.checked_put("Jose", &v, Some(last.as_deref()), 1)
                .await?;
            let leader = self.pick_server(true).await?;
            self.driver.leave(false).await?;
            self.crash_servers(HashSet::from([leader])).await?;
            time::sleep(Duration::from_secs(3)).await;
            self.driver.connect().await?;
            self.checked_get("Jose", Some(Some(&v)), 1).await?;
            last = Some(v);
        }
        Ok(())
    }
}
