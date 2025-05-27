//! One-shot client issuing control commands for testing purposes.

use std::collections::{HashMap, HashSet};

use crate::drivers::{DriverClosedLoop, DriverReply};

use serde::Deserialize;

use tokio::time::Duration;

use summerset::{
    logged_err, parsed_config, pf_error, pf_info, Bitmap, CommandResult,
    ConfChange, CtrlReply, CtrlRequest, GenericEndpoint, ReplicaId, ServerInfo,
    SummersetError,
};

/// Mod parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsMess {
    /// Comma-separated list of servers to pause.
    /// Use special letter 'a' for all servers or 'l' for current leader.
    pub pause: String,

    /// Comma-separated list of servers to resume.
    /// Use special letter 'a' for all servers or 'l' for current leader.
    pub resume: String,

    /// String form of configured leader ID (or empty string).
    /// Only used by relevant protocols.
    pub leader: String,

    /// Range of keys to apply the following responders set conf.
    /// Only used by relevant protocols.
    ///   - if "ka-kb", implies the range [ka, kb]
    ///   - if special string "full", implies the full range
    ///   - if special string "reset", implies a conf reset
    pub key_range: String,

    /// Comma-separated list of servers as configured responders.
    /// Only used by relevant protocols.
    pub responder: String,

    /// Single-shot write request for experimental purpose.
    /// Expects the format "key:value".
    pub write: String,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsMess {
    fn default() -> Self {
        ModeParamsMess {
            pause: "".into(),
            resume: "".into(),
            leader: "/".into(),
            key_range: "/".into(),
            responder: "/".into(),
            write: "".into(),
        }
    }
}

/// One-shot control client struct.
pub(crate) struct ClientMess {
    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// Mode parameters struct.
    params: ModeParamsMess,

    /// Map from replica ID -> (addr, is_leader), queried from manager.
    servers_info: Option<HashMap<ReplicaId, ServerInfo>>,
}

impl ClientMess {
    /// Creates a new one-shot control client.
    pub(crate) fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsMess;
                                      pause, resume, leader,
                                      key_range, responder, write)?;

        Ok(ClientMess {
            driver: DriverClosedLoop::new(endpoint, timeout),
            params,
            servers_info: None,
        })
    }

    /// Parse string into optional server ID.
    fn parse_optional_server(
        &self,
        id_str: &str,
    ) -> Result<Option<ReplicaId>, SummersetError> {
        let s = id_str.trim();
        if s.is_empty() || s == "/" {
            Ok(None)
        } else {
            Ok(Some(s.parse()?))
        }
    }

    /// Parse comma-separated string of server IDs.
    fn parse_comma_separated(
        &self,
        list_str: &str,
    ) -> Result<HashSet<ReplicaId>, SummersetError> {
        let mut servers = HashSet::new();
        for s in list_str.trim().split(',') {
            if s == "l" && self.servers_info.is_some() {
                // special character 'l' means leader(s)
                for (&id, info) in self.servers_info.as_ref().unwrap() {
                    if info.is_leader {
                        servers.insert(id);
                    }
                }
            } else if s == "a" && self.servers_info.is_some() {
                // special character 'a' means all servers
                for &id in self.servers_info.as_ref().unwrap().keys() {
                    servers.insert(id);
                }
            } else if s != "/" {
                // else, should be a numerical replica ID
                servers.insert(s.parse()?);
            }
        }
        Ok(servers)
    }

    /// Parse the `key_range` field for conf changes. Returns:
    ///   - `Some(Some((ka, kb)))` if a specified range
    ///   - `Some(None)` if a full range
    ///   - `None` if a conf reset indicated
    fn parse_conf_key_range(
        &self,
        range_str: &str,
    ) -> Result<Option<Option<(String, String)>>, SummersetError> {
        match range_str {
            "full" => Ok(Some(None)),
            "reset" => Ok(None),
            _ => {
                let mut range = vec![];
                for s in range_str.trim().split('-') {
                    range.push(s.to_string());
                }
                if range.len() != 2 {
                    logged_err!("invalid key_range: {}", range_str)
                } else {
                    let mut range_drain = range.into_iter();
                    Ok(Some(Some((
                        range_drain.next().unwrap(),
                        range_drain.next().unwrap(),
                    ))))
                }
            }
        }
    }

    /// Parse the `write` field for a single-shot write. Returns a key-value
    /// pair on success.
    fn parse_write_key_value(
        &self,
        write_str: &str,
    ) -> Result<(String, String), SummersetError> {
        if let Some((key, value)) = write_str.trim().split_once(':') {
            Ok((key.to_string(), value.to_string()))
        } else {
            logged_err!("invalid write_str: {}", write_str)
        }
    }

    /// Query the manager for current servers info.
    async fn get_servers_info(&mut self) -> Result<(), SummersetError> {
        if self.servers_info.is_none() {
            self.driver
                .ctrl_stub()
                .send_req_insist(&CtrlRequest::QueryInfo)?;

            let reply = self.driver.ctrl_stub().recv_reply().await?;
            match reply {
                CtrlReply::QueryInfo {
                    population,
                    servers_info,
                } => {
                    debug_assert_eq!(servers_info.len() as u8, population);
                    self.servers_info = Some(servers_info);
                }
                _ => return logged_err!("unexpected control reply type"),
            }
        }

        Ok(())
    }

    /// Pause the list of servers.
    async fn pause_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let req = CtrlRequest::PauseServers { servers };
        self.driver.ctrl_stub().send_req_insist(&req)?;

        let reply = self.driver.ctrl_stub().recv_reply().await?;
        match reply {
            CtrlReply::PauseServers { .. } => Ok(()),
            _ => logged_err!("unexpected control reply type"),
        }
    }

    /// Resume the list of servers.
    async fn resume_servers(
        &mut self,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let req = CtrlRequest::ResumeServers { servers };
        self.driver.ctrl_stub().send_req_insist(&req)?;

        let reply = self.driver.ctrl_stub().recv_reply().await?;
        match reply {
            CtrlReply::ResumeServers { .. } => Ok(()),
            _ => logged_err!("unexpected control reply type"),
        }
    }

    /// Make a responders configuration change.
    async fn responders_conf_change(
        &mut self,
        conf: ConfChange,
    ) -> Result<(), SummersetError> {
        loop {
            match self.driver.conf(conf.clone()).await? {
                DriverReply::Redirect { .. } => {
                    // retry
                }
                DriverReply::Conf { changed, .. } => {
                    if changed {
                        return Ok(());
                    } else {
                        return logged_err!(
                            "responders conf change ignored (invalid?)"
                        );
                    }
                }
                _ => {
                    return logged_err!("unexpected driver reply type");
                }
            }
        }
    }

    /// Make a single-shot write request.
    async fn single_shot_write(
        &mut self,
        key: String,
        value: String,
    ) -> Result<(), SummersetError> {
        loop {
            let reply = self.driver.put(&key, &value).await?;
            match reply {
                DriverReply::Redirect { .. } => {
                    // retry
                }
                DriverReply::Success { cmd_result, .. } => match cmd_result {
                    CommandResult::Put { .. } => {
                        return Ok(());
                    }
                    _ => {
                        return logged_err!("unexpected command result type");
                    }
                },
                _ => {
                    return logged_err!("unexpected driver reply type");
                }
            }
        }
    }

    /// Runs the one-shot client to make specified control requests.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;
        self.get_servers_info().await?;

        // pause and resume
        if !self.params.pause.is_empty() {
            let servers = self.parse_comma_separated(&self.params.pause)?;
            pf_info!("pausing servers {:?}", servers);
            self.pause_servers(servers).await?;
        }
        if !self.params.resume.is_empty() {
            let servers = self.parse_comma_separated(&self.params.resume)?;
            pf_info!("resuming servers {:?}", servers);
            self.resume_servers(servers).await?;
        }

        // responders config change
        // unless all three fields are the special character '/', they will
        // be parsed at the same time
        if !(self.params.leader == "/"
            && self.params.key_range == "/"
            && self.params.responder == "/")
        {
            let leader = self.parse_optional_server(&self.params.leader)?;
            let range = self.parse_conf_key_range(&self.params.key_range)?;
            let responders =
                self.parse_comma_separated(&self.params.responder)?;
            let delta = if let Some(range) = range {
                ConfChange {
                    reset: false,
                    leader,
                    range,
                    responders: Some(Bitmap::from((
                        self.driver.population(),
                        responders,
                    ))),
                }
            } else {
                ConfChange {
                    reset: true,
                    leader: None,
                    range: None,
                    responders: None,
                }
            };
            pf_info!("responders conf change {:?}", delta);
            self.responders_conf_change(delta).await?;
        }

        // single-shot write
        if !self.params.write.is_empty() {
            let (key, value) =
                self.parse_write_key_value(&self.params.write)?;
            pf_info!("single-shot write {}:{}", key, value);
            self.single_shot_write(key, value).await?;
        }

        self.driver.leave(true).await?;
        Ok(())
    }
}
