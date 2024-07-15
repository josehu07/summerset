//! One-shot client issuing control commands for testing purposes.

use std::collections::{HashMap, HashSet};

use crate::drivers::DriverClosedLoop;

use serde::Deserialize;

use tokio::time::Duration;

use summerset::{
    logged_err, parsed_config, pf_error, pf_info, CtrlReply, CtrlRequest,
    GenericEndpoint, ReplicaId, ServerInfo, SummersetError,
};

/// Mod parameters struct.
#[derive(Debug, Deserialize)]
pub struct ModeParamsMess {
    /// Comma-separated list of servers to pause.
    pub pause: String,

    /// Comma-separated list of servers to resume.
    pub resume: String,
}

#[allow(clippy::derivable_impls)]
impl Default for ModeParamsMess {
    fn default() -> Self {
        ModeParamsMess {
            pause: "".into(),
            resume: "".into(),
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
                                    pause, resume)?;

        Ok(ClientMess {
            driver: DriverClosedLoop::new(endpoint, timeout),
            params,
            servers_info: None,
        })
    }

    /// Parse comma-separated string of server IDs.
    fn parse_comma_separated(
        &self,
        list_str: &str,
    ) -> Result<HashSet<ReplicaId>, SummersetError> {
        let mut servers = HashSet::new();
        for s in list_str.split(',') {
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
            } else {
                // else, should be a numerical replica ID
                servers.insert(s.parse()?);
            }
        }
        Ok(servers)
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

    /// Runs the one-shot client to make specified control requests.
    pub(crate) async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;
        self.get_servers_info().await?;

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

        self.driver.leave(true).await?;
        Ok(())
    }
}
