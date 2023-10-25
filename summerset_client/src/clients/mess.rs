//! One-shot client issuing control commands for testing purposes.

use std::collections::HashSet;

use crate::drivers::DriverClosedLoop;

use serde::Deserialize;

use tokio::time::Duration;

use summerset::{
    ReplicaId, GenericEndpoint, CtrlRequest, CtrlReply, SummersetError,
    pf_info, pf_error, logged_err, parsed_config,
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
pub struct ClientMess {
    /// Closed-loop request driver.
    driver: DriverClosedLoop,

    /// Mode parameters struct.
    params: ModeParamsMess,
}

impl ClientMess {
    /// Creates a new one-shot control client.
    pub fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
        params_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        let params = parsed_config!(params_str => ModeParamsMess;
                                    pause, resume)?;

        Ok(ClientMess {
            driver: DriverClosedLoop::new(endpoint, timeout),
            params,
        })
    }

    /// Parse comma-separated string of server IDs.
    fn parse_comma_separated(
        list_str: &str,
    ) -> Result<Vec<ReplicaId>, SummersetError> {
        let mut servers = vec![];
        for s in list_str.split(',') {
            servers.push(s.parse()?);
        }
        Ok(servers)
    }

    /// Pause the list of servers.
    async fn pause_servers(
        &mut self,
        servers: Vec<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let servers = HashSet::from_iter(servers.into_iter());
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

    /// Resume the list of servers.
    async fn resume_servers(
        &mut self,
        servers: Vec<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let servers = HashSet::from_iter(servers.into_iter());
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

    /// Runs the one-shot client to make specified control requests.
    pub async fn run(&mut self) -> Result<(), SummersetError> {
        self.driver.connect().await?;

        if !self.params.pause.is_empty() {
            let servers = Self::parse_comma_separated(&self.params.pause)?;
            pf_info!(self.driver.id; "pausing servers {:?}", servers);
            self.pause_servers(servers).await?;
        }

        if !self.params.resume.is_empty() {
            let servers = Self::parse_comma_separated(&self.params.resume)?;
            pf_info!(self.driver.id; "resuming servers {:?}", servers);
            self.resume_servers(servers).await?;
        }

        Ok(())
    }
}