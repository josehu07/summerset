//! Replication protocol: Raft.
//!
//! References:
//!   - <https://raft.github.io/raft.pdf>
//!   - <https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf>

use crate::utils::{SummersetError, Bitmap, Timer};

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ReplicaConfigRaft {
    /// Client request batching interval in microsecs.
    pub batch_interval_us: u64,

    /// Client request batching maximum batch size.
    pub max_batch_size: usize,

    /// Path to backing log file.
    pub backer_path: String,

    /// Whether to call `fsync()`/`fdatasync()` on logger.
    pub logger_sync: bool,

    // Performance simulation params (all zeros means no perf simulation):
    pub perf_storage_a: u64,
    pub perf_storage_b: u64,
    pub perf_network_a: u64,
    pub perf_network_b: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ReplicaConfigRaft {
    fn default() -> Self {
        ReplicaConfigRaft {
            batch_interval_us: 1000,
            max_batch_size: 5000,
            backer_path: "/tmp/summerset.raft.wal".into(),
            logger_sync: false,
            perf_storage_a: 0,
            perf_storage_b: 0,
            perf_network_a: 0,
            perf_network_b: 0,
        }
    }
}

/// Raft server replica module.
pub struct RaftReplica {
    /// Replica ID in cluster.
    id: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Majority quorum size.
    quorum_cnt: u8,

    /// Configuration parameters struct.
    config: ReplicaConfigRaft,
}

#[async_trait]
impl GenericReplica for RaftReplica {
    async fn new_and_setup(
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        Ok(RaftReplica {
            id,
            population,
            quorum_cnt: (population / 2) + 1,
            config,
            _api_addr: api_addr,
            _p2p_addr: p2p_addr,
        })
    }

    async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<bool, SummersetError> {
    }

    fn id(&self) -> ReplicaId {
        self.id
    }
}

/// Configuration parameters struct.
#[derive(Debug, Deserialize)]
pub struct ClientConfigRaft {
    /// Which server to pick initially.
    pub init_server_id: ReplicaId,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigRaft {
    fn default() -> Self {
        ClientConfigRaft { init_server_id: 0 }
    }
}

/// Raft client-side module.
pub struct RaftClient {
    /// Client ID.
    id: ClientId,

    /// Configuration parameters struct.
    _config: ClientConfigRaft,

    /// List of active servers information.
    servers: HashMap<ReplicaId, SocketAddr>,

    /// Current server ID to talk to.
    server_id: ReplicaId,

    /// Control API stub to the cluster manager.
    ctrl_stub: ClientCtrlStub,

    /// API stubs for communicating with servers.
    api_stubs: HashMap<ReplicaId, ClientApiStub>,
}

#[async_trait]
impl GenericEndpoint for RaftClient {
    async fn new_and_setup(
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Self, SummersetError> {
        // connect to the cluster manager and get assigned a client ID
        pf_info!("c"; "connecting to manager '{}'...", manager);
        let ctrl_stub = ClientCtrlStub::new_by_connect(manager).await?;
        let id = ctrl_stub.id;

        // parse protocol-specific configs
        let config = parsed_config!(config_str => ClientConfigRaft;
                                    init_server_id)?;
        let init_server_id = config.init_server_id;

        Ok(RaftClient {
            id,
            _config: config,
            servers: HashMap::new(),
            server_id: init_server_id,
            ctrl_stub,
            api_stubs: HashMap::new(),
        })
    }

    async fn connect(&mut self) -> Result<(), SummersetError> {
        // disallow reconnection without leaving
        if !self.api_stubs.is_empty() {
            return logged_err!(self.id; "reconnecting without leaving");
        }

        // ask the manager about the list of active servers
        let mut sent =
            self.ctrl_stub.send_req(Some(&CtrlRequest::QueryInfo))?;
        while !sent {
            sent = self.ctrl_stub.send_req(None)?;
        }

        let reply = self.ctrl_stub.recv_reply().await?;
        match reply {
            CtrlReply::QueryInfo {
                population,
                servers,
            } => {
                // shift to a new server_id if current one not active
                assert!(!servers.is_empty());
                while !servers.contains_key(&self.server_id) {
                    self.server_id = (self.server_id + 1) % population;
                }
                // establish connection to all servers
                self.servers = servers
                    .into_iter()
                    .map(|(id, info)| (id, info.0))
                    .collect();
                for (&id, &server) in &self.servers {
                    pf_info!(self.id; "connecting to server {} '{}'...", id, server);
                    let api_stub =
                        ClientApiStub::new_by_connect(self.id, server).await?;
                    self.api_stubs.insert(id, api_stub);
                }
                Ok(())
            }
            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }

    async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        // send leave notification to all servers
        for (id, mut api_stub) in self.api_stubs.drain() {
            let mut sent = api_stub.send_req(Some(&ApiRequest::Leave))?;
            while !sent {
                sent = api_stub.send_req(None)?;
            }

            while api_stub.recv_reply().await? != ApiReply::Leave {}
            pf_info!(self.id; "left server connection {}", id);
            api_stub.forget();
        }

        // if permanently leaving, send leave notification to the manager
        if permanent {
            let mut sent =
                self.ctrl_stub.send_req(Some(&CtrlRequest::Leave))?;
            while !sent {
                sent = self.ctrl_stub.send_req(None)?;
            }

            while self.ctrl_stub.recv_reply().await? != CtrlReply::Leave {}
            pf_info!(self.id; "left manager connection");
        }

        Ok(())
    }

    fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError> {
        if self.api_stubs.contains_key(&self.server_id) {
            self.api_stubs
                .get_mut(&self.server_id)
                .unwrap()
                .send_req(req)
        } else {
            Err(SummersetError(format!(
                "server_id {} not in api_stubs",
                self.server_id
            )))
        }
    }

    async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        if self.api_stubs.contains_key(&self.server_id) {
            let reply = self
                .api_stubs
                .get_mut(&self.server_id)
                .unwrap()
                .recv_reply()
                .await?;

            if let ApiReply::Reply {
                ref result,
                ref redirect,
                ..
            } = reply
            {
                // if the current server redirects me to a different server
                if result.is_none() && redirect.is_some() {
                    let redirect_id = redirect.unwrap();
                    assert!(self.servers.contains_key(&redirect_id));
                    self.server_id = redirect_id;
                    pf_debug!(self.id; "redirected to replica {} '{}'",
                                       redirect_id, self.servers[&redirect_id]);
                }
            }

            Ok(reply)
        } else {
            Err(SummersetError(format!(
                "server_id {} not in api_stubs",
                self.server_id
            )))
        }
    }

    fn id(&self) -> ClientId {
        self.id
    }

    fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        &mut self.ctrl_stub
    }
}
