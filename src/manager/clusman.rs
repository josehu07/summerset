//! Summerset cluster manager oracle implementation.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use crate::client::ClientId;
use crate::manager::{
    ClientReactor, CtrlMsg, CtrlReply, CtrlRequest, ServerReigner,
};
use crate::protocols::SmrProtocol;
use crate::server::ReplicaId;
use crate::utils::{ConfNum, RespondersConf, SummersetError, ME};

use serde::{Deserialize, Serialize};

use tokio::sync::{mpsc, watch};
use tokio::time::{self, Duration};

/// Information about an active server.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    /// The server's client-facing API address.
    pub api_addr: SocketAddr,

    /// The server's internal peer-peer API address.
    pub p2p_addr: SocketAddr,

    /// This server is a leader? (leader could be non-unique)
    pub is_leader: bool,

    /// This server is a read lease grantor? (only for relevant protocols)
    pub is_grantor: bool,

    /// This server is a read lease grantee? (only for relevant protocols)
    pub is_grantee: bool,

    /// This server is currently paused?
    pub is_paused: bool,

    /// In-mem log start index after latest snapshot.
    pub start_slot: usize,
}

/// Standalone cluster manager oracle.
pub struct ClusterManager {
    /// SMR Protocol in use.
    protocol: SmrProtocol,

    /// Address string for server-facing control messages API.
    _srv_addr: SocketAddr,

    /// Address string for client-facing control events API.
    _cli_addr: SocketAddr,

    /// Total number of server replicas in cluster.
    population: u8,

    /// Receiver side of the server ID assignment channel.
    rx_id_assign: mpsc::UnboundedReceiver<()>,

    /// Sender side of the server ID assignment result channel.
    tx_id_result: mpsc::UnboundedSender<(ReplicaId, u8)>,

    /// Information of current active servers.
    servers_info: HashMap<ReplicaId, ServerInfo>,

    /// Currently assigned server IDs.
    assigned_ids: HashSet<ReplicaId>,

    /// Approximate current responders configuration.
    responders_conf: RespondersConf,

    /// Latest known responders config number.
    latest_conf_num: ConfNum,

    /// ServerReigner module.
    server_reigner: ServerReigner,

    /// ClientReactor module.
    client_reactor: ClientReactor,
}

impl ClusterManager {
    /// Creates a new standalone cluster manager and sets up required
    /// functionality modules.
    pub async fn new_and_setup(
        protocol: SmrProtocol,
        srv_addr: SocketAddr,
        cli_addr: SocketAddr,
        population: u8,
    ) -> Result<Self, SummersetError> {
        if population == 0 {
            return logged_err!("invalid population {}", population);
        }

        ME.get_or_init(|| "m".into());

        let (tx_id_assign, rx_id_assign) = mpsc::unbounded_channel();
        let (tx_id_result, rx_id_result) = mpsc::unbounded_channel();
        let server_reigner =
            ServerReigner::new_and_setup(srv_addr, tx_id_assign, rx_id_result)
                .await?;

        let client_reactor = ClientReactor::new_and_setup(cli_addr).await?;

        Ok(ClusterManager {
            protocol,
            _srv_addr: srv_addr,
            _cli_addr: cli_addr,
            population,
            rx_id_assign,
            tx_id_result,
            servers_info: HashMap::new(),
            assigned_ids: HashSet::new(),
            responders_conf: RespondersConf::empty(population),
            latest_conf_num: 0,
            server_reigner,
            client_reactor,
        })
    }

    /// Assign the first vacant server ID to a new server.
    fn assign_server_id(&mut self) -> Result<(), SummersetError> {
        for id in 0..self.population {
            if !self.assigned_ids.contains(&id) {
                self.tx_id_result.send((id, self.population))?;
                self.assigned_ids.insert(id);
                return Ok(());
            }
        }

        logged_err!("no server ID < population left available")
    }

    /// Main event loop logic of the cluster manager. Breaks out of the loop
    /// only upon catching termination signals to the process.
    pub async fn run(
        &mut self,
        mut rx_term: watch::Receiver<bool>,
    ) -> Result<(), SummersetError> {
        loop {
            tokio::select! {
                // receiving server ID assignment request
                _ = self.rx_id_assign.recv() => {
                    if let Err(e) = self.assign_server_id() {
                        pf_error!("error assigning new server ID: {}", e);
                    }
                },

                // receiving server control message
                ctrl_msg = self.server_reigner.recv_ctrl() => {
                    if let Err(_e) = ctrl_msg {
                        // NOTE: commented out to prevent console lags
                        //       during benchmarking
                        // pf_error!("error receiving ctrl msg: {}", e);
                        continue;
                    }
                    let (server, msg) = ctrl_msg.unwrap();
                    if let Err(e) = self.handle_ctrl_msg(server, msg).await {
                        pf_error!("error handling ctrl msg <- {}: {}",
                                       server, e);
                    }
                },

                // receiving client control request
                ctrl_req = self.client_reactor.recv_req() => {
                    if let Err(_e) = ctrl_req {
                        // NOTE: commented out to prevent console lags
                        //       during benchmarking
                        // pf_error!("error receiving ctrl req: {}", e);
                        continue;
                    }
                    let (client, req) = ctrl_req.unwrap();
                    if let Err(e) = self.handle_ctrl_req(client, req).await {
                        pf_error!("error handling ctrl req <- {}: {}",
                                       client, e);
                    }
                },

                // receiving termination signal
                _ = rx_term.changed() => {
                    pf_warn!("manager caught termination signal");
                    break;
                }
            }
        }

        Ok(())
    }
}

// ClusterManager server-initiated control message handlers
impl ClusterManager {
    /// Handler of NewServerJoin message.
    fn handle_new_server_join(
        &mut self,
        server: ReplicaId,
        protocol: SmrProtocol,
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        if self.servers_info.contains_key(&server) {
            return logged_err!("NewServerJoin got duplicate ID: {}", server);
        }
        if protocol != self.protocol {
            return logged_err!(
                "NewServerJoin with mismatch protocol: {}",
                protocol
            );
        }

        // gather the list of all existing known servers
        let to_peers: HashMap<ReplicaId, SocketAddr> = self
            .servers_info
            .iter()
            .map(|(&server, info)| (server, info.p2p_addr))
            .collect();

        // save new server's info
        self.servers_info.insert(
            server,
            ServerInfo {
                api_addr,
                p2p_addr,
                is_leader: false,
                is_grantor: false,
                is_grantee: false,
                is_paused: false,
                start_slot: 0,
            },
        );

        // tell it to connect to all other existing known servers
        self.server_reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: self.population,
                to_peers,
            },
            server,
        )?;
        Ok(())
    }

    /// Handler of LeaderStatus message.
    fn handle_leader_status(
        &mut self,
        server: ReplicaId,
        step_up: bool,
    ) -> Result<(), SummersetError> {
        if !self.servers_info.contains_key(&server) {
            return logged_err!("manager got unknown server ID: {}", server);
        }

        // update this server's info
        let info = self.servers_info.get_mut(&server).unwrap();
        if step_up && info.is_leader {
            logged_err!("server {} is already marked as leader", server)
        } else if !step_up && !info.is_leader {
            logged_err!("server {} is already marked as non-leader", server)
        } else {
            info.is_leader = step_up;
            Ok(())
        }
    }

    /// Handler of RespondersConf message.
    fn handle_responders_conf(
        &mut self,
        server: ReplicaId,
        conf_num: ConfNum,
        new_conf: RespondersConf,
    ) -> Result<(), SummersetError> {
        if !self.servers_info.contains_key(&server) {
            return logged_err!("manager got unknown server ID: {}", server);
        }

        // update current responders config if number up-to-date
        if conf_num >= self.latest_conf_num {
            self.responders_conf = new_conf;
            self.latest_conf_num = conf_num;
        } else {
            pf_warn!("outdated responders config number {} ignored", conf_num);
        }
        Ok(())
    }

    /// Handler of autonomous SnapshotUpTo message.
    fn handle_snapshot_up_to(
        &mut self,
        server: ReplicaId,
        new_start: usize,
    ) -> Result<(), SummersetError> {
        if !self.servers_info.contains_key(&server) {
            return logged_err!("manager got unknown server ID: {}", server);
        }

        // update this server's info
        let info = self.servers_info.get_mut(&server).unwrap();
        if new_start < info.start_slot {
            logged_err!(
                "server {} snapshot up to {} < {}",
                server,
                new_start,
                self.servers_info[&server].start_slot
            )
        } else {
            info.start_slot = new_start;
            Ok(())
        }
    }

    /// Synthesized handler of server-initiated control messages.
    async fn handle_ctrl_msg(
        &mut self,
        server: ReplicaId,
        msg: CtrlMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            CtrlMsg::NewServerJoin {
                id,
                protocol,
                api_addr,
                p2p_addr,
            } => {
                if id != server {
                    return logged_err!(
                        "NewServerJoin with mismatch ID: {} != {}",
                        id,
                        server
                    );
                }
                self.handle_new_server_join(
                    server, protocol, api_addr, p2p_addr,
                )?;
            }

            CtrlMsg::LeaderStatus { step_up } => {
                self.handle_leader_status(server, step_up)?;
            }

            CtrlMsg::RespondersConf { conf_num, new_conf } => {
                self.handle_responders_conf(server, conf_num, new_conf)?;
            }

            CtrlMsg::SnapshotUpTo { new_start } => {
                self.handle_snapshot_up_to(server, new_start)?;
            }

            _ => {} // ignore all other types
        }

        Ok(())
    }
}

// ClusterManager client-initiated control request handlers
impl ClusterManager {
    /// Handler of client QueryInfo request.
    fn handle_client_query_info(
        &mut self,
        client: ClientId,
    ) -> Result<(), SummersetError> {
        self.client_reactor.send_reply(
            CtrlReply::QueryInfo {
                population: self.population,
                servers_info: self.servers_info.clone(),
            },
            client,
        )
    }

    /// Handler of client QueryConf request.
    fn handle_client_query_conf(
        &mut self,
        client: ClientId,
    ) -> Result<(), SummersetError> {
        self.client_reactor.send_reply(
            CtrlReply::QueryConf {
                conf_num: self.latest_conf_num,
                now_conf: self.responders_conf.clone(),
            },
            client,
        )
    }

    /// Handler of client ResetServers request.
    async fn handle_client_reset_servers(
        &mut self,
        client: ClientId,
        servers: HashSet<ReplicaId>,
        durable: bool,
    ) -> Result<(), SummersetError> {
        let num_replicas = self.servers_info.len();
        let mut servers: Vec<ReplicaId> = if servers.is_empty() {
            // all active servers
            self.servers_info.keys().copied().collect()
        } else {
            servers.into_iter().collect()
        };

        // reset specified server(s)
        let mut reset_done = HashSet::new();
        while let Some(s) = servers.pop() {
            // send reset server control message to server
            self.server_reigner
                .send_ctrl(CtrlMsg::ResetState { durable }, s)?;

            // remove information about this server
            debug_assert!(self.assigned_ids.contains(&s));
            debug_assert!(self.servers_info.contains_key(&s));
            self.assigned_ids.remove(&s);
            self.servers_info.remove(&s);

            // wait for the new server ID assignment request from it
            self.rx_id_assign.recv().await;
            if let Err(e) = self.assign_server_id() {
                return logged_err!("error assigning new server ID: {}", e);
            }

            // wait a while to ensure the server's transport hub is setup
            time::sleep(Duration::from_millis(500)).await;

            reset_done.insert(s);
        }

        // now the reset servers should be sending NewServerJoin messages to
        // me. Process them until all servers joined
        while self.servers_info.len() < num_replicas {
            let (s, msg) = self.server_reigner.recv_ctrl().await?;
            if let Err(e) = self.handle_ctrl_msg(s, msg).await {
                pf_error!("error handling ctrl msg <- {}: {}", s, e);
            }
        }

        self.client_reactor.send_reply(
            CtrlReply::ResetServers {
                servers: reset_done,
            },
            client,
        )
    }

    /// Handler of client PauseServers request.
    async fn handle_client_pause_servers(
        &mut self,
        client: ClientId,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let mut servers: Vec<ReplicaId> = if servers.is_empty() {
            // all active servers
            self.servers_info.keys().copied().collect()
        } else {
            servers.into_iter().collect()
        };

        // pause specified server(s)
        let mut pause_done = HashSet::new();
        while let Some(s) = servers.pop() {
            // send pause server control message to server
            self.server_reigner.send_ctrl(CtrlMsg::Pause, s)?;

            // set the is_paused flag
            debug_assert!(self.servers_info.contains_key(&s));
            self.servers_info.get_mut(&s).unwrap().is_paused = true;

            // wait for dummy reply
            loop {
                let (server, reply) = self.server_reigner.recv_ctrl().await?;
                if server != s || reply != CtrlMsg::PauseReply {
                    self.handle_ctrl_msg(server, reply).await?;
                } else {
                    break;
                }
            }

            pause_done.insert(s);
        }

        self.client_reactor.send_reply(
            CtrlReply::PauseServers {
                servers: pause_done,
            },
            client,
        )
    }

    /// Handler of client ResumeServers request.
    async fn handle_client_resume_servers(
        &mut self,
        client: ClientId,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let mut servers: Vec<ReplicaId> = if servers.is_empty() {
            // all active servers
            self.servers_info.keys().copied().collect()
        } else {
            servers.into_iter().collect()
        };

        // resume specified server(s)
        let mut resume_done = HashSet::new();
        while let Some(s) = servers.pop() {
            // send resume server control message to server
            self.server_reigner.send_ctrl(CtrlMsg::Resume, s)?;

            // wait for dummy reply
            loop {
                let (server, reply) = self.server_reigner.recv_ctrl().await?;
                if server != s || reply != CtrlMsg::ResumeReply {
                    self.handle_ctrl_msg(server, reply).await?;
                } else {
                    break;
                }
            }

            // clear the is_paused flag
            debug_assert!(self.servers_info.contains_key(&s));
            self.servers_info.get_mut(&s).unwrap().is_paused = false;

            resume_done.insert(s);
        }

        self.client_reactor.send_reply(
            CtrlReply::ResumeServers {
                servers: resume_done,
            },
            client,
        )
    }

    /// Handler of client TakeSnapshot request.
    async fn handle_client_take_snapshot(
        &mut self,
        client: ClientId,
        servers: HashSet<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let mut servers: Vec<ReplicaId> = if servers.is_empty() {
            // all active servers
            self.servers_info.keys().copied().collect()
        } else {
            servers.into_iter().collect()
        };

        // tell specified server(s)
        let mut snapshot_up_to = HashMap::new();
        while let Some(s) = servers.pop() {
            // send take snapshot control message to server
            self.server_reigner.send_ctrl(CtrlMsg::TakeSnapshot, s)?;

            // wait for reply
            loop {
                let (server, reply) = self.server_reigner.recv_ctrl().await?;
                match reply {
                    CtrlMsg::SnapshotUpTo { new_start } if server == s => {
                        // update the log start index info
                        debug_assert!(self.servers_info.contains_key(&s));
                        if new_start < self.servers_info[&s].start_slot {
                            return logged_err!(
                                "server {} snapshot up to {} < {}",
                                s,
                                new_start,
                                self.servers_info[&s].start_slot
                            );
                        } else {
                            self.servers_info.get_mut(&s).unwrap().start_slot =
                                new_start;
                        }

                        snapshot_up_to.insert(s, new_start);
                        break;
                    }

                    _ => self.handle_ctrl_msg(server, reply).await?,
                }
            }
        }

        self.client_reactor
            .send_reply(CtrlReply::TakeSnapshot { snapshot_up_to }, client)
    }

    /// Synthesized handler of client-initiated control requests.
    async fn handle_ctrl_req(
        &mut self,
        client: ClientId,
        req: CtrlRequest,
    ) -> Result<(), SummersetError> {
        match req {
            CtrlRequest::QueryInfo => {
                self.handle_client_query_info(client)?;
            }

            CtrlRequest::QueryConf => {
                self.handle_client_query_conf(client)?;
            }

            CtrlRequest::ResetServers { servers, durable } => {
                self.handle_client_reset_servers(client, servers, durable)
                    .await?;
            }

            CtrlRequest::PauseServers { servers } => {
                self.handle_client_pause_servers(client, servers).await?;
            }

            CtrlRequest::ResumeServers { servers } => {
                self.handle_client_resume_servers(client, servers).await?;
            }

            CtrlRequest::TakeSnapshot { servers } => {
                self.handle_client_take_snapshot(client, servers).await?;
            }

            _ => {} // ignore all other types
        }

        Ok(())
    }
}
