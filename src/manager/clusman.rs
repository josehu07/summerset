//! Summerset cluster manager oracle implementation.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::manager::{
    CtrlMsg, ServerReigner, CtrlRequest, CtrlReply, ClientReactor,
};
use crate::server::ReplicaId;
use crate::client::ClientId;
use crate::protocols::SmrProtocol;

use tokio::sync::{mpsc, watch};

/// Information about an active server.
#[derive(Debug, Clone)]
struct ServerInfo {
    /// The server's client-facing API address.
    api_addr: SocketAddr,

    /// The server's internal peer-peer API address.
    p2p_addr: SocketAddr,

    /// This server is a leader (leader could be non-unique).
    is_leader: bool,
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

    /// ServerReigner module.
    server_reigner: ServerReigner,

    /// Receiver side of the server ID assignment channel.
    rx_id_assign: mpsc::UnboundedReceiver<()>,

    /// Sender side of the server ID assignment result channel.
    tx_id_result: mpsc::UnboundedSender<(ReplicaId, u8)>,

    /// ClientReactor module.
    client_reactor: ClientReactor,

    /// Information of current active servers.
    server_info: HashMap<ReplicaId, ServerInfo>,

    /// Currently assigned server IDs.
    assigned_ids: HashSet<ReplicaId>,
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
            return logged_err!("m"; "invalid population {}", population);
        }

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
            server_reigner,
            rx_id_assign,
            tx_id_result,
            client_reactor,
            server_info: HashMap::new(),
            assigned_ids: HashSet::new(),
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

        logged_err!("m"; "no server ID < population left available")
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
                        pf_error!("m"; "error assigning new server ID: {}", e);
                    }
                },

                // receiving server control message
                ctrl_msg = self.server_reigner.recv_ctrl() => {
                    if let Err(e) = ctrl_msg {
                        pf_error!("m"; "error receiving ctrl msg: {}", e);
                        continue;
                    }
                    let (server, msg) = ctrl_msg.unwrap();
                    if let Err(e) = self.handle_ctrl_msg(server, msg).await {
                        pf_error!("m"; "error handling ctrl msg <- {}: {}",
                                       server, e);
                    }
                },

                // receiving client control request
                ctrl_req = self.client_reactor.recv_req() => {
                    if let Err(e) = ctrl_req {
                        pf_error!("m"; "error receiving ctrl req: {}", e);
                        continue;
                    }
                    let (client, req) = ctrl_req.unwrap();
                    if let Err(e) = self.handle_ctrl_req(client, req).await {
                        pf_error!("m"; "error handling ctrl req <- {}: {}",
                                       client, e);
                    }
                },

                // receiving termination signal
                _ = rx_term.changed() => {
                    pf_warn!("m"; "manager caught termination signal");
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
        if self.server_info.contains_key(&server) {
            return logged_err!("m"; "server join got duplicate ID: {}",
                                    server);
        }
        if protocol != self.protocol {
            return logged_err!("m"; "server join with mismatch protocol: {}",
                                    protocol);
        }

        // tell it to connect to all existing known servers
        let to_peers: HashMap<ReplicaId, SocketAddr> = self
            .server_info
            .iter()
            .map(|(&server, info)| (server, info.p2p_addr))
            .collect();
        self.server_reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: self.population,
                to_peers,
            },
            server,
        )?;

        // save new server's info
        self.server_info.insert(
            server,
            ServerInfo {
                api_addr,
                p2p_addr,
                is_leader: false,
            },
        );
        Ok(())
    }

    /// Handler of LeaderStatus message.
    fn handle_leader_status(
        &mut self,
        server: ReplicaId,
        step_up: bool,
    ) -> Result<(), SummersetError> {
        if !self.server_info.contains_key(&server) {
            return logged_err!("m"; "leader status got unknown ID: {}", server);
        }

        // update this server's info
        let info = self.server_info.get_mut(&server).unwrap();
        if step_up && info.is_leader {
            logged_err!("m"; "server {} is already marked as leader", server)
        } else if !step_up && !info.is_leader {
            logged_err!("m"; "server {} is already marked as non-leader", server)
        } else {
            info.is_leader = step_up;
            Ok(())
        }
    }

    /// Synthesized handler of server-initiated control messages.
    async fn handle_ctrl_msg(
        &mut self,
        server: ReplicaId,
        msg: CtrlMsg,
    ) -> Result<(), SummersetError> {
        #[allow(clippy::single_match)]
        match msg {
            CtrlMsg::NewServerJoin {
                id,
                protocol,
                api_addr,
                p2p_addr,
            } => {
                if id != server {
                    return logged_err!("m"; "server join with mismatch ID: {} != {}",
                                            id, server);
                }
                self.handle_new_server_join(
                    server, protocol, api_addr, p2p_addr,
                )?;
            }

            CtrlMsg::LeaderStatus { step_up } => {
                self.handle_leader_status(server, step_up)?;
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
        // gather public addresses of all active servers
        let servers: HashMap<ReplicaId, (SocketAddr, bool)> = self
            .server_info
            .iter()
            .map(|(&server, info)| (server, (info.api_addr, info.is_leader)))
            .collect();
        self.client_reactor
            .send_reply(CtrlReply::QueryInfo { servers }, client)
    }

    /// Handler of client ResetServers request.
    async fn handle_client_reset_servers(
        &mut self,
        client: ClientId,
        servers: HashSet<ReplicaId>,
        durable: bool,
    ) -> Result<(), SummersetError> {
        let num_replicas = self.server_info.len();
        let mut servers: Vec<ReplicaId> = if servers.is_empty() {
            // all active servers
            self.server_info.keys().copied().collect()
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
            assert!(self.assigned_ids.contains(&s));
            assert!(self.server_info.contains_key(&s));
            self.assigned_ids.remove(&s);
            self.server_info.remove(&s);

            // wait for the new server ID assignment request from it
            self.rx_id_assign.recv().await;
            if let Err(e) = self.assign_server_id() {
                return logged_err!("m"; "error assigning new server ID: {}", e);
            }

            reset_done.insert(s);
        }

        // now the reset servers should be sending NewServerJoin messages to
        // me. Process them until all servers joined
        while self.server_info.len() < num_replicas {
            let (s, msg) = self.server_reigner.recv_ctrl().await?;
            if let Err(e) = self.handle_ctrl_msg(s, msg).await {
                pf_error!("m"; "error handling ctrl msg <- {}: {}", s, e);
            }
        }

        self.client_reactor.send_reply(
            CtrlReply::ResetServers {
                servers: reset_done,
            },
            client,
        )
    }

    /// Synthesized handler of client-initiated control requests.
    async fn handle_ctrl_req(
        &mut self,
        client: ClientId,
        req: CtrlRequest,
    ) -> Result<(), SummersetError> {
        #[allow(clippy::single_match)]
        match req {
            CtrlRequest::QueryInfo => {
                self.handle_client_query_info(client)?;
            }

            CtrlRequest::ResetServers { servers, durable } => {
                self.handle_client_reset_servers(client, servers, durable)
                    .await?;
            }

            _ => {} // ignore all other types
        }

        Ok(())
    }
}
