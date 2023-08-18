//! Summerset cluster manager oracle implementation.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::manager::{
    CtrlMsg, ServerReigner, CtrlRequest, CtrlReply, ClientReactor,
};
use crate::server::ReplicaId;
use crate::client::ClientId;
use crate::protocols::SmrProtocol;

/// Information about an active server.
// TODO: maybe add things like leader info, etc.
#[derive(Debug, Clone)]
struct ServerInfo {
    /// The server's client-facing API address.
    api_addr: SocketAddr,

    /// The server's internal peer-peer API address.
    p2p_addr: SocketAddr,
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

    /// ClientReactor module.
    client_reactor: ClientReactor,

    /// Information of current active servers.
    server_info: HashMap<ReplicaId, ServerInfo>,
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

        let server_reigner = ServerReigner::new_and_setup(srv_addr).await?;
        let client_reactor = ClientReactor::new_and_setup(cli_addr).await?;

        Ok(ClusterManager {
            protocol,
            _srv_addr: srv_addr,
            _cli_addr: cli_addr,
            population,
            server_reigner,
            client_reactor,
            server_info: HashMap::new(),
        })
    }

    /// Main event loop logic of the cluster manager.
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // receiving server control message
                ctrl_msg = self.server_reigner.recv_ctrl() => {
                    if let Err(e) = ctrl_msg {
                        pf_error!("m"; "error receiving ctrl msg: {}", e);
                        continue;
                    }
                    let (server, msg) = ctrl_msg.unwrap();
                    if let Err(e) = self.handle_ctrl_msg(server, msg) {
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
                    if let Err(e) = self.handle_ctrl_req(client, req) {
                        pf_error!("m"; "error handling ctrl req <- {}: {}",
                                       client, e);
                    }
                },
            }
        }
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
        self.server_info
            .insert(server, ServerInfo { api_addr, p2p_addr });
        Ok(())
    }

    /// Synthesized handler of server-initiated control messages.
    fn handle_ctrl_msg(
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
        let servers: HashMap<ReplicaId, SocketAddr> = self
            .server_info
            .iter()
            .map(|(&server, info)| (server, info.api_addr))
            .collect();
        self.client_reactor
            .send_reply(CtrlReply::QueryInfo { servers }, client)
    }

    /// Synthesized handler of client-initiated control requests.
    fn handle_ctrl_req(
        &mut self,
        client: ClientId,
        req: CtrlRequest,
    ) -> Result<(), SummersetError> {
        #[allow(clippy::single_match)]
        match req {
            CtrlRequest::QueryInfo => {
                self.handle_client_query_info(client)?;
            }

            _ => {} // ignore all other types
        }

        Ok(())
    }
}
