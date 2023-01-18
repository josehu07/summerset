//! Replication protocol: simple push.

use tonic::{Request, Response, Status};
use tonic::transport::{Server, Channel};
use crate::external_api_proto::external_api_client::ExternalApiClient;

mod simple_push_proto {
    tonic::include_proto!("simple_push");
}
use simple_push_proto::{PushRecordRequest, PushRecordReply};
use simple_push_proto::simple_push_client::SimplePushClient;
use simple_push_proto::simple_push_server::{SimplePush, SimplePushServer};

use crate::smr_server::ServerRpcSender;
use crate::smr_client::ClientRpcSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{ReplicatorServerNode, ReplicatorClientStub};
use crate::utils::{SummersetError, InitError};

use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Mutex;
use rand::Rng;
use tokio::runtime::Runtime;

/// SimplePush replication protocol server module. TODO: description.
#[derive(Debug, Default)]
pub struct SimplePushServerNode {
    /// List of peer nodes addresses.
    peers: Vec<String>,

    /// Map from peer index to RPC connection client.
    conns: HashMap<usize, SimplePushClient<Channel>>,
}

impl ReplicatorServerNode for SimplePushServerNode {
    /// Create a new SimplePush protocol server module.
    fn new(
        peers: Vec<String>,
        smr_addr: SocketAddr,
        main_runtime: &Runtime,
    ) -> Result<Self, InitError> {
        let node = SimplePushServerNode {
            peers, // save peers list
            conns: HashMap::new(),
        };

        // add and start the internal communication tonic service
        main_runtime.spawn(
            Server::builder()
                .add_service(SimplePushServer::new(node))
                .serve(smr_addr),
        );

        Ok(node)
    }

    /// Establish connections to peers.
    fn connect_peers(
        &mut self,
        sender: &mut ServerRpcSender,
    ) -> Result<(), InitError> {
        self.conns =
            sender.connect_multi(SimplePushClient::connect, &self.peers)?;
        Ok(())
    }

    /// TODO: description.
    fn replicate(
        &self,
        cmd: Command,
        _sender: &Mutex<ServerRpcSender>,
        sm: &StateMachine,
    ) -> Result<CommandResult, SummersetError> {
        // simply push the record to all peers
        // let replies =
        //     sender.send_msg_multi(SimplePushClient::push_record, _, conns)?;
        // for reply in replies {
        //     if !reply.success {
        //         // TODO: use protocol-specific error sub-type.
        //         return;
        //     }
        // }

        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        Ok(sm.execute(&cmd))
    }
}

#[tonic::async_trait]
impl SimplePush for SimplePushServerNode {
    /// Handler of PushRecord request.
    async fn push_record(
        &self,
        request: Request<PushRecordRequest>,
    ) -> Result<Response<PushRecordReply>, Status> {
        let req = request.into_inner();

        // TODO: correct logic

        // compose the reply and return
        let reply = PushRecordReply {
            request_id: req.request_id,
            success: true,
        };
        Ok(Response::new(reply))
    }
}

/// SimplePush replication protocol client stub.
#[derive(Debug)]
pub struct SimplePushClientStub {
    /// List of server nodes addresses.
    #[allow(dead_code)]
    servers: Vec<String>,

    /// Current primary server index.
    #[allow(dead_code)]
    primary_idx: usize,

    /// Connection established to the primary server.
    primary_conn: Option<ExternalApiClient<Channel>>,
}

impl ReplicatorClientStub for SimplePushClientStub {
    /// Create a new SimplePush protocol client stub.
    fn new(servers: Vec<String>) -> Result<Self, InitError> {
        if servers.is_empty() {
            Err(InitError("servers list is empty".into()))
        } else {
            // randomly pick a server as primary and setup connection
            let primary_idx = rand::thread_rng().gen_range(0..servers.len());

            Ok(SimplePushClientStub {
                servers,
                primary_idx,
                primary_conn: None,
            })
        }
    }

    /// Establish connection(s) to server(s).
    fn connect_servers(
        &mut self,
        sender: &mut ClientRpcSender,
    ) -> Result<(), InitError> {
        // connect to chosen primary server
        self.primary_conn =
            Some(sender.connect(&self.servers[self.primary_idx])?);
        Ok(())
    }

    /// Complete the given command by sending it to the currently connected
    /// server and trust the result unconditionally.
    fn complete(
        &mut self,
        cmd: Command,
        sender: &mut ClientRpcSender,
    ) -> Result<CommandResult, SummersetError> {
        sender.issue(self.primary_conn.as_mut().unwrap(), cmd)
    }
}
