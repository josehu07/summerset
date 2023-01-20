//! Replication protocol: simple push.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::external_api_proto::external_api_client::ExternalApiClient;

mod simple_push_proto {
    tonic::include_proto!("simple_push");
}
use simple_push_proto::{PushRecordRequest, PushRecordReply};
use simple_push_proto::simple_push_client::SimplePushClient;
use simple_push_proto::simple_push_server::{SimplePush, SimplePushServer};

use crate::protocols::SMRProtocol;
use crate::smr_server::{SummersetServerNode, ServerRpcSender};
use crate::smr_client::ClientRpcSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{
    ReplicatorServerNode, ReplicatorCommService, ReplicatorClientStub,
};
use crate::utils::{SummersetError, InitError};

use tonic::{Request, Response, Status};
use tonic::transport;

use rand::Rng;

/// SimplePush replication protocol server module. TODO: description.
#[derive(Debug, Default)]
pub struct SimplePushServerNode {
    /// List of peer nodes addresses.
    peers: Vec<String>,

    /// Map from peer index to RPC connection client.
    conns: Mutex<HashMap<usize, SimplePushClient<transport::Channel>>>,
}

impl ReplicatorServerNode for SimplePushServerNode {
    /// Create a new SimplePush protocol server module.
    fn new(peers: Vec<String>) -> Result<Self, InitError> {
        Ok(SimplePushServerNode {
            peers, // save peers list
            conns: Mutex::new(HashMap::new()),
        })
    }

    /// Establish connections to peers.
    fn connect_peers(&self, sender: &ServerRpcSender) -> Result<(), InitError> {
        let conns =
            sender.connect_multi(SimplePushClient::connect, &self.peers)?;

        // takes lock on `self.conns` map
        *self.conns.lock().unwrap() = conns;

        Ok(())
    }

    /// TODO: description.
    fn replicate(
        &self,
        cmd: Command,
        _sender: &ServerRpcSender,
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

/// SimplePush replication protocol internal communication service struct.
#[derive(Debug)]
pub struct SimplePushCommService {
    node: Arc<SummersetServerNode>,
}

#[tonic::async_trait]
impl SimplePush for SimplePushCommService {
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

impl ReplicatorCommService for SimplePushCommService {
    /// Create a new internal communication service struct.
    fn new(node: Arc<SummersetServerNode>) -> Result<Self, InitError> {
        if node.protocol != SMRProtocol::SimplePush {
            Err(InitError(
                "cannot create new SimplePushCommService: ".to_string()
                    + &format!("wrong node.protocol {}", node.protocol),
            ))
        } else {
            Ok(SimplePushCommService { node })
        }
    }

    /// Convert the holder struct into a tonic service that owns the struct,
    /// and build a new tonic Router for the service.
    fn build_tonic_router(
        self: Box<Self>,
    ) -> Option<transport::server::Router> {
        Some(
            transport::Server::builder()
                .add_service(SimplePushServer::new(*self)),
        )
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
    primary_conn: Option<ExternalApiClient<transport::Channel>>,
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
        sender: &ClientRpcSender,
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
        sender: &ClientRpcSender,
    ) -> Result<CommandResult, SummersetError> {
        sender.issue(self.primary_conn.as_mut().unwrap(), cmd)
    }
}
