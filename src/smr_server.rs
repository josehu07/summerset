//! Summerset server side core structures.

use std::sync::Arc;

use crate::external_api_proto::{DoCommandRequest, DoCommandReply};
use crate::external_api_proto::external_api_server::{
    ExternalApi, ExternalApiServer,
};

use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{ReplicatorServerNode, ReplicatorCommService};
use crate::transport::TonicRPCSender;
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

use tonic::{Request, Response, Status};
use tonic::transport;

/// Server node struct, consisting of a replicator module and a state machine.
/// The replicator module type is specified at new(). The state machine is
/// currently a simple in-memory key-value HashMap. Anything required to be
/// persistently durable should be made so inside the replicator module. The
/// state machine HashMap itself is volatile.
#[derive(Debug)]
pub struct SummersetServerNode {
    /// The SMR protocol in use.
    pub protocol: SMRProtocol,

    /// State machine, which is a simple in-memory HashMap.
    pub kvlocal: StateMachine,

    /// Server internal RPC sender state.
    pub rpc_sender: TonicRPCSender,

    /// Replicator module running a replication protocol. Not using generic
    /// here because the type of protocol to be used is known only at runtime
    /// invocation.
    replicator: Box<dyn ReplicatorServerNode>,
}

impl SummersetServerNode {
    /// Create a new server node running given replication protocol type.
    pub fn new(
        protocol: SMRProtocol,
        peers: Vec<String>,
    ) -> Result<Self, InitError> {
        let rpc_sender = TonicRPCSender::new()?;

        // return InitError if the peers list does not meet the replication
        // protocol's requirement
        let replicator = protocol.new_server_node(peers)?;

        Ok(SummersetServerNode {
            protocol,
            kvlocal: StateMachine::new(),
            rpc_sender,
            replicator,
        })
    }

    /// Establish connections to peers.
    pub async fn connect_peers(&self) -> Result<(), InitError> {
        self.replicator.connect_peers(self).await?;
        Ok(())
    }

    /// Handle a command received from client.
    ///
    /// This method takes a shared reference to self and is safe for multiple
    /// threads to call at the same time. Conflicts should be resolved within
    /// the implementation of components.
    pub async fn handle(
        &self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator.replicate(cmd, self).await
    }
}

/// Holder struct of the client key-value API tonic service.
#[derive(Debug)]
pub struct SummersetApiService {
    /// Arc reference to node.
    node: Arc<SummersetServerNode>,
}

impl SummersetApiService {
    /// Create a new client API service holder struct.
    pub fn new(node: Arc<SummersetServerNode>) -> Result<Self, InitError> {
        Ok(SummersetApiService { node })
    }

    /// Convert the holder struct into a tonic service that owns the struct,
    /// and build a new tonic Router for the service.
    pub fn build_tonic_router(self) -> transport::server::Router {
        transport::Server::builder().add_service(ExternalApiServer::new(self))
    }
}

#[tonic::async_trait]
impl ExternalApi for SummersetApiService {
    /// Handler of client command request.
    async fn do_command(
        &self,
        request: Request<DoCommandRequest>,
    ) -> Result<Response<DoCommandReply>, Status> {
        let req = request.into_inner();

        // deserialize JSON into Command struct
        let cmd = serde_json::from_str(&req.command).map_err(|e| {
            Status::unknown(format!(
                "failed to deserialize command string {}: {}",
                &req.command, e
            ))
        })?;

        // handle command on server node
        let result = self.node.handle(cmd).await.map_err(|e| {
            Status::unknown(format!("error in handling command: {:?}", e))
        })?;

        // serialize CommandResult into JSON
        let result_str = serde_json::to_string(&result).map_err(|e| {
            Status::unknown(format!(
                "failed to serialize command result {:?}: {}",
                &result, e
            ))
        })?;

        // compose the reply and return
        let reply = DoCommandReply {
            request_id: req.request_id,
            result: result_str,
        };
        Ok(Response::new(reply))
    }
}

/// Holder struct of the server internal communication tonic service.
#[derive(Debug)]
pub struct InternalCommService {
    /// Box to the protocol-specific struct.
    replicator_comm: Box<dyn ReplicatorCommService>,
}

impl InternalCommService {
    /// Create a new internal communication service holder struct.
    pub fn new(
        protocol: SMRProtocol,
        node: Arc<SummersetServerNode>,
    ) -> Result<Self, InitError> {
        if protocol != node.protocol {
            return Err(InitError(
                "failed to create InternalCommService: ".to_string()
                    + &format!(
                        "protocol {} mismatches {}",
                        protocol, node.protocol
                    ),
            ));
        }

        // return InitError if the node does not meet the protocol's requirement
        let replicator_comm = protocol.new_comm_service(node)?;

        Ok(InternalCommService { replicator_comm })
    }

    /// Convert the holder struct into a tonic service that owns the struct,
    /// and build a new tonic Router for the service. Returns `None` if the
    /// protocol in use does not have internal communication protos.
    pub fn build_tonic_router(self) -> Option<transport::server::Router> {
        self.replicator_comm.build_tonic_router()
    }
}
