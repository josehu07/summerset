//! Summerset server side core structures.

use std::collections::HashMap;
use std::marker::{Send, Copy};
use std::sync::Arc;
use std::future::Future;

use crate::external_api_proto::{DoCommandRequest, DoCommandReply};
use crate::external_api_proto::external_api_server::{
    ExternalApi, ExternalApiServer,
};

use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{ReplicatorServerNode, ReplicatorCommService};
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

use tonic::{Request, Response, Status};
use tonic::transport;

use tokio::runtime::{Runtime, Builder};
use tokio::task::JoinSet;

/// Server node struct, consisting of a replicator module and a state machine.
/// The replicator module type is specified at new(). The state machine is
/// currently a simple in-memory key-value HashMap. Anything required to be
/// persistently durable should be made so inside the replicator module. The
/// state machine HashMap itself is volatile.
#[derive(Debug)]
pub struct SummersetServerNode {
    /// The SMR protocol in use.
    pub protocol: SMRProtocol,

    /// Server internal RPC sender state.
    rpc_sender: ServerRpcSender,

    /// Replicator module running a replication protocol. Not using generic
    /// here because the type of protocol to be used is known only at runtime
    /// invocation.
    replicator: Box<dyn ReplicatorServerNode>,

    /// State machine, which is a simple in-memory HashMap.
    kvlocal: StateMachine,
}

impl SummersetServerNode {
    /// Create a new server node running given replication protocol type.
    pub fn new(
        protocol: SMRProtocol,
        peers: Vec<String>,
    ) -> Result<Self, InitError> {
        let rpc_sender = ServerRpcSender::new()?;

        // return InitError if the peers list does not meet the replication
        // protocol's requirement
        let replicator = protocol.new_server_node(peers)?;

        Ok(SummersetServerNode {
            protocol,
            rpc_sender,
            replicator,
            kvlocal: StateMachine::new(),
        })
    }

    /// Establish connections to peers.
    pub fn connect_peers(&self) -> Result<(), InitError> {
        self.replicator.connect_peers(&self.rpc_sender)?;
        Ok(())
    }

    /// Handle a command received from client.
    ///
    /// This method takes a shared reference to self and is safe for multiple
    /// threads to call at the same time. Conflicts should be resolved within
    /// the implementation of components.
    pub fn handle(
        &self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator
            .replicate(cmd, &self.rpc_sender, &self.kvlocal)
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
        let result = self.node.handle(cmd).map_err(|e| {
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

/// Server internal RPC sender state and helper functions.
#[derive(Debug)]
pub struct ServerRpcSender {
    /// Tokio multi-threaded runtime, created explicitly.
    mt_runtime: Runtime,

    /// Tokio current-thread runtime, created explicitly.
    ct_runtime: Runtime,
}

impl ServerRpcSender {
    /// Create a new server internal RPC sender struct with explicit tokio
    /// multi-threaded runtime. Returns `Err(InitError)` if failed to build
    /// the runtime.
    fn new() -> Result<Self, InitError> {
        let mt_runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                InitError(format!("failed to build tokio mt_runtime: {}", e))
            })?;
        let ct_runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                InitError(format!("failed to build tokio ct_runtime: {}", e))
            })?;

        Ok(ServerRpcSender {
            mt_runtime,
            ct_runtime,
        })
    }

    /// Connect to one address and block until acknowledgement.
    /// This function is provided as a generic function due to the fact that
    /// each tonic protobuf client is a different type generated by prost.
    /// Returns `Ok(C)` containing the protobuf RPC client on success.
    pub fn connect<Conn, Fut>(
        &self,
        connect_fn: impl FnOnce(String) -> Fut,
        peer_addr: &String,
    ) -> Result<Conn, SummersetError>
    where
        Fut: Future<Output = Result<Conn, tonic::transport::Error>>,
    {
        self.mt_runtime
            .block_on(connect_fn(format!("http://{}", peer_addr)))
            .map_err(|e| {
                SummersetError::ServerConnError(format!(
                    "failed to connect to peer address {}: {}",
                    peer_addr, e
                ))
            })
    }

    /// Connect to multiple addresses and block until all of the connections
    /// have been established. On success, returns `Ok(HashMap<idx, C>)`
    /// containing a map from peer index to protobuf RPC client structs.
    /// Returns error immediately if any of them fails.
    pub fn connect_multi<Conn, Fut>(
        &self,
        connect_fn: impl FnOnce(String) -> Fut + Send + Copy + 'static,
        peer_addrs: &Vec<String>,
    ) -> Result<HashMap<usize, Conn>, SummersetError>
    where
        Conn: Send + 'static,
        Fut: Future<Output = Result<Conn, tonic::transport::Error>> + Send,
    {
        if peer_addrs.len() == 0 {
            return Ok(HashMap::new());
        }

        // spawn all futures concurrently, prefixing the results with index
        let mut jset = JoinSet::new();
        for (idx, addr) in peer_addrs.iter().enumerate() {
            let addr_str = format!("http://{}", addr);
            jset.spawn_on(
                async move { (idx, connect_fn(addr_str).await) },
                self.mt_runtime.handle(),
            );
        }

        // join them in order of completion
        let mut conns = HashMap::with_capacity(peer_addrs.len());
        while let Some(jres) = self.ct_runtime.block_on(jset.join_next()) {
            if let Err(je) = jres {
                return Err(SummersetError::ServerConnError(format!(
                    "join error in connect_all: {}",
                    je
                )));
            }

            // `idx` is the index of corresponding peer in passed-in
            // vector `peer_addrs`
            // `tres` is a `Result<C, tonic::transport::Error>`
            let (idx, tres) = jres.unwrap();
            if let Err(te) = tres {
                return Err(SummersetError::ServerConnError(format!(
                    "failed to connect to peer address {}: {}",
                    &peer_addrs[idx], te
                )));
            }
            conns.insert(idx, tres.unwrap());
        }

        assert!(conns.len() == peer_addrs.len());
        Ok(conns)
    }

    /// Send RPC request to one connection and block until acknowledgement.
    /// This function is provided as a generic function due to the fact that
    /// each tonic protobuf client and request/reply is a different type
    /// generated by prost. Returns `Ok(Reply)` containing the RPC reply on
    /// success.
    pub fn send_rpc<Conn, Request, Reply, Fut>(
        &self,
        _rpc_fn: impl FnOnce(Request) -> Fut,
        _request: Request,
        _peer_conns: &Conn,
    ) -> Result<Reply, SummersetError>
    where
        Fut: Future<Output = Result<Reply, tonic::transport::Error>>,
    {
        Err(SummersetError::ServerConnError("TODO".into()))
    }

    /// Send RPCs to multiple peer connections and block until all have been
    /// replied. On success, returns `Ok(HashMap<idx, Reply>)` containing a
    /// map from peer index to desired reply structs. Returns error
    /// immediately if any of them fails.
    pub fn send_rpc_multi<Conn, Request, Reply, Fut>(
        &self,
        _rpc_fn: impl FnOnce(Request) -> Fut + Send + Copy,
        _requests: Vec<Request>,
        _peer_conns: &Vec<Conn>,
    ) -> Result<HashMap<usize, Reply>, SummersetError>
    where
        Conn: Send,
        Fut: Future<Output = Result<Reply, tonic::transport::Error>> + Send,
    {
        Err(SummersetError::ServerConnError("TODO".into()))
    }
}

#[cfg(test)]
mod smr_server_tests {
    use super::{ServerRpcSender, SummersetServerNode, SMRProtocol};

    #[test]
    fn new_rpc_sender() {
        let sender = ServerRpcSender::new();
        assert!(sender.is_ok());
    }

    #[test]
    fn new_server_node() {
        let node = SummersetServerNode::new(
            SMRProtocol::DoNothing,
            vec!["hostB:50078".into(), "hostC:50078".into()],
        );
        assert!(node.is_ok());
    }
}
