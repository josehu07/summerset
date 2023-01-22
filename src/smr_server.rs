//! Summerset server side core structures.

use std::collections::HashMap;
use std::marker::Send;
use std::sync::Arc;

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

use futures::future::BoxFuture;

use atomic_refcell::AtomicRefCell;

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
    pub rpc_sender: ServerRpcSender,

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
        let rpc_sender = ServerRpcSender::new()?;

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
    pub fn connect_peers(&self) -> Result<(), InitError> {
        self.replicator.connect_peers(&self)?;
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
        self.replicator.replicate(cmd, &self)
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

/// Helper macro for wrapping a `BoxFuture`-flavor closure over a tonic
/// client async connection method.
/// Usage example: `connect_fn!(SimplePushClient, connect)`.
///
/// The `async move` sub-closure is needed to enforce the returned future to
/// directly move the `Arc<_>` argument instead of capturing a reference to it.
#[macro_export]
macro_rules! connect_fn {
    ($cli:ident, $fn:ident) => {
        |addr| Box::pin(async move { $cli::$fn(addr).await })
    };
}

/// Helper macro for wrapping a `BoxFuture`-flavor closure over a tonic
/// client RPC async issuer method.
/// Usage example: `rpc_fn!(SimplePushClient, push_record)`.
///
/// The `async move` sub-closure is needed to enforce the returned future to
/// directly move the `Arc<_>` argument instead of capturing a reference to it.
#[macro_export]
macro_rules! rpc_fn {
    ($cli:ident, $fn:ident) => {
        |conn, req| {
            Box::pin(
                async move { $cli::$fn(&mut *conn.borrow_mut(), req).await },
            )
        }
    };
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
    ///
    /// Uses `BoxFuture` as a level of indirection to allow passing in
    /// different tonic client connect async functions.
    pub fn connect<Conn, Fut>(
        &self,
        connect_fn: fn(
            String,
        )
            -> BoxFuture<'static, Result<Conn, transport::Error>>,
        peer_addr: &String,
    ) -> Result<Conn, SummersetError> {
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
    /// have been established. On success, returns `Ok(HashMap<u64, Conn>)`
    /// containing a map from peer ID to protobuf RPC client structs. Returns
    /// error immediately if any of them fails.
    ///
    /// Uses `BoxFuture` as a level of indirection to allow passing in
    /// different tonic client connect async functions.
    pub fn connect_multi<Conn>(
        &self,
        connect_fn: fn(
            String,
        )
            -> BoxFuture<'static, Result<Conn, transport::Error>>,
        peer_addrs: HashMap<u64, &String>,
    ) -> Result<HashMap<u64, Conn>, SummersetError>
    where
        Conn: Send + 'static,
    {
        let num_addrs = peer_addrs.len();
        if num_addrs == 0 {
            return Ok(HashMap::new());
        }

        // spawn all futures concurrently, prefixing the results with ID
        let mut jset = JoinSet::new();
        for (&id, addr) in peer_addrs.iter() {
            let addr_str = format!("http://{}", addr);
            jset.spawn_on(
                async move { (id, connect_fn(addr_str).await) },
                self.mt_runtime.handle(),
            );
        }

        // join them in order of completion
        let mut conns = HashMap::with_capacity(num_addrs);
        while let Some(jres) = self.ct_runtime.block_on(jset.join_next()) {
            if let Err(je) = jres {
                return Err(SummersetError::ServerConnError(format!(
                    "join error in connect_multi: {}",
                    je
                )));
            }

            // `tres` is a `Result<Conn, tonic::transport::Error>`
            let (id, tres) = jres.unwrap();
            if let Err(te) = tres {
                return Err(SummersetError::ServerConnError(format!(
                    "failed to connect to peer address {}: {}",
                    &peer_addrs.get(&id).unwrap(),
                    te
                )));
            }
            conns.insert(id, tres.unwrap());
        }

        assert!(conns.len() == num_addrs);
        Ok(conns)
    }

    /// Send RPC request to one connection and block until acknowledgement.
    /// This function is provided as a generic function due to the fact that
    /// each tonic protobuf client and request/reply is a different type
    /// generated by prost. Returns `Ok(Reply)` containing the RPC reply on
    /// success.
    ///
    /// Uses `BoxFuture` as a level of indirection to allow passing in
    /// different tonic client RPC async issuer functions.
    pub fn send_rpc<Conn, Req, Resp>(
        &self,
        rpc_fn: fn(
            Arc<AtomicRefCell<Conn>>,
            Request<Req>,
        )
            -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        request: Req,
        peer_conn: Arc<AtomicRefCell<Conn>>,
    ) -> Result<Resp, SummersetError> {
        self.mt_runtime
            .block_on(rpc_fn(peer_conn, Request::new(request)))
            .map(|r| r.into_inner())
            .map_err(|e| {
                SummersetError::ServerConnError(format!(
                    "failed to send RPC to peer connection: {}",
                    e
                ))
            })
    }

    /// Send RPCs to multiple peer connections and block until all have been
    /// replied. On success, returns `Ok(HashMap<u64, Reply>)` containing a
    /// map from peer ID to desired reply structs. Returns error immediately
    /// if any of them fails.
    pub fn send_rpc_multi<Conn, Req, Resp>(
        &self,
        rpc_fn: fn(
            Arc<AtomicRefCell<Conn>>,
            Request<Req>,
        )
            -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        request: Req,
        peer_conns: HashMap<u64, Arc<AtomicRefCell<Conn>>>,
    ) -> Result<HashMap<u64, Resp>, SummersetError>
    where
        Conn: Send + Sync + 'static,
        Req: Send + Clone + 'static,
        Resp: Send + 'static,
    {
        let num_conns = peer_conns.len();
        if num_conns == 0 {
            return Ok(HashMap::new());
        }

        // spawn all futures concurrently, prefixing the results with ID
        let mut jset = JoinSet::new();
        for (id, conn) in peer_conns.into_iter() {
            let req_copy = request.clone();
            jset.spawn_on(
                async move { (id, rpc_fn(conn, Request::new(req_copy)).await) },
                self.mt_runtime.handle(),
            );
        }

        // join them in order of completion
        let mut replies = HashMap::with_capacity(num_conns);
        while let Some(jres) = self.ct_runtime.block_on(jset.join_next()) {
            if let Err(je) = jres {
                return Err(SummersetError::ServerConnError(format!(
                    "join error in send_rpc_multi: {}",
                    je
                )));
            }

            // `tres` is a `Result<Conn, tonic::Status>`
            let (id, tres) = jres.unwrap();
            if let Err(te) = tres {
                return Err(SummersetError::ServerConnError(format!(
                    "failed to send RPC to peer connection: {}",
                    te
                )));
            }
            replies.insert(id, tres.unwrap().into_inner());
        }

        assert!(replies.len() == num_conns);
        Ok(replies)
    }
}

#[cfg(test)]
mod smr_server_tests {
    use super::{SummersetServerNode, SMRProtocol};

    #[test]
    fn new_server_node() {
        let node = SummersetServerNode::new(
            SMRProtocol::DoNothing,
            vec!["hostB:50078".into(), "hostC:50078".into()],
        );
        assert!(node.is_ok());
    }
}
