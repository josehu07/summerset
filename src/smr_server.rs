//! Summerset server side core structures.

use tonic::{Request, Response, Status};
use tonic::transport::Server;
use crate::external_api_proto::{DoCommandRequest, DoCommandReply};
use crate::external_api_proto::external_api_server::{
    ExternalApi, ExternalApiServer,
};

use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::ReplicatorServerNode;
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

use std::net::SocketAddr;
use std::collections::HashMap;
use std::marker::{Send, Copy};
use std::sync::Mutex;
use std::future::Future;
use tokio::runtime::{Runtime, Builder};
use tokio::task::JoinSet;

/// Server node struct, consisting of a replicator module and a state machine.
/// The replicator module type is specified at new(). The state machine is
/// currently a simple in-memory key-value HashMap. Anything required to be
/// persistently durable should be made so inside the replicator module. The
/// state machine HashMap itself is volatile.
#[derive(Debug)]
pub struct SummersetServerNode {
    /// Server internal RPC sender state.
    rpc_sender: Mutex<ServerRpcSender>,

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
        smr_addr: SocketAddr,
        main_runtime: &Runtime, // for starting internal communication service
    ) -> Result<Self, InitError> {
        // initialize internal RPC sender
        let rpc_sender = ServerRpcSender::new()?;

        // return InitError if the peers list does not meet the replication
        // protocol's requirement
        protocol
            .new_server_node(peers, smr_addr, main_runtime)
            .map(|s| SummersetServerNode {
                rpc_sender: Mutex::new(rpc_sender),
                replicator: s,
                kvlocal: StateMachine::new(),
            })
    }

    /// Establish connections to peers and start the client API service.
    pub fn start(
        &mut self,
        api_addr: SocketAddr,
        main_runtime: &Runtime, // for starting internal communication service
    ) -> Result<(), InitError> {
        // connect to peers
        self.replicator
            .connect_peers(&mut self.rpc_sender.lock().unwrap())?;

        // add and start the client key-value API tonic service
        main_runtime.spawn(
            Server::builder()
                .add_service(ExternalApiServer::new(*self))
                .serve(api_addr),
        );

        Ok(())
    }

    /// Handle a command received from client.
    pub fn handle(
        &self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator
            .replicate(cmd, &self.rpc_sender, &self.kvlocal)
    }
}

#[tonic::async_trait]
impl ExternalApi for SummersetServerNode {
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
        let result = self.handle(cmd).map_err(|e| {
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
        &mut self,
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
        &mut self,
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
        &mut self,
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
        &mut self,
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
    use tokio::runtime::Builder;

    #[test]
    fn new_rpc_sender() {
        let sender = ServerRpcSender::new();
        assert!(sender.is_ok());
    }

    #[test]
    fn new_server_node() {
        let main_runtime =
            Builder::new_multi_thread().enable_all().build().unwrap();
        let node = SummersetServerNode::new(
            SMRProtocol::DoNothing,
            vec!["hostB:50078".into(), "hostC:50078".into()],
            "[::1]:50078".parse().unwrap(),
            &main_runtime,
        );
        assert!(node.is_ok());
    }
}
