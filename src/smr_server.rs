//! Summerset server side core structures.

use tokio::runtime::{Runtime, Builder};
use tonic::{Request, Response, Status};
use crate::external_api_proto::{DoCommandRequest, DoCommandReply};
use crate::external_api_proto::external_api_server::ExternalApi;

use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::ReplicatorServerNode;
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

use std::sync::Mutex;

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
        peers: &Vec<String>,
    ) -> Result<Self, InitError> {
        // initialize internal RPC sender
        let mut rpc_sender = ServerRpcSender::new()?;

        // return InitError if the peers list does not meet the replication
        // protocol's requirement
        protocol.new_server_node(peers, &mut rpc_sender).map(|s| {
            SummersetServerNode {
                rpc_sender: Mutex::new(rpc_sender),
                replicator: s,
                kvlocal: StateMachine::new(),
            }
        })
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
    /// Handler of client Get request.
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

impl Default for SummersetServerNode {
    fn default() -> Self {
        // default constructor is of protocol type DoNothing
        Self::new(SMRProtocol::DoNothing, &Vec::new()).unwrap()
    }
}

/// Server internal RPC sender state and helper functions.
#[derive(Debug)]
pub struct ServerRpcSender {
    /// Tokio multi-thread runtime, created explicitly.
    runtime: Runtime,
}

impl ServerRpcSender {
    /// Create a new server internal RPC sender struct with explicit tokio
    /// multi-threaded runtime. Returns `Err(InitError)` if failed to build
    /// the runtime.
    fn new() -> Result<Self, InitError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                InitError(format!("failed to build tokio runtime: {}", e))
            })?;
        Ok(ServerRpcSender { runtime })
    }

    /// Get a mutable reference to the tokio runtime. Connections and RPC
    /// issuing logic will be handled in the corresponding file of the
    /// protocol (we do not provide helpers here because each protocol
    /// will have a different RPC client struct type).
    pub fn runtime(&mut self) -> &mut Runtime {
        &mut self.runtime
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
            &vec!["hostB:50078".into(), "hostC:50078".into()],
        );
        assert!(node.is_ok());
    }
}
