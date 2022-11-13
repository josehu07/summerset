//! Summerset server side core structures.

use tonic::{Request, Response, Status};
use crate::external_api_proto::{DoCommandRequest, DoCommandReply};
use crate::external_api_proto::external_api_server::ExternalApi;

use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::ReplicatorServerNode;
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

/// Server node struct, consisting of a replicator module and a state machine.
/// The replicator module type is specified at new(). The state machine is
/// currently a simple in-memory key-value HashMap. Anything required to be
/// persistently durable should be made so inside the replicator module. The
/// state machine HashMap itself is volatile.
#[derive(Debug)]
pub struct SummersetServerNode {
    /// Replicator module running a replication protocol.
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
        // return InitError if the peers list does not meet the replication
        // protocol's requirement
        protocol
            .new_server_node(peers)
            .map(|s| SummersetServerNode {
                replicator: s,
                kvlocal: StateMachine::new(),
            })
    }

    /// Handle a command received from client.
    pub fn handle(
        &self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator.replicate(cmd, &self.kvlocal)
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

#[cfg(test)]
mod smr_server_tests {
    use super::{SummersetServerNode, SMRProtocol};

    #[test]
    fn new_server_node() {
        let node = SummersetServerNode::new(
            SMRProtocol::DoNothing,
            &vec![
                "hostA:50078".into(),
                "hostB:50078".into(),
                "hostC:50078".into(),
            ],
        );
        assert!(node.is_ok());
    }
}
