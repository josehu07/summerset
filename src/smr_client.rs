//! Summerset client side core structures.

use crate::external_api_proto::{DoCommandRequest, DoCommandReply};

use crate::statemach::{Command, CommandResult};
use crate::replicator::ReplicatorClientStub;
use crate::transport::TonicRPCSender;
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

use rand::Rng;

/// Client library struct, consisting of an RPC sender struct and a
/// replicator client-side stub.
#[derive(Debug)]
pub struct SummersetClientStub {
    /// Client request RPC sender state.
    pub rpc_sender: TonicRPCSender,

    /// Replication protocol's client stub. Not using generic here because
    /// the type of protocol to be used is known only at runtime invocation.
    replicator_stub: Box<dyn ReplicatorClientStub>,
}

impl SummersetClientStub {
    /// Create a new client stub of given replication protocol type.
    pub fn new(
        protocol: SMRProtocol,
        servers: Vec<String>,
    ) -> Result<Self, InitError> {
        // initialize RPC request sender
        let rpc_sender = TonicRPCSender::new()?;

        // return InitError if the servers list does not meet the replication
        // protocol's requirement
        protocol
            .new_client_stub(servers)
            .map(|c| SummersetClientStub {
                rpc_sender,
                replicator_stub: c,
            })
    }

    /// Establish connection(s) to server(s).
    pub async fn connect_servers(&self) -> Result<(), InitError> {
        self.replicator_stub.connect_servers(self).await
    }

    /// Complete the given command on the servers cluster.
    pub async fn complete(
        &self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator_stub.complete(cmd, self).await
    }

    /// Compose a `DoCommandRequest` from `Command` by serializing the
    /// struct into JSON.
    pub fn serialize_request(
        cmd: &Command,
    ) -> Result<DoCommandRequest, SummersetError> {
        // serialize Command struct into JSON
        let request_str = serde_json::to_string(cmd).map_err(|e| {
            SummersetError::ClientSerdeError(format!(
                "failed to serialize command {:?}: {}",
                cmd, e
            ))
        })?;

        // compose the request struct
        let request_id: u64 = rand::thread_rng().gen(); // random u64 ID
        Ok(DoCommandRequest {
            request_id,
            command: request_str,
        })
    }

    /// Extract a `CommandResult` from `DoCommandReply` by deserializing the
    /// reply from JSON.
    pub fn deserialize_reply(
        reply: &DoCommandReply,
        request_id: u64,
    ) -> Result<CommandResult, SummersetError> {
        // verify `request_id`
        if reply.request_id != request_id {
            return Err(SummersetError::TonicConnError(format!(
                "mismatch request_id in response: expect {}, found {}",
                request_id, reply.request_id,
            )));
        }

        // deserialize JSON into CommandResult struct
        let result = serde_json::from_str(&reply.result).map_err(|e| {
            SummersetError::ClientSerdeError(format!(
                "failed to deserialize command result string {}: {}",
                &reply.result, e
            ))
        })?;
        Ok(result)
    }
}
