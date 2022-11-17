//! Summerset client side core structures.

use tokio::runtime::{Runtime, Builder};
use tonic::{transport::Channel, Request};
use crate::external_api_proto::DoCommandRequest;
use crate::external_api_proto::external_api_client::ExternalApiClient;

use crate::statemach::{Command, CommandResult};
use crate::replicator::ReplicatorClientStub;
use crate::protocols::SMRProtocol;
use crate::utils::{SummersetError, InitError};

use rand::Rng;

/// Client library struct, consisting of an RPC sender struct and a
/// replicator client-side stub.
#[derive(Debug)]
pub struct SummersetClientStub {
    /// Client request RPC sender state.
    rpc_sender: ClientRpcSender,

    /// Replication protocol's client stub. Not using generic here because
    /// the type of protocol to be used is known only at runtime invocation.
    replicator_stub: Box<dyn ReplicatorClientStub>,
}

impl SummersetClientStub {
    /// Create a new client stub of given replication protocol type.
    pub fn new(
        protocol: SMRProtocol,
        servers: &Vec<String>,
    ) -> Result<Self, InitError> {
        // initialize RPC request sender
        let mut rpc_sender = ClientRpcSender::new()?;

        // return InitError if the servers list does not meet the replication
        // protocol's requirement
        protocol.new_client_stub(servers, &mut rpc_sender).map(|c| {
            SummersetClientStub {
                rpc_sender,
                replicator_stub: c,
            }
        })
    }

    /// Complete the given command on the servers cluster.
    pub fn complete(
        &mut self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator_stub.complete(cmd, &mut self.rpc_sender)
    }
}

/// Client request RPC sender state and helper functions.
#[derive(Debug)]
pub struct ClientRpcSender {
    /// Tokio multi-thread runtime, created explicitly.
    runtime: Runtime,
}

impl ClientRpcSender {
    /// Create a new client RPC sender struct with explicit tokio
    /// multi-threaded runtime. Returns `Err(InitError)` if failed to build
    /// the runtime.
    fn new() -> Result<Self, InitError> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                InitError(format!("failed to build tokio runtime: {}", e))
            })?;
        Ok(ClientRpcSender { runtime })
    }

    /// Add a new client-server connection to list. Returns `Ok(conn_idx)` on
    /// success.
    pub fn connect(
        &mut self,
        server_addr: &String,
    ) -> Result<ExternalApiClient<Channel>, SummersetError> {
        self.runtime
            .block_on(ExternalApiClient::connect(format!(
                "http://{}",
                server_addr
            )))
            .map_err(|e| {
                SummersetError::ClientConnError(format!(
                    "failed to connect to server address {}: {}",
                    server_addr, e
                ))
            })
    }

    /// Issue a command to the server connection at given index, and block
    /// until its response.
    pub fn issue(
        &mut self,
        conn: &mut ExternalApiClient<Channel>,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        // serialize Command struct into JSON
        let request_str = serde_json::to_string(&cmd).map_err(|e| {
            SummersetError::ClientSerdeError(format!(
                "failed to serialize command {:?}: {}",
                &cmd, e
            ))
        })?;

        // compose the request struct
        let request_id: u64 = rand::thread_rng().gen(); // random u64 ID
        let request = Request::new(DoCommandRequest {
            request_id,
            command: request_str,
        });

        // issue the request and block until acknowledgement
        let response = self
            .runtime
            .block_on(conn.do_command(request))
            .map_err(|e| {
                SummersetError::ClientConnError(format!(
                    "failed to issue command {:?}: {}",
                    &cmd, e
                ))
            })?
            .into_inner();
        if response.request_id != request_id {
            return Err(SummersetError::ClientConnError(format!(
                "mismatch request_id in response: expect {}, found {}",
                request_id, response.request_id,
            )));
        }

        // deserialize JSON into CommandResult struct and return
        let result = serde_json::from_str(&response.result).map_err(|e| {
            SummersetError::ClientSerdeError(format!(
                "failed to deserialize command result string {}: {}",
                &response.result, e
            ))
        })?;
        Ok(result)
    }
}

#[cfg(test)]
mod smr_client_tests {
    use super::{ClientRpcSender, SummersetClientStub, SMRProtocol, InitError};

    #[test]
    fn new_rpc_sender() {
        let sender = ClientRpcSender::new();
        assert!(sender.is_ok());
    }

    #[test]
    fn new_client_stub_empty_servers() {
        let stub =
            SummersetClientStub::new(SMRProtocol::DoNothing, &Vec::new());
        assert!(stub.is_err());
        assert_eq!(
            stub.unwrap_err(),
            InitError("servers list is empty".into())
        );
    }
}
