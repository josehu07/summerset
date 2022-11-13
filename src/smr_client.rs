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
    request_sender: ClientRequestSender,

    /// Replication protocol's client stub.
    replicator_stub: Box<dyn ReplicatorClientStub>,
}

impl SummersetClientStub {
    /// Create a new client stub of given replication protocol type.
    pub fn new(
        protocol: SMRProtocol,
        servers: &Vec<String>,
    ) -> Result<Self, InitError> {
        // initialize RPC request sender
        let request_sender = ClientRequestSender::new()?;

        // return InitError if the servers list does not meet the replication
        // protocol's requirement
        protocol
            .new_client_stub(servers)
            .map(|c| SummersetClientStub {
                request_sender,
                replicator_stub: c,
            })
    }

    /// Complete the given command on the servers cluster.
    pub fn complete(
        &mut self,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        self.replicator_stub.complete(cmd, &mut self.request_sender)
    }
}

/// Client request RPC sender state and helper functions.
#[derive(Debug)]
pub struct ClientRequestSender {
    /// Tokio multi-thread runtime, created explicitly.
    runtime: Runtime, // tokio multi-threaded runtime

    /// List of active client-server connections.
    pub connections: Vec<ExternalApiClient<Channel>>,
}

impl ClientRequestSender {
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
        Ok(ClientRequestSender {
            runtime,
            connections: vec![],
        })
    }

    /// Add a new client-server connection to list. Returns `Ok(conn_idx)` on
    /// success.
    pub fn connect(
        &mut self,
        server_addr: &String,
    ) -> Result<usize, SummersetError> {
        let client = self
            .runtime
            .block_on(ExternalApiClient::connect(format!(
                "http://{}",
                server_addr
            )))
            .map_err(|e| {
                SummersetError::ClientConnError(format!(
                    "failed to connect to server address {}: {}",
                    server_addr, e
                ))
            })?;
        self.connections.push(client);
        Ok(self.connections.len() - 1)
    }

    /// Disconnect the connection at given index.
    pub fn disconnect(
        &mut self,
        conn_idx: usize,
    ) -> Result<(), SummersetError> {
        if conn_idx + 1 > self.connections.len() {
            Err(SummersetError::ClientConnError(format!(
                "failed to disconnect: connection index {} out of bound",
                conn_idx
            )))
        } else {
            self.connections.remove(conn_idx);
            Ok(())
        }
    }

    /// Issue a command to the server connection at given index, and block
    /// until its response.
    pub fn issue(
        &mut self,
        conn_idx: usize,
        cmd: Command,
    ) -> Result<CommandResult, SummersetError> {
        if conn_idx + 1 > self.connections.len() {
            return Err(SummersetError::ClientConnError(format!(
                "failed to issue command: connection index {} out of bound",
                conn_idx
            )));
        }

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
            .block_on(self.connections[conn_idx].do_command(request))
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
    use super::{ClientRequestSender, SummersetClientStub, SMRProtocol, InitError};

    #[test]
    fn new_request_sender() {
        let sender = ClientRequestSender::new();
        assert!(sender.is_ok());
        assert!(sender.unwrap().connections.is_empty());
    }

    #[test]
    fn new_client_stub() {
        let stub = SummersetClientStub::new(
            SMRProtocol::DoNothing,
            &vec![
                "hostA:50078".into(),
                "hostB:50078".into(),
                "hostC:50078".into(),
            ],
        );
        assert!(stub.is_ok());
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
