//! Replication protocol: do nothing.

use tonic::transport::Channel;
use crate::external_api_proto::external_api_client::ExternalApiClient;

use crate::smr_server::ServerRpcSender;
use crate::smr_client::ClientRpcSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{ReplicatorServerNode, ReplicatorClientStub};
use crate::utils::{SummersetError, InitError};

use std::net::SocketAddr;
use std::sync::Mutex;
use rand::Rng;
use tokio::runtime::Runtime;

/// DoNothing replication protocol server module. Immediately executes given
/// command on state machine without doing anything else.
#[derive(Debug, Default)]
pub struct DoNothingServerNode {}

impl ReplicatorServerNode for DoNothingServerNode {
    /// Create a new DoNothing protocol server module.
    fn new(
        _peers: Vec<String>,
        _smr_addr: SocketAddr,
        _main_runtime: &Runtime,
    ) -> Result<Self, InitError> {
        Ok(DoNothingServerNode {})
    }

    /// Establish connections to peers.
    fn connect_peers(
        &mut self,
        _sender: &mut ServerRpcSender,
    ) -> Result<(), InitError> {
        Ok(())
    }

    /// Do nothing and immediately execute the command on state machine.
    fn replicate(
        &self,
        cmd: Command,
        _sender: &Mutex<ServerRpcSender>,
        sm: &StateMachine,
    ) -> Result<CommandResult, SummersetError> {
        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        Ok(sm.execute(&cmd))
    }
}

/// DoNothing replication protocol client stub.
#[derive(Debug)]
pub struct DoNothingClientStub {
    /// List of server nodes addresses.
    #[allow(dead_code)]
    servers: Vec<String>,

    /// Currently chosen server index.
    #[allow(dead_code)]
    curr_idx: usize,

    /// Connection established to the chosen server.
    curr_conn: Option<ExternalApiClient<Channel>>,
}

impl ReplicatorClientStub for DoNothingClientStub {
    /// Create a new DoNothing protocol client stub.
    fn new(servers: Vec<String>) -> Result<Self, InitError> {
        if servers.is_empty() {
            Err(InitError("servers list is empty".into()))
        } else {
            // randomly pick a server and setup connection
            let curr_idx = rand::thread_rng().gen_range(0..servers.len());

            Ok(DoNothingClientStub {
                servers,
                curr_idx,
                curr_conn: None,
            })
        }
    }

    /// Establish connection(s) to server(s).
    fn connect_servers(
        &mut self,
        sender: &mut ClientRpcSender,
    ) -> Result<(), InitError> {
        // connect to chosen server
        self.curr_conn = Some(sender.connect(&self.servers[self.curr_idx])?);
        Ok(())
    }

    /// Complete the given command by sending it to the currently connected
    /// server and trust the result unconditionally. If haven't established
    /// any connection, randomly pick a server and connect to it now.
    fn complete(
        &mut self,
        cmd: Command,
        sender: &mut ClientRpcSender,
    ) -> Result<CommandResult, SummersetError> {
        sender.issue(self.curr_conn.as_mut().unwrap(), cmd)
    }
}
