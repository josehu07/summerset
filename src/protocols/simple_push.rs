//! Replication protocol: simple push.

use tonic::transport::Channel;
use crate::external_api_proto::external_api_client::ExternalApiClient;

mod simple_push_proto {
    tonic::include_proto!("simple_push");
}
use simple_push_proto::simple_push_client::SimplePushClient;

use crate::smr_server::ServerRpcSender;
use crate::smr_client::ClientRpcSender;
use crate::statemach::{Command, CommandResult, StateMachine};
use crate::replicator::{ReplicatorServerNode, ReplicatorClientStub};
use crate::utils::{SummersetError, InitError};

use std::collections::HashMap;
use std::sync::Mutex;
use rand::Rng;

/// SimplePush replication protocol server module. TODO: description.
#[derive(Debug, Default)]
pub struct SimplePushServerNode {
    /// List of peer nodes addresses.
    peers: Vec<String>,

    /// Map from peer index to RPC connection client.
    conns: HashMap<usize, SimplePushClient<Channel>>,
}

impl ReplicatorServerNode for SimplePushServerNode {
    /// Create a new SimplePush protocol server module.
    fn new(
        peers: &Vec<String>,
        sender: &mut ServerRpcSender,
    ) -> Result<Self, InitError> {
        // establish connection to all peers
        let conns = sender.connect_all(SimplePushClient::connect, peers)?;

        Ok(SimplePushServerNode {
            peers: peers.clone(),
            conns,
        })
    }

    /// TODO: description.
    fn replicate(
        &self,
        cmd: Command,
        sender: &Mutex<ServerRpcSender>,
        sm: &StateMachine,
    ) -> Result<CommandResult, SummersetError> {
        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        Ok(sm.execute(&cmd))
    }
}

/// SimplePush replication protocol client stub.
#[derive(Debug)]
pub struct SimplePushClientStub {
    /// List of server nodes addresses.
    #[allow(dead_code)]
    servers: Vec<String>,

    /// Current primary server index.
    #[allow(dead_code)]
    primary_idx: usize,

    /// Connection established to the primary server.
    primary_conn: ExternalApiClient<Channel>,
}

impl ReplicatorClientStub for SimplePushClientStub {
    /// Create a new SimplePush protocol client stub.
    fn new(
        servers: &Vec<String>,
        sender: &mut ClientRpcSender,
    ) -> Result<Self, InitError> {
        if servers.is_empty() {
            Err(InitError("servers list is empty".into()))
        } else {
            // randomly pick a server and setup connection
            let primary_idx = rand::thread_rng().gen_range(0..servers.len());
            let primary_conn = sender.connect(&servers[primary_idx])?;

            Ok(SimplePushClientStub {
                servers: servers.clone(),
                primary_idx,
                primary_conn,
            })
        }
    }

    /// Complete the given command by sending it to the currently connected
    /// server and trust the result unconditionally. If haven't established
    /// any connection, randomly pick a server and connect to it now.
    fn complete(
        &mut self,
        cmd: Command,
        sender: &mut ClientRpcSender,
    ) -> Result<CommandResult, SummersetError> {
        sender.issue(&mut self.primary_conn, cmd)
    }
}
