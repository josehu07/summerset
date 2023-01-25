//! Replication protocol: do nothing.
//!
//! Immediately executes given command on the state machine upon receiving a
//! client command, and not doing anything else. There are no inter-server
//! communication channels.

use std::sync::{Arc, Mutex};

use crate::external_api_proto::external_api_client::ExternalApiClient;

use crate::{connect_fn, rpc_fn};
use crate::protocols::SMRProtocol;
use crate::smr_server::SummersetServerNode;
use crate::smr_client::SummersetClientStub;
use crate::statemach::{Command, CommandResult};
use crate::replicator::{
    ReplicatorServerNode, ReplicatorCommService, ReplicatorClientStub,
};
use crate::utils::{SummersetError, InitError};

use tonic::transport;

use rand::Rng;

use atomic_refcell::AtomicRefCell;

/// DoNothing replication protocol server module.
#[derive(Debug, Default)]
pub struct DoNothingServerNode {}

#[tonic::async_trait]
impl ReplicatorServerNode for DoNothingServerNode {
    /// Create a new DoNothing protocol server module.
    fn new(_peers: Vec<String>) -> Result<Self, InitError> {
        Ok(DoNothingServerNode {})
    }

    /// Establish connections to peers.
    async fn connect_peers(
        &self,
        _node: &SummersetServerNode,
    ) -> Result<(), InitError> {
        Ok(())
    }

    /// Do nothing and immediately execute the command on state machine.
    async fn replicate(
        &self,
        cmd: Command,
        node: &SummersetServerNode,
    ) -> Result<CommandResult, SummersetError> {
        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        Ok(node.kvlocal.execute(&cmd))
    }
}

/// DoNothing replication protocol internal communication service struct.
#[derive(Debug)]
pub struct DoNothingCommService {}

impl ReplicatorCommService for DoNothingCommService {
    /// Create a new internal communication service struct.
    fn new(node: Arc<SummersetServerNode>) -> Result<Self, InitError> {
        if node.protocol != SMRProtocol::DoNothing {
            Err(InitError(
                "cannot create new DoNothingCommService: ".to_string()
                    + &format!("wrong node.protocol {}", node.protocol),
            ))
        } else {
            Ok(DoNothingCommService {})
        }
    }

    /// DoNothing protocol does not have internal communication.
    fn build_tonic_router(
        self: Box<Self>,
    ) -> Option<transport::server::Router> {
        None
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
    ///
    /// `Mutex` is required since it may be shared by multiple client threads.
    curr_conn: Mutex<
        Option<Arc<AtomicRefCell<ExternalApiClient<transport::Channel>>>>,
    >,
}

#[tonic::async_trait]
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
                curr_conn: Mutex::new(None),
            })
        }
    }

    /// Establish connection(s) to server(s).
    async fn connect_servers(
        &self,
        stub: &SummersetClientStub,
    ) -> Result<(), InitError> {
        // connect to chosen server
        let conn = stub
            .rpc_sender
            .connect(
                connect_fn!(ExternalApiClient, connect),
                self.servers[self.curr_idx].clone(),
            )
            .await?;

        // take lock on `self.curr_conn` and save the connection struct
        let mut curr_conn_guard = self.curr_conn.lock().unwrap();
        *curr_conn_guard = Some(Arc::new(AtomicRefCell::new(conn)));

        Ok(())
    }

    /// Complete the given command by sending it to the currently connected
    /// server and trust the result unconditionally.
    async fn complete(
        &self,
        cmd: Command,
        stub: &SummersetClientStub,
    ) -> Result<CommandResult, SummersetError> {
        // compose the request struct
        let request = SummersetClientStub::serialize_request(&cmd)?;
        let request_id = request.request_id;

        // take lock on `self.curr_conn` and get the connection struct
        let conn = {
            let curr_conn_guard = self.curr_conn.lock().unwrap();
            curr_conn_guard.as_ref().unwrap().clone()
        };

        // send RPC to connected server
        let reply = stub
            .rpc_sender
            .send_rpc(rpc_fn!(ExternalApiClient, do_command), request, conn)
            .await?;

        // extrace the result struct
        let result =
            SummersetClientStub::deserialize_reply(&reply, request_id)?;
        Ok(result)
    }
}
