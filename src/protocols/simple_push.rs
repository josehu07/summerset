//! Replication protocol: simple push.
//!
//! Upon receiving a client command, simply push the command to all peer
//! servers if it is a Put, wait for all acknowledgments, and then apply it on
//! my state machine locally. There are no guarantees on the ordering
//! consistency of operations. There is no persistent log for durability.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::external_api_proto::external_api_client::ExternalApiClient;

mod simple_push_proto {
    tonic::include_proto!("simple_push");
}
use simple_push_proto::{PushRecordRequest, PushRecordReply};
use simple_push_proto::simple_push_client::SimplePushClient;
use simple_push_proto::simple_push_server::{SimplePush, SimplePushServer};

use crate::{connect_fn, rpc_fn};
use crate::protocols::SMRProtocol;
use crate::smr_server::SummersetServerNode;
use crate::smr_client::SummersetClientStub;
use crate::statemach::{Command, CommandResult};
use crate::replicator::{
    ReplicatorServerNode, ReplicatorCommService, ReplicatorClientStub,
};
use crate::utils::{SummersetError, InitError};

use tonic::{Request, Response, Status};
use tonic::transport;

use rand::Rng;

use atomic_refcell::AtomicRefCell;

use log::debug;

/// SimplePush replication protocol server module.
#[derive(Debug, Default)]
pub struct SimplePushServerNode {
    /// List of peer nodes addresses.
    peers: Vec<String>,

    /// Map from peer ID to RPC connection client.
    ///
    /// `Mutex` is required over the `HashMap` because the node struct itself
    /// is shared by multiple tonic services. In each element, `Arc` is needed
    /// because each connection may need to be moved to a different thread for
    /// concurrent RPC issuing purposes. `AtomicRefCell` is further required
    /// because tonic client RPC methods take a mutable reference to self, and
    /// the standard `RefCell` is not `Sync`.
    conns: Mutex<
        HashMap<u64, Arc<AtomicRefCell<SimplePushClient<transport::Channel>>>>,
    >,
}

#[tonic::async_trait]
impl ReplicatorServerNode for SimplePushServerNode {
    /// Create a new SimplePush protocol server module.
    fn new(peers: Vec<String>) -> Result<Self, InitError> {
        Ok(SimplePushServerNode {
            peers, // save peers list
            conns: Mutex::new(HashMap::new()),
        })
    }

    /// Establish connections to peers.
    async fn connect_peers(
        &self,
        node: &SummersetServerNode,
    ) -> Result<(), InitError> {
        // set up connections to peers concurrently, using peer's index in
        // addresses vector as its ID
        let mut peer_addrs = HashMap::with_capacity(self.peers.len());
        for (idx, addr) in self.peers.iter().enumerate() {
            peer_addrs.insert(idx as u64, addr.clone());
        }
        let peer_conns = node
            .rpc_sender
            .connect_multi(connect_fn!(SimplePushClient, connect), peer_addrs)
            .await?;

        // take lock on `self.conns` map
        let mut conns_guard = self.conns.lock().unwrap();

        // populate `self.conns` map
        conns_guard.reserve(peer_conns.len());
        for (id, conn) in peer_conns.into_iter() {
            conns_guard.insert(id, Arc::new(AtomicRefCell::new(conn)));
        }

        Ok(())
    }

    /// Push the command to all peer servers, wait for them to apply the
    /// command and acknowledge, and then apply it locally.
    async fn replicate(
        &self,
        cmd: Command,
        node: &SummersetServerNode,
    ) -> Result<CommandResult, SummersetError> {
        debug!("client req {:?}", cmd);

        if let Command::Put { ref key, ref value } = cmd {
            // compose the request struct
            let request_id: u64 = rand::thread_rng().gen(); // random u64 ID
            let request = PushRecordRequest {
                request_id,
                key: key.clone(),
                value: value.clone(),
            };

            // takes lock on `self.conns` map and populate the `conns` list
            let mut conns = HashMap::with_capacity(self.peers.len());
            {
                let conns_guard = self.conns.lock().unwrap();
                for (&id, conn) in conns_guard.iter() {
                    conns.insert(id, conn.clone());
                }
            }

            // simply push the record to all peers
            let replies: HashMap<u64, PushRecordReply> = node
                .rpc_sender
                .send_rpc_multi(
                    rpc_fn!(SimplePushClient, push_record),
                    request,
                    conns,
                )
                .await?;

            // fail the request if any peer replied unsuccessful
            for (_, reply) in replies {
                if !reply.success {
                    return Err(SummersetError::ProtocolError(
                        "SimplePush: some peer replied unsuccessful".into(),
                    ));
                }
            }
        }

        debug!("replicated {:?}", cmd);

        // the state machine has thread-safe API, so no need to use any
        // additional locks here
        Ok(node.kvlocal.execute(&cmd))
    }
}

/// SimplePush replication protocol internal communication service struct.
#[derive(Debug)]
pub struct SimplePushCommService {
    node: Arc<SummersetServerNode>,
}

#[tonic::async_trait]
impl SimplePush for SimplePushCommService {
    /// Handler of PushRecord request.
    async fn push_record(
        &self,
        request: Request<PushRecordRequest>,
    ) -> Result<Response<PushRecordReply>, Status> {
        let req = request.into_inner();
        debug!("push_record {} {}", &req.key, &req.value);

        // blindly apply the command locally
        self.node.kvlocal.execute(&Command::Put {
            key: req.key,
            value: req.value,
        });

        // compose the reply and return
        let reply = PushRecordReply {
            request_id: req.request_id,
            success: true,
        };
        Ok(Response::new(reply))
    }
}

impl ReplicatorCommService for SimplePushCommService {
    /// Create a new internal communication service struct.
    fn new(node: Arc<SummersetServerNode>) -> Result<Self, InitError> {
        if node.protocol != SMRProtocol::SimplePush {
            Err(InitError(
                "cannot create new SimplePushCommService: ".to_string()
                    + &format!("wrong node.protocol {}", node.protocol),
            ))
        } else {
            Ok(SimplePushCommService { node })
        }
    }

    /// Convert the holder struct into a tonic service that owns the struct,
    /// and build a new tonic Router for the service.
    fn build_tonic_router(
        self: Box<Self>,
    ) -> Option<transport::server::Router> {
        Some(
            transport::Server::builder()
                .add_service(SimplePushServer::new(*self)),
        )
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
    ///
    /// `Mutex` is required since it may be shared by multiple client threads.
    primary_conn: Mutex<
        Option<Arc<AtomicRefCell<ExternalApiClient<transport::Channel>>>>,
    >,
}

#[tonic::async_trait]
impl ReplicatorClientStub for SimplePushClientStub {
    /// Create a new SimplePush protocol client stub.
    fn new(servers: Vec<String>) -> Result<Self, InitError> {
        if servers.is_empty() {
            Err(InitError("servers list is empty".into()))
        } else {
            // randomly pick a server as primary and setup connection
            let primary_idx = rand::thread_rng().gen_range(0..servers.len());

            Ok(SimplePushClientStub {
                servers,
                primary_idx,
                primary_conn: Mutex::new(None),
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
                self.servers[self.primary_idx].clone(),
            )
            .await?;

        // take lock on `self.primary_conn` and save the connection struct
        let mut primary_conn_guard = self.primary_conn.lock().unwrap();
        *primary_conn_guard = Some(Arc::new(AtomicRefCell::new(conn)));

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

        // take lock on `self.primary_conn` and get the connection struct
        let conn = {
            let primary_conn_guard = self.primary_conn.lock().unwrap();
            primary_conn_guard.as_ref().unwrap().clone()
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
