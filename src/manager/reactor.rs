//! Cluster manager client-facing reactor module implementation.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use crate::utils::{
    SummersetError, safe_tcp_read, safe_tcp_write, tcp_bind_with_retry,
};
use crate::server::ReplicaId;
use crate::client::ClientId;

use bytes::BytesMut;

use serde::{Serialize, Deserialize};

use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Control event request from client.
// TODO: maybe add things like leader info, etc.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CtrlRequest {
    /// Query the set of active servers and their info.
    QueryInfo,

    /// Reset the specified server(s) to initial state.
    ResetServer {
        /// ID of server to reset. If `None`, resets all active servers.
        server: Option<ReplicaId>,
        /// If false, cleans durable storage state as well.
        durable: bool,
    },

    /// Client leave notification.
    Leave,
}

/// Control event reply to client.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CtrlReply {
    /// Reply to server info query.
    QueryInfo {
        servers: HashMap<ReplicaId, SocketAddr>,
    },

    /// Reply to server reset request.
    ResetServer { servers: HashSet<ReplicaId> },

    /// Reply to client leave notification.
    Leave,
}

/// The client-facing reactor API module.
pub struct ClientReactor {
    /// Receiver side of the req channel.
    rx_req: mpsc::UnboundedReceiver<(ClientId, CtrlRequest)>,

    /// Map from client ID -> sender side of the reply channel, shared with
    /// the client acceptor thread.
    tx_replies:
        flashmap::ReadHandle<ClientId, mpsc::UnboundedSender<CtrlReply>>,

    /// Join handle of the client acceptor thread.
    _client_acceptor_handle: JoinHandle<()>,

    /// Map from client ID -> client responder thread join handles, shared
    /// with the client acceptor thread.
    _client_responder_handles: flashmap::ReadHandle<ClientId, JoinHandle<()>>,
}

// ClientReactor public API implementation
impl ClientReactor {
    /// Creates a new client-facing responder module and spawns the client
    /// acceptor thread. Creates a req channel for buffering incoming control
    /// requests.
    pub async fn new_and_setup(
        cli_addr: SocketAddr,
    ) -> Result<Self, SummersetError> {
        let (tx_req, rx_req) = mpsc::unbounded_channel();

        let (tx_replies_write, tx_replies_read) =
            flashmap::new::<ClientId, mpsc::UnboundedSender<CtrlReply>>();

        let (client_responder_handles_write, client_responder_handles_read) =
            flashmap::new::<ClientId, JoinHandle<()>>();

        let client_listener = tcp_bind_with_retry(cli_addr, 10).await?;
        let client_acceptor_handle =
            tokio::spawn(Self::client_acceptor_thread(
                tx_req,
                client_listener,
                tx_replies_write,
                client_responder_handles_write,
            ));

        Ok(ClientReactor {
            rx_req,
            tx_replies: tx_replies_read,
            _client_acceptor_handle: client_acceptor_handle,
            _client_responder_handles: client_responder_handles_read,
        })
    }

    /// Returns whether a client ID is connected to me.
    #[allow(dead_code)]
    pub fn has_client(&self, client: ClientId) -> bool {
        let tx_replies_guard = self.tx_replies.guard();
        tx_replies_guard.contains_key(&client)
    }

    /// Waits for the next control event request from some client.
    pub async fn recv_req(
        &mut self,
    ) -> Result<(ClientId, CtrlRequest), SummersetError> {
        match self.rx_req.recv().await {
            Some((id, req)) => Ok((id, req)),
            None => logged_err!("m"; "req channel has been closed"),
        }
    }

    /// Sends a control event reply to specified client.
    pub fn send_reply(
        &mut self,
        reply: CtrlReply,
        client: ClientId,
    ) -> Result<(), SummersetError> {
        let tx_replies_guard = self.tx_replies.guard();
        match tx_replies_guard.get(&client) {
            Some(tx_reply) => {
                tx_reply
                    .send(reply)
                    .map_err(|e| SummersetError(e.to_string()))?;
                Ok(())
            }
            None => {
                logged_err!(
                    "m";
                    "client ID {} not found among active clients",
                    client
                )
            }
        }
    }
}

// ClientReactor client_acceptor thread implementation
impl ClientReactor {
    /// Accepts a new client connection.
    async fn accept_new_client(
        mut stream: TcpStream,
        addr: SocketAddr,
        id: ClientId,
        tx_req: mpsc::UnboundedSender<(ClientId, CtrlRequest)>,
        tx_replies: &mut flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<CtrlReply>,
        >,
        client_responder_handles: &mut flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
        tx_exit: mpsc::UnboundedSender<ClientId>,
    ) -> Result<(), SummersetError> {
        // send ID assignment
        if let Err(e) = stream.write_u64(id).await {
            return logged_err!("m"; "error assigning new client ID: {}", e);
        }

        let mut tx_replies_guard = tx_replies.guard();
        if let Some(sender) = tx_replies_guard.get(&id) {
            if sender.is_closed() {
                // if this client ID has left before, garbage collect it now
                let mut client_responder_handles_guard =
                    client_responder_handles.guard();
                client_responder_handles_guard.remove(id);
                tx_replies_guard.remove(id);
            } else {
                return logged_err!("m"; "duplicate client ID listened: {}", id);
            }
        }
        pf_info!("m"; "accepted new client {}", id);

        let (tx_reply, rx_reply) = mpsc::unbounded_channel();
        tx_replies_guard.insert(id, tx_reply);

        let client_responder_handle =
            tokio::spawn(Self::client_responder_thread(
                id, addr, stream, tx_req, rx_reply, tx_exit,
            ));
        let mut client_responder_handles_guard =
            client_responder_handles.guard();
        client_responder_handles_guard.insert(id, client_responder_handle);

        client_responder_handles_guard.publish();
        tx_replies_guard.publish();
        Ok(())
    }

    /// Removes handles of a left client connection.
    fn remove_left_client(
        id: ClientId,
        tx_replies: &mut flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<CtrlReply>,
        >,
        client_responder_handles: &mut flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
    ) -> Result<(), SummersetError> {
        let mut tx_replies_guard = tx_replies.guard();
        if !tx_replies_guard.contains_key(&id) {
            return logged_err!("m"; "client {} not found among active ones", id);
        }
        tx_replies_guard.remove(id);

        let mut client_responder_handles_guard =
            client_responder_handles.guard();
        client_responder_handles_guard.remove(id);

        Ok(())
    }

    /// Client acceptor thread function.
    async fn client_acceptor_thread(
        tx_req: mpsc::UnboundedSender<(ClientId, CtrlRequest)>,
        client_listener: TcpListener,
        mut tx_replies: flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<CtrlReply>,
        >,
        mut client_responder_handles: flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
    ) {
        pf_debug!("m"; "client_acceptor thread spawned");

        let local_addr = client_listener.local_addr().unwrap();
        pf_info!("m"; "accepting clients on '{}'", local_addr);

        // maintain a monotonically increasing client ID for new clients
        // start with a relatively high value to avoid confusion with
        // server replica IDs
        let mut next_client_id: ClientId = 2857;

        // create an exit mpsc channel for getting notified about termination
        // of client responder threads
        let (tx_exit, mut rx_exit) = mpsc::unbounded_channel();

        loop {
            tokio::select! {
                // new client connection
                accepted = client_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!("m"; "error accepting client connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = Self::accept_new_client(
                        stream,
                        addr,
                        next_client_id,
                        tx_req.clone(),
                        &mut tx_replies,
                        &mut client_responder_handles,
                        tx_exit.clone()
                    ).await {
                        pf_error!("m"; "error accepting new client: {}", e);
                    } else {
                        next_client_id += 1;
                    }
                },

                // a client responder thread exits
                id = rx_exit.recv() => {
                    let id = id.unwrap();
                    if let Err(e) = Self::remove_left_client(
                        id,
                        &mut tx_replies,
                        &mut client_responder_handles
                    ) {
                        pf_error!("m"; "error removing left client {}: {}", id, e);
                    }
                },
            }
        }

        // pf_debug!("m"; "client_acceptor thread exitted");
    }
}

// ClientReactor client_responder thread implementation
impl ClientReactor {
    /// Reads a client control request from given TcpStream.
    async fn read_req(
        // first 8 btyes being the request length, and the rest bytes being the
        // request itself
        req_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<CtrlRequest, SummersetError> {
        safe_tcp_read(req_buf, conn_read).await
    }

    /// Writes a control event reply through given TcpStream.
    fn write_reply(
        reply_buf: &mut BytesMut,
        reply_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        reply: Option<&CtrlReply>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(reply_buf, reply_buf_cursor, conn_write, reply)
    }

    /// Client control request listener and reply sender thread function.
    async fn client_responder_thread(
        id: ClientId,
        addr: SocketAddr,
        conn: TcpStream,
        tx_req: mpsc::UnboundedSender<(ClientId, CtrlRequest)>,
        mut rx_reply: mpsc::UnboundedReceiver<CtrlReply>,
        tx_exit: mpsc::UnboundedSender<ClientId>,
    ) {
        pf_debug!("m"; "client_responder thread for {} ({}) spawned", id, addr);

        let (mut conn_read, conn_write) = conn.into_split();
        let mut req_buf = BytesMut::with_capacity(8 + 1024);
        let mut reply_buf = BytesMut::with_capacity(8 + 1024);
        let mut reply_buf_cursor = 0;

        let mut retrying = false;
        loop {
            tokio::select! {
                // gets a reply to send to client
                reply = rx_reply.recv(), if !retrying => {
                    match reply {
                        Some(reply) => {
                            match Self::write_reply(
                                &mut reply_buf,
                                &mut reply_buf_cursor,
                                &conn_write,
                                Some(&reply)
                            ) {
                                Ok(true) => {
                                    // pf_trace!("m"; "sent -> {} reply {:?}", id, reply);
                                }
                                Ok(false) => {
                                    pf_debug!("m"; "should start retrying reply send -> {}", id);
                                    retrying = true;
                                }
                                Err(e) => {
                                    pf_error!("m"; "error sending -> {}: {}", id, e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // receives control request from client
                req = Self::read_req(&mut req_buf, &mut conn_read) => {
                    match req {
                        Ok(CtrlRequest::Leave) => {
                            // client leaving, send dummy reply and break
                            let reply = CtrlReply::Leave;
                            if let Err(e) = Self::write_reply(
                                &mut reply_buf,
                                &mut reply_buf_cursor,
                                &conn_write,
                                Some(&reply)
                            ) {
                                pf_error!("m"; "error replying -> {}: {}", id, e);
                            } else { // skips `WouldBlock` failure check here
                                pf_info!("m"; "client {} has left", id);
                            }
                            break;
                        },

                        Ok(req) => {
                            // pf_trace!("m"; "recv <- {} req {:?}", id, req);
                            if let Err(e) = tx_req.send((id, req)) {
                                pf_error!("m"; "error sending to tx_req for {}: {}", id, e);
                            }
                        },

                        Err(e) => {
                            pf_error!("m"; "error reading req <- {}: {}", id, e);
                            break; // probably the client exitted without `leave()`
                        }
                    }
                },

                // retrying last unsuccessful reply send
                _ = conn_write.writable(), if retrying => {
                    match Self::write_reply(
                        &mut reply_buf,
                        &mut reply_buf_cursor,
                        &conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!("m"; "finished retrying last reply send -> {}", id);
                            retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!("m"; "still should retry last reply send -> {}", id);
                        }
                        Err(e) => {
                            pf_error!("m"; "error retrying last reply send -> {}: {}", id, e);
                        }
                    }
                }
            }
        }

        if let Err(e) = tx_exit.send(id) {
            pf_error!("m"; "error sending exit signal for {}: {}", id, e);
        }
        pf_debug!("m"; "client_responder thread for {} ({}) exitted", id, addr);
    }
}

#[cfg(test)]
mod reactor_tests {
    use super::*;
    use std::sync::Arc;
    use crate::client::ClientCtrlStub;
    use tokio::sync::Barrier;
    use tokio::time::{self, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_req_reply() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // manager-side
            let mut reactor =
                ClientReactor::new_and_setup("127.0.0.1:53601".parse()?)
                    .await?;
            barrier2.wait().await;
            let (client, req) = reactor.recv_req().await?;
            assert!(reactor.has_client(client));
            assert_eq!(req, CtrlRequest::QueryInfo);
            reactor.send_reply(
                CtrlReply::QueryInfo {
                    servers: HashMap::<ReplicaId, SocketAddr>::from([
                        (0, "127.0.0.1:53700".parse()?),
                        (1, "127.0.0.1:53701".parse()?),
                    ]),
                },
                client,
            )?;
            Ok::<(), SummersetError>(())
        });
        // client-side
        barrier.wait().await;
        let mut ctrl_stub =
            ClientCtrlStub::new_by_connect("127.0.0.1:53601".parse()?).await?;
        ctrl_stub.send_req(Some(&CtrlRequest::QueryInfo))?;
        assert_eq!(
            ctrl_stub.recv_reply().await?,
            CtrlReply::QueryInfo {
                servers: HashMap::<ReplicaId, SocketAddr>::from([
                    (0, "127.0.0.1:53700".parse()?),
                    (1, "127.0.0.1:53701".parse()?),
                ]),
            }
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_client_leave() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // client-side
            barrier2.wait().await;
            let mut ctrl_stub =
                ClientCtrlStub::new_by_connect("127.0.0.1:54601".parse()?)
                    .await?;
            ctrl_stub.send_req(Some(&CtrlRequest::QueryInfo))?;
            assert_eq!(
                ctrl_stub.recv_reply().await?,
                CtrlReply::QueryInfo {
                    servers: HashMap::<ReplicaId, SocketAddr>::from([
                        (0, "127.0.0.1:54700".parse()?),
                        (1, "127.0.0.1:54701".parse()?),
                    ]),
                }
            );
            ctrl_stub.send_req(Some(&CtrlRequest::Leave))?;
            assert_eq!(ctrl_stub.recv_reply().await?, CtrlReply::Leave);
            ctrl_stub.forget();
            time::sleep(Duration::from_millis(10)).await;
            let mut ctrl_stub =
                ClientCtrlStub::new_by_connect("127.0.0.1:54601".parse()?)
                    .await?;
            ctrl_stub.send_req(Some(&CtrlRequest::QueryInfo))?;
            assert_eq!(
                ctrl_stub.recv_reply().await?,
                CtrlReply::QueryInfo {
                    servers: HashMap::<ReplicaId, SocketAddr>::from([
                        (0, "127.0.0.1:54710".parse()?),
                        (1, "127.0.0.1:54711".parse()?),
                    ]),
                }
            );
            Ok::<(), SummersetError>(())
        });
        // manager-side
        let mut reactor =
            ClientReactor::new_and_setup("127.0.0.1:54601".parse()?).await?;
        barrier.wait().await;
        let (client, req) = reactor.recv_req().await?;
        assert!(reactor.has_client(client));
        assert_eq!(req, CtrlRequest::QueryInfo);
        reactor.send_reply(
            CtrlReply::QueryInfo {
                servers: HashMap::<ReplicaId, SocketAddr>::from([
                    (0, "127.0.0.1:54700".parse()?),
                    (1, "127.0.0.1:54701".parse()?),
                ]),
            },
            client,
        )?;
        let (client2, req2) = reactor.recv_req().await?;
        assert!(reactor.has_client(client2));
        assert!(!reactor.has_client(client));
        assert_eq!(req2, CtrlRequest::QueryInfo);
        reactor.send_reply(
            CtrlReply::QueryInfo {
                servers: HashMap::<ReplicaId, SocketAddr>::from([
                    (0, "127.0.0.1:54710".parse()?),
                    (1, "127.0.0.1:54711".parse()?),
                ]),
            },
            client2,
        )?;
        Ok(())
    }
}
