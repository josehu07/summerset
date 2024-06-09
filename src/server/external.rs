//! Summerset server external API module implementation.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::utils::{
    SummersetError, safe_tcp_read, safe_tcp_write, tcp_bind_with_retry,
};
use crate::server::{ReplicaId, Command, CommandResult};
use crate::client::ClientId;

use get_size::GetSize;

use bytes::BytesMut;

use serde::{Serialize, Deserialize};

use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Notify};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};

/// External API request ID type.
pub type RequestId = u64;

/// Request received from client.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub enum ApiRequest {
    /// Regular request.
    Req {
        /// Client request ID.
        id: RequestId,

        /// Command to be replicated and executed.
        cmd: Command,
    },

    /// Client leave notification.
    Leave,
}

impl ApiRequest {
    /// Is the command contained in the request read-only?
    #[inline]
    pub fn read_only(&self) -> bool {
        if let ApiRequest::Req { cmd, .. } = self {
            cmd.read_only()
        } else {
            false
        }
    }
}

/// Reply back to client.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub enum ApiReply {
    /// Reply to regular request.
    Reply {
        /// ID of the corresponding client request.
        id: RequestId,

        /// Command result, or `None` if unsuccessful.
        result: Option<CommandResult>,

        /// Set if the service wants me to talk to a specific server.
        redirect: Option<ReplicaId>,
    },

    /// Reply to client leave notification.
    Leave,
}

/// The external client-facing API module.
pub struct ExternalApi {
    /// My replica ID.
    me: ReplicaId,

    /// Receiver side of the req channel.
    rx_req: mpsc::UnboundedReceiver<(ClientId, ApiRequest)>,

    /// Map from client ID -> sender side of its reply channel, shared with
    /// the client acceptor thread.
    tx_replies: flashmap::ReadHandle<ClientId, mpsc::UnboundedSender<ApiReply>>,

    /// Notify used as batch dumping signal, shared with the batch ticker
    /// thread.
    batch_notify: Arc<Notify>,

    /// Maximum number of requests to return per batch; 0 means no limit.
    max_batch_size: usize,

    /// Join handle of the client acceptor thread.
    _client_acceptor_handle: JoinHandle<()>,

    /// Map from client ID -> client servant thread join handles, shared with
    /// the client acceptor thread.
    _client_servant_handles: flashmap::ReadHandle<ClientId, JoinHandle<()>>,

    /// Join handle of the batch ticker thread.
    _batch_ticker_handle: JoinHandle<()>,
}

// ExternalApi public API implementation
impl ExternalApi {
    /// Creates a new external API module. Spawns the client acceptor thread
    /// and the batch ticker thread. Creates a req channel for buffering
    /// incoming client requests.
    pub(crate) async fn new_and_setup(
        me: ReplicaId,
        api_addr: SocketAddr,
        batch_interval: Duration,
        max_batch_size: usize,
    ) -> Result<Self, SummersetError> {
        if batch_interval < Duration::from_micros(1) {
            return logged_err!(
                me;
                "batch_interval {} us too small",
                batch_interval.as_micros()
            );
        }

        let (tx_req, rx_req) = mpsc::unbounded_channel();

        let (tx_replies_write, tx_replies_read) =
            flashmap::new::<ClientId, mpsc::UnboundedSender<ApiReply>>();

        let (client_servant_handles_write, client_servant_handles_read) =
            flashmap::new::<ClientId, JoinHandle<()>>();

        let client_listener = tcp_bind_with_retry(api_addr, 10).await?;
        let client_acceptor_handle =
            tokio::spawn(Self::client_acceptor_thread(
                me,
                tx_req,
                client_listener,
                tx_replies_write,
                client_servant_handles_write,
            ));

        let batch_notify = Arc::new(Notify::new());
        let batch_ticker_handle = tokio::spawn(Self::batch_ticker_thread(
            me,
            batch_interval,
            batch_notify.clone(),
        ));

        Ok(ExternalApi {
            me,
            rx_req,
            tx_replies: tx_replies_read,
            batch_notify,
            max_batch_size,
            _client_acceptor_handle: client_acceptor_handle,
            _client_servant_handles: client_servant_handles_read,
            _batch_ticker_handle: batch_ticker_handle,
        })
    }

    /// Returns whether a client ID is connected to me.
    pub(crate) fn has_client(&self, client: ClientId) -> bool {
        let tx_replies_guard = self.tx_replies.guard();
        tx_replies_guard.contains_key(&client)
    }

    /// Waits for the next batch dumping signal and collects all requests
    /// currently in the req channel. Returns a non-empty `VecDeque` of
    /// requests on success.
    pub(crate) async fn get_req_batch(
        &mut self,
    ) -> Result<Vec<(ClientId, ApiRequest)>, SummersetError> {
        let mut batch = Vec::with_capacity(self.max_batch_size);

        // ignore ticks with an empty batch
        while batch.is_empty() {
            self.batch_notify.notified().await;

            while self.max_batch_size == 0 || batch.len() < self.max_batch_size
            {
                match self.rx_req.try_recv() {
                    Ok((client, req)) => batch.push((client, req)),
                    Err(TryRecvError::Empty) => break,
                    Err(e) => return Err(SummersetError::from(e)),
                }
            }
        }

        debug_assert!(!batch.is_empty());
        Ok(batch)
    }

    /// Sends a reply back to client by sending to the reply channel.
    pub(crate) fn send_reply(
        &mut self,
        reply: ApiReply,
        client: ClientId,
    ) -> Result<(), SummersetError> {
        let tx_replies_guard = self.tx_replies.guard();
        match tx_replies_guard.get(&client) {
            Some(tx_reply) => {
                tx_reply.send(reply).map_err(SummersetError::msg)?;
                Ok(())
            }
            None => {
                logged_err!(
                    self.me;
                    "client ID {} not found among active clients",
                    client
                )
            }
        }
    }

    /// Broadcasts a reply to all connected clients (mostly used for testing).
    #[allow(dead_code)]
    pub(crate) fn bcast_reply(
        &mut self,
        reply: ApiReply,
    ) -> Result<(), SummersetError> {
        let tx_replies_guard = self.tx_replies.guard();
        for tx_reply in tx_replies_guard.values() {
            tx_reply.send(reply.clone()).map_err(SummersetError::msg)?;
        }
        Ok(())
    }
}

// ExternalApi client_acceptor thread implementation
impl ExternalApi {
    /// Accepts a new client connection.
    async fn accept_new_client(
        me: ReplicaId,
        mut stream: TcpStream,
        addr: SocketAddr,
        tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
        tx_replies: &mut flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<ApiReply>,
        >,
        client_servant_handles: &mut flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
        tx_exit: mpsc::UnboundedSender<ClientId>,
    ) -> Result<(), SummersetError> {
        let id = match stream.read_u64().await {
            Ok(id) => id,
            Err(e) => {
                return logged_err!(me; "error receiving new client ID: {}", e);
            }
        };

        let mut tx_replies_guard = tx_replies.guard();
        if let Some(sender) = tx_replies_guard.get(&id) {
            if sender.is_closed() {
                // if this client ID has left before, garbage collect it now
                let mut client_servant_handles_guard =
                    client_servant_handles.guard();
                client_servant_handles_guard.remove(id);
                tx_replies_guard.remove(id);
            } else {
                return logged_err!(me; "duplicate client ID listened: {}", id);
            }
        }
        pf_debug!(me; "accepted new client {}", id);

        let (tx_reply, rx_reply) = mpsc::unbounded_channel();
        tx_replies_guard.insert(id, tx_reply);

        let client_servant_handle = tokio::spawn(Self::client_servant_thread(
            me, id, addr, stream, tx_req, rx_reply, tx_exit,
        ));
        let mut client_servant_handles_guard = client_servant_handles.guard();
        client_servant_handles_guard.insert(id, client_servant_handle);

        client_servant_handles_guard.publish();
        tx_replies_guard.publish();
        Ok(())
    }

    /// Removes handles of a left client connection.
    fn remove_left_client(
        me: ReplicaId,
        id: ClientId,
        tx_replies: &mut flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<ApiReply>,
        >,
        client_servant_handles: &mut flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
    ) -> Result<(), SummersetError> {
        let mut tx_replies_guard = tx_replies.guard();
        if !tx_replies_guard.contains_key(&id) {
            return logged_err!(me; "client {} not found among active ones", id);
        }
        tx_replies_guard.remove(id);

        let mut client_servant_handles_guard = client_servant_handles.guard();
        client_servant_handles_guard.remove(id);

        Ok(())
    }

    /// Client acceptor thread function.
    async fn client_acceptor_thread(
        me: ReplicaId,
        tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
        client_listener: TcpListener,
        mut tx_replies: flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<ApiReply>,
        >,
        mut client_servant_handles: flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
    ) {
        pf_debug!(me; "client_acceptor thread spawned");

        let local_addr = client_listener.local_addr().unwrap();
        pf_info!(me; "accepting clients on '{}'", local_addr);

        // create an exit mpsc channel for getting notified about termination
        // of client servant threads
        let (tx_exit, mut rx_exit) = mpsc::unbounded_channel();

        loop {
            tokio::select! {
                // new client connection
                accepted = client_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!(me; "error accepting client connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = Self::accept_new_client(
                        me,
                        stream,
                        addr,
                        tx_req.clone(),
                        &mut tx_replies,
                        &mut client_servant_handles,
                        tx_exit.clone()
                    ).await {
                        pf_error!(me; "error accepting new client: {}", e);
                    }
                },

                // a client servant thread exits
                id = rx_exit.recv() => {
                    let id = id.unwrap();
                    if let Err(e) = Self::remove_left_client(
                        me,
                        id,
                        &mut tx_replies,
                        &mut client_servant_handles
                    ) {
                        pf_error!(me; "error removing left client {}: {}", id, e);
                    }
                }
            }
        }

        // pf_debug!(me; "client_acceptor thread exitted");
    }
}

// ExternalApi client_servant thread implementation
impl ExternalApi {
    /// Reads a client request from given TcpStream.
    async fn read_req(
        // first 8 btyes being the request length, and the rest bytes being the
        // request itself
        req_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<ApiRequest, SummersetError> {
        safe_tcp_read(req_buf, conn_read).await
    }

    /// Writes a reply through given TcpStream.
    fn write_reply(
        reply_buf: &mut BytesMut,
        reply_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        reply: Option<&ApiReply>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(reply_buf, reply_buf_cursor, conn_write, reply)
    }

    /// Client request listener and reply sender thread function.
    async fn client_servant_thread(
        me: ReplicaId,
        id: ClientId,
        addr: SocketAddr,
        conn: TcpStream,
        tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
        mut rx_reply: mpsc::UnboundedReceiver<ApiReply>,
        tx_exit: mpsc::UnboundedSender<ClientId>,
    ) {
        pf_debug!(me; "client_servant thread for {} '{}' spawned", id, addr);

        let (mut conn_read, conn_write) = conn.into_split();
        let mut req_buf = BytesMut::with_capacity(8 + 1024);
        let mut reply_buf = BytesMut::with_capacity(8 + 1024);
        let mut reply_buf_cursor = 0;

        let mut retrying = false;
        loop {
            tokio::select! {
                // select between getting a new reply to send back and receiving
                // new client request, prioritizing the former
                biased;

                // gets a reply to send back
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
                                    // pf_trace!(me; "replied -> {} reply {:?}", id, reply);
                                }
                                Ok(false) => {
                                    pf_debug!(me; "should start retrying reply send -> {}", id);
                                    retrying = true;
                                }
                                Err(_e) => {
                                    // NOTE: commented out to prevent console lags
                                    // during benchmarking
                                    // pf_error!(me; "error replying -> {}: {}", id, e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // retrying last unsuccessful send
                _ = conn_write.writable(), if retrying => {
                    match Self::write_reply(
                        &mut reply_buf,
                        &mut reply_buf_cursor,
                        &conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!(me; "finished retrying last reply send -> {}", id);
                            retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!(me; "still should retry last reply send -> {}", id);
                        }
                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            // during benchmarking
                            // pf_error!(me; "error retrying last reply send -> {}: {}", id, e);
                        }
                    }
                },

                // receives client request
                req = Self::read_req(&mut req_buf, &mut conn_read) => {
                    match req {
                        Ok(ApiRequest::Leave) => {
                            // client leaving, send dummy reply and break
                            let reply = ApiReply::Leave;
                            if let Err(_e) = Self::write_reply(
                                &mut reply_buf,
                                &mut reply_buf_cursor,
                                &conn_write,
                                Some(&reply),
                            ) {
                                // pf_error!(me; "error replying -> {}: {}", id, e);
                            } else { // NOTE: skips `WouldBlock` error check here
                                pf_debug!(me; "client {} has left", id);
                            }
                            break;
                        },

                        Ok(req) => {
                            // pf_trace!(me; "request <- {} req {:?}", id, req);
                            if let Err(e) = tx_req.send((id, req)) {
                                pf_error!(me; "error sending to tx_req for {}: {}", id, e);
                            }
                        },

                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            // during benchmarking
                            // pf_error!(me; "error reading request <- {}: {}", id, e);
                            break; // probably the client exitted without `leave()`
                        }
                    }
                }
            }
        }

        if let Err(e) = tx_exit.send(id) {
            pf_error!(me; "error sending exit signal for {}: {}", id, e);
        }
        pf_debug!(me; "client_servant thread for {} '{}' exitted", id, addr);
    }
}

// ExternalApi batch_ticker thread implementation
impl ExternalApi {
    /// Batch ticker thread function.
    async fn batch_ticker_thread(
        _me: ReplicaId,
        batch_interval: Duration,
        batch_notify: Arc<Notify>,
    ) {
        let mut interval = time::interval(batch_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            batch_notify.notify_one();
            // pf_trace!(me; "batch interval ticked");
        }
    }
}

#[cfg(test)]
mod external_tests {
    use super::*;
    use crate::server::{Command, CommandResult};
    use crate::client::{ClientId, ClientApiStub};
    use tokio::sync::Barrier;
    use tokio::time::{self, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_req_reply() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // server-side
            let mut api = ExternalApi::new_and_setup(
                0,
                "127.0.0.1:40110".parse()?,
                Duration::from_millis(1),
                0,
            )
            .await?;
            barrier2.wait().await;
            // recv requests from client
            let mut reqs: Vec<(ClientId, ApiRequest)> = vec![];
            while reqs.len() < 3 {
                let mut req_batch = api.get_req_batch().await?;
                reqs.append(&mut req_batch);
            }
            let client = reqs[0].0;
            assert_eq!(client, 2857);
            debug_assert!(api.has_client(2857));
            assert_eq!(
                reqs[0].1,
                ApiRequest::Req {
                    id: 0,
                    cmd: Command::Put {
                        key: "Jose".into(),
                        value: "123".into(),
                    },
                }
            );
            assert_eq!(
                reqs[1].1,
                ApiRequest::Req {
                    id: 1,
                    cmd: Command::Get { key: "Jose".into() },
                }
            );
            assert_eq!(
                reqs[2].1,
                ApiRequest::Req {
                    id: 1,
                    cmd: Command::Get { key: "Jose".into() },
                }
            );
            // send replies to client
            api.send_reply(
                ApiReply::Reply {
                    id: 0,
                    result: Some(CommandResult::Put { old_value: None }),
                    redirect: None,
                },
                client,
            )?;
            api.send_reply(
                ApiReply::Reply {
                    id: 0,
                    result: None,
                    redirect: Some(1),
                },
                client,
            )?;
            api.send_reply(
                ApiReply::Reply {
                    id: 1,
                    result: Some(CommandResult::Get {
                        value: Some("123".into()),
                    }),
                    redirect: None,
                },
                client,
            )?;
            Ok::<(), SummersetError>(())
        });
        // client-side
        barrier.wait().await;
        let mut api_stub = ClientApiStub::new_by_connect(
            2857,
            "127.0.0.1:43170".parse()?,
            "127.0.0.1:40110".parse()?,
        )
        .await?;
        // send requests to server
        api_stub.send_req(Some(&ApiRequest::Req {
            id: 0,
            cmd: Command::Put {
                key: "Jose".into(),
                value: "123".into(),
            },
        }))?;
        api_stub.send_req(Some(&ApiRequest::Req {
            id: 1,
            cmd: Command::Get { key: "Jose".into() },
        }))?;
        api_stub.send_req(Some(&ApiRequest::Req {
            id: 1,
            cmd: Command::Get { key: "Jose".into() },
        }))?;
        // recv replies from server
        assert_eq!(
            api_stub.recv_reply().await?,
            ApiReply::Reply {
                id: 0,
                result: Some(CommandResult::Put { old_value: None }),
                redirect: None,
            }
        );
        assert_eq!(
            api_stub.recv_reply().await?,
            ApiReply::Reply {
                id: 0,
                result: None,
                redirect: Some(1),
            }
        );
        assert_eq!(
            api_stub.recv_reply().await?,
            ApiReply::Reply {
                id: 1,
                result: Some(CommandResult::Get {
                    value: Some("123".into())
                }),
                redirect: None,
            }
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_client_leave() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // server-side
            let mut api = ExternalApi::new_and_setup(
                0,
                "127.0.0.1:40120".parse()?,
                Duration::from_millis(1),
                0,
            )
            .await?;
            barrier2.wait().await;
            // recv request from client
            let mut reqs: Vec<(ClientId, ApiRequest)> = vec![];
            while reqs.is_empty() {
                let mut req_batch = api.get_req_batch().await?;
                reqs.append(&mut req_batch);
            }
            let client = reqs[0].0;
            assert_eq!(client, 2857);
            debug_assert!(api.has_client(2857));
            assert_eq!(
                reqs[0].1,
                ApiRequest::Req {
                    id: 0,
                    cmd: Command::Put {
                        key: "Jose".into(),
                        value: "123".into(),
                    },
                }
            );
            // send reply to client
            api.send_reply(
                ApiReply::Reply {
                    id: 0,
                    result: Some(CommandResult::Put { old_value: None }),
                    redirect: None,
                },
                client,
            )?;
            // recv request from new client
            reqs.clear();
            while reqs.is_empty() {
                let mut req_batch = api.get_req_batch().await?;
                reqs.append(&mut req_batch);
            }
            let client = reqs[0].0;
            assert_eq!(client, 2858);
            debug_assert!(api.has_client(2858));
            debug_assert!(!api.has_client(2857));
            assert_eq!(
                reqs[0].1,
                ApiRequest::Req {
                    id: 0,
                    cmd: Command::Put {
                        key: "Jose".into(),
                        value: "456".into(),
                    },
                }
            );
            // send reply to new client
            api.send_reply(
                ApiReply::Reply {
                    id: 0,
                    result: Some(CommandResult::Put {
                        old_value: Some("123".into()),
                    }),
                    redirect: None,
                },
                client,
            )?;
            Ok::<(), SummersetError>(())
        });
        // client-side
        {
            barrier.wait().await;
            let mut api_stub = ClientApiStub::new_by_connect(
                2857,
                "127.0.0.1:44170".parse()?,
                "127.0.0.1:40120".parse()?,
            )
            .await?;
            // send request to server
            api_stub.send_req(Some(&ApiRequest::Req {
                id: 0,
                cmd: Command::Put {
                    key: "Jose".into(),
                    value: "123".into(),
                },
            }))?;
            // recv reply from server
            assert_eq!(
                api_stub.recv_reply().await?,
                ApiReply::Reply {
                    id: 0,
                    result: Some(CommandResult::Put { old_value: None }),
                    redirect: None,
                }
            );
            // leave
            api_stub.send_req(Some(&ApiRequest::Leave))?;
            assert_eq!(api_stub.recv_reply().await?, ApiReply::Leave);
            time::sleep(Duration::from_millis(100)).await;
        }
        {
            // come back as new client
            let mut api_stub = ClientApiStub::new_by_connect(
                2858,
                "127.0.0.1:44170".parse()?,
                "127.0.0.1:40120".parse()?,
            )
            .await?;
            // send request to server
            api_stub.send_req(Some(&ApiRequest::Req {
                id: 0,
                cmd: Command::Put {
                    key: "Jose".into(),
                    value: "456".into(),
                },
            }))?;
            // recv reply from server
            assert_eq!(
                api_stub.recv_reply().await?,
                ApiReply::Reply {
                    id: 0,
                    result: Some(CommandResult::Put {
                        old_value: Some("123".into())
                    }),
                    redirect: None,
                }
            );
        }
        Ok(())
    }
}
