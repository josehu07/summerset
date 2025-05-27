//! Summerset server external API module implementation.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::client::ClientId;
use crate::server::{Command, CommandResult, ReplicaId};
use crate::utils::{
    safe_tcp_read, safe_tcp_write, tcp_bind_with_retry, Bitmap, SummersetError,
};

use get_size::GetSize;

use bytes::BytesMut;

use serde::{Deserialize, Serialize};

use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Notify};
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

    /// Responders configuration change. (only used by relevant protocols)
    Conf {
        /// Client request ID.
        id: RequestId,

        /// Configuration change delta to be applied.
        delta: ConfChange,
    },

    /// Client leave notification.
    Leave,
}

impl ApiRequest {
    /// Is the command contained read-only? If so, returns the key queried.
    #[inline]
    pub fn read_only(&self) -> Option<&String> {
        if let ApiRequest::Req { cmd, .. } = self {
            cmd.read_only()
        } else {
            None
        }
    }

    /// Is the command contained non-read-only? If so, returns the key updated.
    #[inline]
    pub fn write_key(&self) -> Option<&String> {
        if let ApiRequest::Req { cmd, .. } = self {
            cmd.write_key()
        } else {
            None
        }
    }

    /// Is the request a configuration change request?
    #[inline]
    pub fn conf_change(&self) -> bool {
        matches!(self, ApiRequest::Conf { .. })
    }
}

/// Configuration change delta used in request API.
#[derive(PartialEq, Eq, Default, Clone, Serialize, Deserialize, GetSize)]
pub struct ConfChange {
    /// If true, indicates a conf reset to default; all the following fields
    /// will be ignored.
    pub reset: bool,

    /// If not `None`, the new supposed leader; otherwise not updated.
    pub leader: Option<ReplicaId>,

    /// Keys range to apply the responders bitmap. If `None`, means the full
    /// range of all keys.
    pub range: Option<(String, String)>,

    /// If not `None`, a new responders bitmap for some range; otherwise not
    /// updated
    pub responders: Option<Bitmap>,
}

impl fmt::Debug for ConfChange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.reset {
            write!(f, "Δ reset")
        } else {
            write!(
                f,
                "Δ <{}> {}=>{}",
                if let Some(leader) = &self.leader {
                    leader.to_string()
                } else {
                    "_".into()
                },
                if let Some((start, end)) = &self.range {
                    format!("{}-{}", start, end)
                } else {
                    "full".into()
                },
                if let Some(responders) = &self.responders {
                    format!("{:?}", responders)
                } else {
                    "_".into()
                }
            )
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

        /// Set if failed read-only attempt when near quorum read (or
        /// some other read-only optimization) enabled and retry indicated.
        rq_retry: Option<Command>,
    },

    /// Reply to responders configuration change. (only for relevant protocols)
    Conf {
        /// ID of the corresponding client request.
        id: RequestId,

        /// True if successful; false otherwise (e.g., if config not valid).
        success: bool,
    },

    /// Reply to client leave notification.
    Leave,
}

impl ApiReply {
    /// Creates a normal reply with given result.
    #[inline]
    pub fn normal(id: RequestId, result: Option<CommandResult>) -> Self {
        ApiReply::Reply {
            id,
            result,
            redirect: None,
            rq_retry: None,
        }
    }

    /// Creates a reply with redirect hint.
    #[inline]
    pub fn redirect(id: RequestId, redirect: Option<ReplicaId>) -> Self {
        ApiReply::Reply {
            id,
            result: None,
            redirect,
            rq_retry: None,
        }
    }

    /// Creates a reply with rq_retry flag.
    #[inline]
    pub fn rq_retry(
        id: RequestId,
        read_cmd: Command,
        redirect: Option<ReplicaId>,
    ) -> Self {
        ApiReply::Reply {
            id,
            result: None,
            redirect,
            rq_retry: Some(read_cmd),
        }
    }
}

/// The external client-facing API module.
pub(crate) struct ExternalApi {
    /// My replica ID.
    _me: ReplicaId,

    /// Receiver side of the req channel.
    rx_req: mpsc::UnboundedReceiver<(ClientId, ApiRequest)>,

    /// Map from client ID -> sender side of its reply channel, shared with
    /// the client acceptor task.
    tx_replies: flashmap::ReadHandle<ClientId, mpsc::UnboundedSender<ApiReply>>,

    /// Notify used as batch dumping signal, shared with the batch ticker
    /// task.
    batch_notify: Arc<Notify>,

    /// Maximum number of requests to return per batch; 0 means no limit.
    max_batch_size: usize,

    /// Join handle of the client acceptor task.
    _client_acceptor_handle: JoinHandle<()>,

    /// Map from client ID -> client servant task join handles, shared with
    /// the client acceptor task.
    _client_servant_handles: flashmap::ReadHandle<ClientId, JoinHandle<()>>,

    /// Join handle of the batch ticker task.
    _batch_ticker_handle: JoinHandle<()>,
}

// ExternalApi public API implementation
impl ExternalApi {
    /// Creates a new external API module. Spawns the client acceptor task
    /// and the batch ticker task. Creates a req channel for buffering
    /// incoming client requests.
    pub(crate) async fn new_and_setup(
        me: ReplicaId,
        api_addr: SocketAddr,
        batch_interval: Duration,
        max_batch_size: usize,
    ) -> Result<Self, SummersetError> {
        if batch_interval < Duration::from_micros(1) {
            return logged_err!(
                "batch_interval {} us too small",
                batch_interval.as_micros()
            );
        }

        let (tx_req, rx_req) = mpsc::unbounded_channel();

        let (tx_replies_write, tx_replies_read) =
            flashmap::new::<ClientId, mpsc::UnboundedSender<ApiReply>>();

        let (client_servant_handles_write, client_servant_handles_read) =
            flashmap::new::<ClientId, JoinHandle<()>>();

        let client_listener = tcp_bind_with_retry(api_addr, 15).await?;
        let mut acceptor = ExternalApiAcceptorTask::new(
            tx_req,
            client_listener,
            tx_replies_write,
            client_servant_handles_write,
        );
        let client_acceptor_handle =
            tokio::spawn(async move { acceptor.run().await });

        let batch_notify = Arc::new(Notify::new());
        let mut batch_ticker = ExternalApiBatchTickerTask::new(
            me,
            batch_interval,
            batch_notify.clone(),
        );
        let batch_ticker_handle =
            tokio::spawn(async move { batch_ticker.run().await });

        Ok(ExternalApi {
            _me: me,
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

/// ExternalApi client acceptor task.
struct ExternalApiAcceptorTask {
    tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
    tx_replies:
        flashmap::WriteHandle<ClientId, mpsc::UnboundedSender<ApiReply>>,

    client_listener: TcpListener,
    client_servant_handles: flashmap::WriteHandle<ClientId, JoinHandle<()>>,

    tx_exit: mpsc::UnboundedSender<ClientId>,
    rx_exit: mpsc::UnboundedReceiver<ClientId>,
}

impl ExternalApiAcceptorTask {
    /// Creates the client acceptor task.
    fn new(
        tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
        client_listener: TcpListener,
        tx_replies: flashmap::WriteHandle<
            ClientId,
            mpsc::UnboundedSender<ApiReply>,
        >,
        client_servant_handles: flashmap::WriteHandle<ClientId, JoinHandle<()>>,
    ) -> Self {
        // create an exit mpsc channel for getting notified about termination
        // of client servant tasks
        let (tx_exit, rx_exit) = mpsc::unbounded_channel();

        ExternalApiAcceptorTask {
            tx_req,
            tx_replies,
            client_listener,
            client_servant_handles,
            tx_exit,
            rx_exit,
        }
    }

    /// Accepts a new client connection.
    async fn accept_new_client(
        &mut self,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        let id = match stream.read_u64().await {
            Ok(id) => id,
            Err(e) => {
                return logged_err!("error receiving new client ID: {}", e);
            }
        };

        let mut tx_replies_guard = self.tx_replies.guard();
        if let Some(sender) = tx_replies_guard.get(&id) {
            if sender.is_closed() {
                // if this client ID has left before, garbage collect it now
                let mut client_servant_handles_guard =
                    self.client_servant_handles.guard();
                client_servant_handles_guard.remove(id);
                tx_replies_guard.remove(id);
            } else {
                return logged_err!("duplicate client ID listened: {}", id);
            }
        }
        pf_debug!("accepted new client {}", id);

        let (tx_reply, rx_reply) = mpsc::unbounded_channel();
        tx_replies_guard.insert(id, tx_reply);

        let mut servant = ExternalApiServantTask::new(
            id,
            addr,
            stream,
            self.tx_req.clone(),
            rx_reply,
            self.tx_exit.clone(),
        );
        let client_servant_handle =
            tokio::spawn(async move { servant.run().await });
        let mut client_servant_handles_guard =
            self.client_servant_handles.guard();
        client_servant_handles_guard.insert(id, client_servant_handle);

        client_servant_handles_guard.publish();
        tx_replies_guard.publish();
        Ok(())
    }

    /// Removes handles of a left client connection.
    fn remove_left_client(
        &mut self,
        id: ClientId,
    ) -> Result<(), SummersetError> {
        let mut tx_replies_guard = self.tx_replies.guard();
        if !tx_replies_guard.contains_key(&id) {
            return logged_err!("client {} not found among active ones", id);
        }
        tx_replies_guard.remove(id);

        let mut client_servant_handles_guard =
            self.client_servant_handles.guard();
        client_servant_handles_guard.remove(id);

        Ok(())
    }

    /// Starts the client acceptor task loop.
    async fn run(&mut self) {
        pf_debug!("client_acceptor task spawned");

        let local_addr = self.client_listener.local_addr().unwrap();
        pf_info!("accepting clients on '{}'", local_addr);

        loop {
            tokio::select! {
                // new client connection
                accepted = self.client_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!("error accepting client connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = self.accept_new_client(
                        stream,
                        addr,
                    ).await {
                        pf_error!("error accepting new client: {}", e);
                    }
                },

                // a client servant task exits
                id = self.rx_exit.recv() => {
                    let id = id.unwrap();
                    if let Err(e) = self.remove_left_client(
                        id
                    ) {
                        pf_error!("error removing left client {}: {}", id, e);
                    }
                }
            }
        }

        // pf_debug!("client_acceptor task exited");
    }
}

/// ExternalApi per-client servant task.
struct ExternalApiServantTask {
    id: ClientId,
    addr: SocketAddr,

    conn_read: OwnedReadHalf,
    conn_write: OwnedWriteHalf,

    tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
    req_buf: BytesMut,

    rx_reply: mpsc::UnboundedReceiver<ApiReply>,
    reply_buf: BytesMut,
    reply_buf_cursor: usize,
    retrying: bool,

    tx_exit: mpsc::UnboundedSender<ClientId>,
}

impl ExternalApiServantTask {
    /// Creates a per-server servant task.
    fn new(
        id: ClientId,
        addr: SocketAddr,
        conn: TcpStream,
        tx_req: mpsc::UnboundedSender<(ClientId, ApiRequest)>,
        rx_reply: mpsc::UnboundedReceiver<ApiReply>,
        tx_exit: mpsc::UnboundedSender<ClientId>,
    ) -> Self {
        let (conn_read, conn_write) = conn.into_split();

        let req_buf = BytesMut::with_capacity(8 + 1024);
        let reply_buf = BytesMut::with_capacity(8 + 1024);
        let reply_buf_cursor = 0;
        let retrying = false;

        ExternalApiServantTask {
            id,
            addr,
            conn_read,
            conn_write,
            tx_req,
            req_buf,
            rx_reply,
            reply_buf,
            reply_buf_cursor,
            retrying,
            tx_exit,
        }
    }

    /// Reads a client request from given TcpStream.
    /// This is a non-method function to ease `tokio::select!` sharing.
    async fn read_req(
        // first 8 bytes being the request length, and the rest bytes being the
        // request itself
        req_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<ApiRequest, SummersetError> {
        safe_tcp_read(req_buf, conn_read).await
    }

    /// Writes a reply through given TcpStream.
    /// This is a non-method function to ease `tokio::select!` sharing.
    fn write_reply(
        reply_buf: &mut BytesMut,
        reply_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        reply: Option<&ApiReply>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(reply_buf, reply_buf_cursor, conn_write, reply)
    }

    /// Starts a per-client servant task loop.
    async fn run(&mut self) {
        pf_debug!(
            "client_servant task for {} '{}' spawned",
            self.id,
            self.addr
        );

        loop {
            tokio::select! {
                // select between getting a new reply to send back and receiving
                // new client request, prioritizing the former
                biased;

                // gets a reply to send back
                reply = self.rx_reply.recv(), if !self.retrying => {
                    match reply {
                        Some(reply) => {
                            match Self::write_reply(
                                &mut self.reply_buf,
                                &mut self.reply_buf_cursor,
                                &self.conn_write,
                                Some(&reply)
                            ) {
                                Ok(true) => {
                                    // pf_trace!("replied -> {} reply {:?}", id, reply);
                                }
                                Ok(false) => {
                                    pf_debug!("should start retrying reply send -> {}", self.id);
                                    self.retrying = true;
                                }
                                Err(_e) => {
                                    // NOTE: commented out to prevent console lags
                                    //       during benchmarking
                                    // pf_error!("error replying -> {}: {}", id, e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // retrying last unsuccessful send
                _ = self.conn_write.writable(), if self.retrying => {
                    match Self::write_reply(
                        &mut self.reply_buf,
                        &mut self.reply_buf_cursor,
                        &self.conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!("finished retrying last reply send -> {}", self.id);
                            self.retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!("still should retry last reply send -> {}", self.id);
                        }
                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            //       during benchmarking
                            // pf_error!("error retrying last reply send -> {}: {}", id, e);
                        }
                    }
                },

                // receives client request
                req = Self::read_req(&mut self.req_buf, &mut self.conn_read) => {
                    match req {
                        Ok(ApiRequest::Leave) => {
                            // client leaving, send dummy reply and break
                            let reply = ApiReply::Leave;
                            if let Err(_e) = Self::write_reply(
                                &mut self.reply_buf,
                                &mut self.reply_buf_cursor,
                                &self.conn_write,
                                Some(&reply),
                            ) {
                                // pf_error!("error replying -> {}: {}", id, e);
                            } else { // NOTE: skips `WouldBlock` error check here
                                pf_debug!("client {} has left", self.id);
                            }
                            break;
                        },

                        Ok(req) => {
                            // pf_trace!("request <- {} req {:?}", id, req);
                            if let Err(e) = self.tx_req.send((self.id, req)) {
                                pf_error!("error sending to tx_req for {}: {}", self.id, e);
                            }
                        },

                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            //       during benchmarking
                            // pf_error!("error reading request <- {}: {}", id, e);
                            break; // probably the client exited without `leave()`
                        }
                    }
                }
            }
        }

        if let Err(e) = self.tx_exit.send(self.id) {
            pf_error!("error sending exit signal for {}: {}", self.id, e);
        }
        pf_debug!("client_servant task for {} '{}' exited", self.id, self.addr);
    }
}

/// ExternalApi batch ticker task.
struct ExternalApiBatchTickerTask {
    _me: ReplicaId,

    batch_interval: Duration,
    batch_notify: Arc<Notify>,
}

impl ExternalApiBatchTickerTask {
    /// Creates the batch ticker task.
    fn new(
        me: ReplicaId,
        batch_interval: Duration,
        batch_notify: Arc<Notify>,
    ) -> Self {
        ExternalApiBatchTickerTask {
            _me: me,
            batch_interval,
            batch_notify,
        }
    }

    /// Starts the batch ticker task loop.
    async fn run(&mut self) {
        let mut interval = time::interval(self.batch_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            self.batch_notify.notify_one();
            // pf_trace!("batch interval ticked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ClientApiStub, ClientId};
    use crate::server::{Command, CommandResult};
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
                "127.0.0.1:30000".parse()?,
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
                ApiReply::normal(
                    0,
                    Some(CommandResult::Put { old_value: None }),
                ),
                client,
            )?;
            api.send_reply(ApiReply::redirect(0, Some(1)), client)?;
            api.send_reply(
                ApiReply::normal(
                    1,
                    Some(CommandResult::Get {
                        value: Some("123".into()),
                    }),
                ),
                client,
            )?;
            Ok::<(), SummersetError>(())
        });
        // client-side
        barrier.wait().await;
        let mut api_stub =
            ClientApiStub::new_by_connect(2857, "127.0.0.1:30000".parse()?)
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
            ApiReply::normal(0, Some(CommandResult::Put { old_value: None }))
        );
        assert_eq!(
            api_stub.recv_reply().await?,
            ApiReply::redirect(0, Some(1))
        );
        assert_eq!(
            api_stub.recv_reply().await?,
            ApiReply::normal(
                1,
                Some(CommandResult::Get {
                    value: Some("123".into()),
                }),
            )
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
                "127.0.0.1:30100".parse()?,
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
                ApiReply::normal(
                    0,
                    Some(CommandResult::Put { old_value: None }),
                ),
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
                ApiReply::normal(
                    0,
                    Some(CommandResult::Put {
                        old_value: Some("123".into()),
                    }),
                ),
                client,
            )?;
            Ok::<(), SummersetError>(())
        });
        // client-side
        {
            barrier.wait().await;
            let mut api_stub =
                ClientApiStub::new_by_connect(2857, "127.0.0.1:30100".parse()?)
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
                ApiReply::normal(
                    0,
                    Some(CommandResult::Put { old_value: None })
                )
            );
            // leave
            api_stub.send_req(Some(&ApiRequest::Leave))?;
            assert_eq!(api_stub.recv_reply().await?, ApiReply::Leave);
            time::sleep(Duration::from_millis(100)).await;
        }
        {
            // come back as new client
            let mut api_stub =
                ClientApiStub::new_by_connect(2858, "127.0.0.1:30100".parse()?)
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
                ApiReply::normal(
                    0,
                    Some(CommandResult::Put {
                        old_value: Some("123".into())
                    })
                )
            );
        }
        Ok(())
    }
}
