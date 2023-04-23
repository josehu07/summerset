//! Summerset server external API module implementation.

use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use crate::core::utils::SummersetError;
use crate::core::replica::{ReplicaId, GeneralReplica};
use crate::core::client::ClientId;
use crate::core::statemach::{Command, CommandResult};

use serde::{Serialize, Deserialize};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

use log::{trace, debug, info, warn, error};

/// External API request ID type.
pub type RequestId = u64;

/// Request received from client.
// TODO: add information fields such as read-only flag...
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ApiRequest {
    /// Request ID.
    id: RequestId,

    /// Command to the state machine.
    cmd: Command,
}

/// Reply back to client.
// TODO: add information fields such as success status...
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ApiReply {
    /// Request Id corresponding to this reply.
    id: RequestId,

    /// Command execution result returned by the state machine.
    result: CommandResult,
}

/// The external client-facing API module.
#[derive(Debug)]
pub struct ExternalApi<'r, Rpl>
where
    Rpl: 'r + GeneralReplica,
{
    /// Reference to protocol-specific replica struct.
    replica: &'r Rpl,

    /// Receiver side of the req channel.
    rx_req: Option<mpsc::Receiver<(ClientId, ApiRequest)>>,

    /// Map from client ID -> sender side of its reply channel.
    tx_replies: Arc<Mutex<HashMap<ClientId, mpsc::Sender<ApiReply>>>>,

    /// TCP listener for client connections, shared with the client acceptor
    /// thread.
    client_listener: Option<Arc<Mutex<TcpListener>>>,

    /// Notify used as batch dumping signal, shared with the batch ticker
    /// thread.
    batch_notify: Arc<Notify>,

    /// Join handle of the client acceptor thread.
    client_acceptor_handle: Option<JoinHandle<()>>,

    /// Map from client ID -> request listener thread join handles, shared
    /// with the client acceptor thread.
    client_servant_handles: Arc<Mutex<HashMap<ClientId, JoinHandle<()>>>>,

    /// Join handle of the batch ticker thread.
    batch_ticker_handle: Option<JoinHandle<()>>,
}

// ExternalApi Public API implementation
impl<'r, Rpl> ExternalApi<'r, Rpl> {
    /// Size of a request in bytes.
    pub const REQ_SIZE: usize = size_of::<ApiRequest>();

    /// Size of a reply in bytes.
    pub const REPLY_SIZE: usize = size_of::<ApiReply>();

    /// Creates a new external API module.
    pub fn new(replica: &'r Rpl) -> Self {
        ExternalApi {
            replica,
            rx_req: None,
            tx_replies: Arc::new(Mutex::new(HashMap::new())),
            client_listener: None,
            batch_notify: Arc::new(Notify::new()),
            client_acceptor_handle: None,
            client_servant_handles: Arc::new(Mutex::new(HashMap::new())),
            batch_ticker_handle: None,
        }
    }

    /// Spawns the client acceptor thread and the batch ticker thread. Creates
    /// a req channel for buffering incoming client requests and a reply
    /// channel for sending back replies to clients. The capacity
    /// `chan_req_cap` determines a hard limit of how many requests we can
    /// buffer for batching. Creates a TCP listener for client connections.
    pub async fn setup(
        &mut self,
        chan_req_cap: usize,
        chan_reply_cap: usize,
        batch_interval: Duration,
    ) -> Result<(), SummersetError> {
        let me = self.replica.id();

        if let Some(_) = self.client_acceptor_handle {
            return logged_err!(me, "client_acceptor thread already spawned");
        }
        if let Some(_) = self.batch_ticker_handle {
            return logged_err!(me, "batch_ticker thread already spawned");
        }
        if chan_req_cap == 0 {
            return logged_err!(me, "invalid chan_req_cap {}", chan_req_cap);
        }
        if chan_reply_cap == 0 {
            return logged_err!(
                me,
                "invalid chan_reply_cap {}",
                chan_reply_cap
            );
        }
        if batch_interval < Duration::as_micros(1) {
            return logged_err!(
                me,
                "batch_interval '{}' too small",
                batch_interval
            );
        }

        let (tx_req, mut rx_req) = mpsc::channel(chan_req_cap);
        self.rx_req = Some(rx_req);

        let client_listener =
            TcpListener::bind(self.replica.api_addr()).await?;
        self.client_listener = Some(Arc::new(Mutex::new(client_listener)));

        let client_waiter_handle = tokio::spawn(Self::client_waiter_thread(
            me,
            tx_req,
            chan_reply_cap,
            self.client_listener.unwrap().clone(),
            self.tx_replies.clone(),
            self.client_servant_handles.clone(),
        ));
        let batch_ticker_handle = tokio::spawn(Self::batch_ticker_thread(
            me,
            batch_interval,
            self.batch_notify.clone(),
        ));
        self.client_waiter_handle = Some(client_waiter_handle);
        self.batch_ticker_handle = Some(batch_ticker_handle);

        Ok(())
    }

    /// Waits for the next batch dumping signal and collects all requests
    /// currently in the req channel. Returns a `VecDeque` of requests on
    /// success.
    pub async fn get_req_batch(
        &mut self,
    ) -> Result<VecDeque<(ClientId, ApiRequest)>, SummersetError> {
        if let None = self.client_waiter_handle {
            return logged_err!(
                self.replica.id(),
                "get_req_batch called before setup"
            );
        }

        self.batch_notify.notified().await;
        let batch = VecDeque::new();

        match self.rx_req {
            Some(ref mut rx_req) => loop {
                match self.rx_req.try_recv() {
                    Ok((client, req)) => batch.push_back((client, req)),
                    Err(TryRecvError::Empty) => break,
                    Err(e) => return Err(e),
                }
            },
            None => logged_err!(self.replica.id(), "rx_req not created yet"),
        }

        Ok(batch)
    }

    /// Sends a reply back to client by sending to the reply channel.
    pub async fn send_reply(
        &mut self,
        reply: ApiReply,
        client: ClientId,
    ) -> Result<(), SummersetError> {
        let mut tx_replies_guard = self.tx_replies.lock().unwrap();
        if !tx_replies_guard.contains_key(client) {
            return logged_err!(
                self.replica.id(),
                "client ID {} not found among active clients",
                client
            );
        }

        tx_replies_guard[client].send(reply).await?;
        Ok(())
    }
}

// ExternalApi client_acceptor thread implementation
impl<'r, Rpl> ExternalApi<'r, Rpl> {
    /// Client acceptor thread function.
    async fn client_acceptor_thread(
        me: ReplicaId,
        tx_req: mpsc::Sender<(ClientId, ApiRequest)>,
        chan_reply_cap: usize,
        client_listener: Arc<Mutex<TcpListener>>,
        tx_replies: Arc<Mutex<HashMap<ClientId, mpsc::Sender<ApiReply>>>>,
        client_servant_handles: Arc<Mutex<HashMap<ClientId, JoinHandle<()>>>>,
    ) {
        pf_debug!(me, "client_acceptor thread spawned");

        // need tokio::sync::Mutex here since held across await
        let mut client_listener_guard = client_listener.lock().unwrap();

        loop {
            let mut stream = client_listener_guard.accept().await;
            if let Err(e) = stream {
                pf_warn!(me, "error accepting client connection: {}", e);
                continue;
            }
            let mut stream = stream.unwrap();

            let id = stream.read_u64().await; // receive client ID
            if let Err(e) = id {
                pf_error!(me, "error receiving new client ID: {}", e);
                continue;
            }
            let id = id.unwrap();

            let mut tx_replies_guard = tx_replies.lock().unwrap();
            if tx_replies_guard.contains_key(&id) {
                pf_error!(me, "duplicate client ID listened: {}", id);
                continue;
            }

            let (tx_reply, mut rx_reply) = mpsc::channel(chan_reply_cap);
            tx_replies_guard.insert(id, tx_reply);

            let client_servant_handle =
                tokio::spawn(Self::client_servant_thread(
                    me,
                    id,
                    stream,
                    tx_req.clone(),
                    rx_reply,
                ));
            let mut client_servant_handles_guard =
                client_servant_handles.lock().unwrap();
            client_servant_handles_guard.insert(id, client_servant_handle);

            pf_info!(me, "accepted new client {}", id);
        }

        pf_debug!(me, "client_acceptor thread exitted");
    }
}

// ExternalApi client_servant thread implementation
impl<'r, Rpl> ExternalApi<'r, Rpl> {
    /// Reads a client request from given TcpStream.
    async fn read_req(
        conn: &mut TcpStream,
    ) -> Result<ApiRequest, SummersetError> {
        let req_buf: Vec<u8> = vec![0; Self::REQ_SIZE];
        conn.read_exact(&mut req_buf[..]).await?;
        let req = decode_from_slice(&req_buf)?;
        Ok(req)
    }

    /// Writes a reply through given TcpStream.
    async fn write_reply(
        reply: &ApiReply,
        conn: &mut TcpStream,
    ) -> Result<(), SummersetError> {
        let reply_bytes = encode_to_vec(reply)?;
        conn.write_all(&reply_bytes[..]).await?;
        Ok(())
    }

    /// Client request listener and reply sender thread function.
    async fn client_servant_thread(
        me: ReplicaId,
        id: ClientId,
        conn: TcpStream,
        tx_req: mpsc::Sender<(ClientId, ApiRequest)>,
        rx_reply: mpsc::Receiver<ApiReply>,
    ) {
        pf_debug!(me, "client_servant thread for {} spawned", id);

        loop {
            tokio::select! {
                // select between getting a new reply to send back and receiving
                // new client request, prioritizing the former
                biased;

                // gets a reply to send back
                reply = rx_reply.recv() => {
                    match reply {
                        Some(reply) => {
                            if let Err(e) = Self::write_reply(&reply,&mut conn).await {
                                pf_error!(me, "error replying to {}: {}", id, e);
                            } else {
                                pf_trace!(me, "replied to {} reply {:?}", id, reply);
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // receives client request
                req = Self::read_req(&mut conn) => {
                    match req {
                        Ok(req) => {
                            pf_trace!(me, "request from {} req {:?}", id, req);
                            if let Err(e) = tx_req.send((id, req)).await {
                                pf_error!(
                                    me, "error sending to tx_req for {}: {}", id, e
                                );
                            }
                        },
                        Err(e) => {
                            pf_error!(me, "error reading request from {}", id);
                        }
                    }
                },
            }
        }

        pf_debug!(me, "client_servant thread for {} exitted", id);
    }
}

// ExternalApi batch_ticker thread implementation
impl<'r, Rpl> ExternalApi<'r, Rpl> {
    /// Batch ticker thread function.
    async fn batch_ticker_thread(
        me: ReplicaId,
        batch_interval: Duration,
        batch_notify: Arc<Notify>,
    ) {
        let mut interval = time::interval(batch_interval);

        loop {
            interval.tick().await;
            batch_notify.notify_one();
            pf_debug!(me, "batch interval ticked");
        }
    }
}

#[cfg(test)]
mod external_tests {
    use super::*;
    use std::time::SystemTime;
    use crate::core::replica::DummyReplica;
    use crate::core::client::DummyClient;
    use crate::core::external::{ApiRequest, ApiReply};
    use crate::core::statemach::{Command, CommandResult};
    use tokio::sync::Barrier;
    use tokio::time::{self, Duration};

    #[test]
    fn interval_tick() {
        let mut interval = time::interval(Duration::from_micros(100));
        let threshold = Duration::from_micros(10);
        let mut now = SystemTime::now();
        for _ in 0..3 {
            tokio_test::block_on(interval.tick());
            let new_now = SystemTime::now();
            assert!(new_now - now > threshold);
            now = new_now;
        }
    }

    #[test]
    fn api_setup() -> Result<(), SummersetError> {
        let replica = Default::default();
        let mut api = ExternalApi::new(&replica);
        assert!(
            tokio_test::block_on(api.setup(0, 0, Duration::as_millis(1)))
                .is_err()
        );
        assert!(tokio_test::block_on(api.setup(
            100,
            100,
            Duration::as_nanos(10)
        ))
        .is_err());
        tokio_test::block_on(api.setup(100, 100, Duration::as_millis(1)))?;
        assert!(api.rx_req.is_some());
        assert!(api.client_listener.is_some());
        assert!(api.client_acceptor_handle.is_some());
        assert!(api.batch_ticker_handle.is_some());
        Ok(())
    }

    #[test]
    fn req_reply_api() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // server-side
            let replica: DummyReplica = Default::default();
            let mut api = ExternalApi::new(&replica);
            api.setup(5, 5, Duration::from_millis(1)).await?;
            barrier2.wait().await;
            // TODO: complete me!
        });
        // client-side
        let client: DummyClient = Default::default();
        tokio_test::block_on(barrier.wait());
        tokio_test::block_on(client.connect())?;
        tokio_test::block_on(client.send_req(ApiRequest {
            id: 0,
            cmd: Command::Put {
                key: "Jose".into(),
                value: "123".into(),
            },
        }))?;
        tokio_test::block_on(client.send_req(ApiRequest {
            id: 1,
            cmd: Command::Get { key: "Jose".into() },
        }))?;
        assert_eq!(
            tokio_test::block_on(client.recv_reply())?,
            ApiReply {
                id: 0,
                result: CommandResult::PutResult { old_value: None }
            }
        );
        assert_eq!(
            tokio_test::block_on(client.recv_reply())?,
            ApiReply {
                id: 1,
                result: CommandResult::GetResult {
                    value: "123".into()
                }
            }
        );
        Ok(())
    }
}
