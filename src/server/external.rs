//! Summerset server external API module implementation.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, Command, CommandResult};
use crate::client::ClientId;

use serde::{Serialize, Deserialize};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

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
pub struct ExternalApi {
    /// My replica ID.
    me: ReplicaId,

    /// Receiver side of the req channel.
    rx_req: Option<mpsc::Receiver<(ClientId, ApiRequest)>>,

    /// Map from client ID -> sender side of its reply channel, shared with
    /// the client acceptor thread.
    tx_replies: Option<flashmap::ReadHandle<ClientId, mpsc::Sender<ApiReply>>>,

    /// Notify used as batch dumping signal, shared with the batch ticker
    /// thread.
    batch_notify: Arc<Notify>,

    /// Join handle of the client acceptor thread.
    client_acceptor_handle: Option<JoinHandle<()>>,

    /// Map from client ID -> client servant thread join handles, shared with
    /// the client acceptor thread.
    client_servant_handles:
        Option<flashmap::ReadHandle<ClientId, JoinHandle<()>>>,

    /// Join handle of the batch ticker thread.
    batch_ticker_handle: Option<JoinHandle<()>>,
}

// ExternalApi Public API implementation
impl ExternalApi {
    /// Creates a new external API module.
    pub fn new(me: ReplicaId) -> Self {
        ExternalApi {
            me,
            rx_req: None,
            tx_replies: None,
            batch_notify: Arc::new(Notify::new()),
            client_acceptor_handle: None,
            client_servant_handles: None,
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
        api_addr: SocketAddr,
        batch_interval: Duration,
        chan_req_cap: usize,
        chan_reply_cap: usize,
    ) -> Result<(), SummersetError> {
        if self.client_acceptor_handle.is_some() {
            return logged_err!(self.me; "setup already done");
        }
        if chan_req_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_req_cap {}",
                chan_req_cap
            );
        }
        if chan_reply_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_reply_cap {}",
                chan_reply_cap
            );
        }
        if batch_interval < Duration::from_micros(1) {
            return logged_err!(
                self.me;
                "batch_interval {} us too small",
                batch_interval.as_micros()
            );
        }

        let (tx_req, rx_req) = mpsc::channel(chan_req_cap);
        self.rx_req = Some(rx_req);

        let (tx_replies_write, tx_replies_read) =
            flashmap::new::<ClientId, mpsc::Sender<ApiReply>>();
        self.tx_replies = Some(tx_replies_read);

        let client_listener = TcpListener::bind(api_addr).await?;

        let (client_servant_handles_write, client_servant_handles_read) =
            flashmap::new::<ClientId, JoinHandle<()>>();
        self.client_servant_handles = Some(client_servant_handles_read);

        let client_acceptor_handle =
            tokio::spawn(Self::client_acceptor_thread(
                self.me,
                tx_req,
                chan_reply_cap,
                client_listener,
                tx_replies_write,
                client_servant_handles_write,
            ));
        self.client_acceptor_handle = Some(client_acceptor_handle);

        let batch_ticker_handle = tokio::spawn(Self::batch_ticker_thread(
            self.me,
            batch_interval,
            self.batch_notify.clone(),
        ));
        self.batch_ticker_handle = Some(batch_ticker_handle);

        Ok(())
    }

    /// Waits for the next batch dumping signal and collects all requests
    /// currently in the req channel. Returns a `VecDeque` of requests on
    /// success.
    pub async fn get_req_batch(
        &mut self,
    ) -> Result<VecDeque<(ClientId, ApiRequest)>, SummersetError> {
        if let None = self.client_acceptor_handle {
            return logged_err!(self.me; "get_req_batch called before setup");
        }

        self.batch_notify.notified().await;
        let mut batch = VecDeque::new();

        match self.rx_req {
            Some(ref mut rx_req) => loop {
                match rx_req.try_recv() {
                    Ok((client, req)) => batch.push_back((client, req)),
                    Err(TryRecvError::Empty) => break,
                    Err(e) => return Err(SummersetError::from(e)),
                }
            },
            None => return logged_err!(self.me; "rx_req not created yet"),
        }

        Ok(batch)
    }

    /// Sends a reply back to client by sending to the reply channel.
    pub async fn send_reply(
        &mut self,
        reply: ApiReply,
        client: ClientId,
    ) -> Result<(), SummersetError> {
        if let None = self.client_acceptor_handle {
            return logged_err!(self.me; "send_reply called before setup");
        }

        let tx_replies_guard = self.tx_replies.as_ref().unwrap().guard();
        match tx_replies_guard.get(&client) {
            Some(tx_reply) => {
                tx_reply
                    .send(reply)
                    .await
                    .map_err(|e| SummersetError(e.to_string()))?;
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
}

// ExternalApi client_acceptor thread implementation
impl ExternalApi {
    /// Client acceptor thread function.
    async fn client_acceptor_thread(
        me: ReplicaId,
        tx_req: mpsc::Sender<(ClientId, ApiRequest)>,
        chan_reply_cap: usize,
        client_listener: TcpListener,
        mut tx_replies: flashmap::WriteHandle<ClientId, mpsc::Sender<ApiReply>>,
        mut client_servant_handles: flashmap::WriteHandle<
            ClientId,
            JoinHandle<()>,
        >,
    ) {
        pf_debug!(me; "client_acceptor thread spawned");

        loop {
            let accepted = client_listener.accept().await;
            if let Err(e) = accepted {
                pf_warn!(me; "error accepting client connection: {}", e);
                continue;
            }
            let (mut stream, addr) = accepted.unwrap();

            let id = stream.read_u64().await; // receive client ID
            if let Err(e) = id {
                pf_error!(me; "error receiving new client ID: {}", e);
                continue;
            }
            let id = id.unwrap();

            let mut tx_replies_guard = tx_replies.guard();
            if tx_replies_guard.contains_key(&id) {
                pf_error!(me; "duplicate client ID listened: {}", id);
                continue;
            }
            pf_info!(me; "accepted new client {}", id);

            let (tx_reply, rx_reply) = mpsc::channel(chan_reply_cap);
            tx_replies_guard.insert(id, tx_reply);

            let client_servant_handle =
                tokio::spawn(Self::client_servant_thread(
                    me,
                    id,
                    addr,
                    stream,
                    tx_req.clone(),
                    rx_reply,
                ));
            let mut client_servant_handles_guard =
                client_servant_handles.guard();
            client_servant_handles_guard.insert(id, client_servant_handle);

            client_servant_handles_guard.publish();
            tx_replies_guard.publish();
        }

        // pf_debug!(me; "client_acceptor thread exitted");
    }
}

// ExternalApi client_servant thread implementation
impl ExternalApi {
    /// Reads a client request from given TcpStream.
    async fn read_req(
        conn_read: &mut ReadHalf<'_>,
    ) -> Result<ApiRequest, SummersetError> {
        let req_len = conn_read.read_u64().await?; // receive length first
        let mut req_buf: Vec<u8> = vec![0; req_len as usize];
        conn_read.read_exact(&mut req_buf[..]).await?;
        let req = decode_from_slice(&req_buf)?;
        Ok(req)
    }

    /// Writes a reply through given TcpStream.
    async fn write_reply(
        reply: &ApiReply,
        conn_write: &mut WriteHalf<'_>,
    ) -> Result<(), SummersetError> {
        let reply_bytes = encode_to_vec(reply)?;
        conn_write.write_u64(reply_bytes.len() as u64).await?; // send length first
        conn_write.write_all(&reply_bytes[..]).await?;
        Ok(())
    }

    /// Client request listener and reply sender thread function.
    async fn client_servant_thread(
        me: ReplicaId,
        id: ClientId,
        addr: SocketAddr,
        mut conn: TcpStream,
        tx_req: mpsc::Sender<(ClientId, ApiRequest)>,
        mut rx_reply: mpsc::Receiver<ApiReply>,
    ) {
        pf_debug!(me; "client_servant thread for {} ({}) spawned", id, addr);

        let (mut conn_read, mut conn_write) = conn.split();

        loop {
            tokio::select! {
                // select between getting a new reply to send back and receiving
                // new client request, prioritizing the former
                biased;

                // gets a reply to send back
                reply = rx_reply.recv() => {
                    match reply {
                        Some(reply) => {
                            if let Err(e) = Self::write_reply(&reply, &mut conn_write).await {
                                pf_error!(me; "error replying to {}: {}", id, e);
                            } else {
                                pf_trace!(me; "replied to {} reply {:?}", id, reply);
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // receives client request
                req = Self::read_req(&mut conn_read) => {
                    match req {
                        Ok(req) => {
                            pf_trace!(me; "request from {} req {:?}", id, req);
                            if let Err(e) = tx_req.send((id, req)).await {
                                pf_error!(
                                    me; "error sending to tx_req for {}: {}", id, e
                                );
                            }
                        },
                        Err(e) => {
                            pf_error!(me; "error reading request from {}: {}", id, e);
                        }
                    }
                },
            }
        }

        pf_debug!(me; "client_servant thread for {} ({}) exitted", id, addr);
    }
}

// ExternalApi batch_ticker thread implementation
impl ExternalApi {
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
            pf_debug!(me; "batch interval ticked");
        }
    }
}

#[cfg(test)]
mod external_tests {
    use super::*;
    use std::collections::VecDeque;
    use crate::server::{Command, CommandResult};
    use crate::client::{ClientId, ClientApiStub};
    use rand::Rng;
    use tokio::sync::Barrier;
    use tokio::time::Duration;

    #[test]
    fn api_setup() -> Result<(), SummersetError> {
        let mut api = ExternalApi::new(0);
        assert!(tokio_test::block_on(api.setup(
            "127.0.0.1:52700".parse()?,
            Duration::from_millis(1),
            0,
            0
        ))
        .is_err());
        assert!(tokio_test::block_on(api.setup(
            "127.0.0.1:52700".parse()?,
            Duration::from_nanos(10),
            100,
            100,
        ))
        .is_err());
        tokio_test::block_on(api.setup(
            "127.0.0.1:52700".parse()?,
            Duration::from_millis(1),
            100,
            100,
        ))?;
        assert!(api.rx_req.is_some());
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
            let mut api = ExternalApi::new(0);
            api.setup(
                "127.0.0.1:53700".parse()?,
                Duration::from_millis(1),
                5,
                5,
            )
            .await?;
            barrier2.wait().await;
            let mut reqs: VecDeque<(ClientId, ApiRequest)> = VecDeque::new();
            while reqs.len() < 2 {
                let mut req_batch = api.get_req_batch().await?;
                reqs.append(&mut req_batch);
            }
            let client = reqs[0].0;
            assert_eq!(
                reqs.pop_front().unwrap().1,
                ApiRequest {
                    id: 0,
                    cmd: Command::Put {
                        key: "Jose".into(),
                        value: "123".into(),
                    },
                }
            );
            assert_eq!(
                reqs.pop_front().unwrap().1,
                ApiRequest {
                    id: 1,
                    cmd: Command::Get { key: "Jose".into() },
                }
            );
            api.send_reply(
                ApiReply {
                    id: 0,
                    result: CommandResult::PutResult { old_value: None },
                },
                client,
            )
            .await?;
            api.send_reply(
                ApiReply {
                    id: 1,
                    result: CommandResult::GetResult {
                        value: Some("123".into()),
                    },
                },
                client,
            )
            .await?;
            Ok::<(), SummersetError>(())
        });
        // client-side
        let client: ClientId = rand::thread_rng().gen();
        tokio_test::block_on(barrier.wait());
        let api_stub = ClientApiStub::new(client);
        let (mut send_stub, mut recv_stub) =
            tokio_test::block_on(api_stub.connect("127.0.0.1:53700".parse()?))?;
        tokio_test::block_on(send_stub.send_req(ApiRequest {
            id: 0,
            cmd: Command::Put {
                key: "Jose".into(),
                value: "123".into(),
            },
        }))?;
        tokio_test::block_on(send_stub.send_req(ApiRequest {
            id: 1,
            cmd: Command::Get { key: "Jose".into() },
        }))?;
        assert_eq!(
            tokio_test::block_on(recv_stub.recv_reply())?,
            ApiReply {
                id: 0,
                result: CommandResult::PutResult { old_value: None }
            }
        );
        assert_eq!(
            tokio_test::block_on(recv_stub.recv_reply())?,
            ApiReply {
                id: 1,
                result: CommandResult::GetResult {
                    value: Some("123".into())
                }
            }
        );
        Ok(())
    }
}
