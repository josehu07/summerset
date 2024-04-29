//! Cluster manager server-facing controller module implementation.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::utils::{
    SummersetError, safe_tcp_read, safe_tcp_write, tcp_bind_with_retry,
};
use crate::server::ReplicaId;
use crate::protocols::SmrProtocol;

use bytes::BytesMut;

use serde::{Serialize, Deserialize};

use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Control message from/to servers. Control traffic could be bidirectional:
/// some initiated by the manager and some by servers.
// TODO: later add basic lease, membership/view change, link drop, etc.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CtrlMsg {
    /// Server -> Manager: new server up, requesting a list of peers' addresses
    /// to connect to.
    NewServerJoin {
        id: ReplicaId,
        protocol: SmrProtocol,
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
    },

    /// Manager -> Server: assign a list of peers to proactively connect to.
    ConnectToPeers {
        population: u8,
        to_peers: HashMap<ReplicaId, SocketAddr>,
    },

    /// Server -> Manager: tell the manager that I steped-up/down as leader.
    LeaderStatus { step_up: bool },

    /// Manager -> Server: reset to initial state. If durable is false, cleans
    /// durable storage state as well.
    ResetState { durable: bool },

    /// Manager -> Server: pause server event loop execution.
    Pause,

    /// Server -> Manager: dummy pause reply.
    PauseReply,

    /// Manager -> Server: resume server event loop execution.
    Resume,

    /// Server -> Manager: dummy resume reply.
    ResumeReply,

    /// Manager -> Server: tell server to take a snapshot now.
    TakeSnapshot,

    /// Server -> Manager: server took snapshot up to log index.
    SnapshotUpTo { new_start: usize },

    /// Server -> Manager: leave notification.
    Leave,

    /// Manager -> Server: dummy leave reply.
    LeaveReply,
}

/// The server-facing controller API module.
pub struct ServerReigner {
    /// Receiver side of the recv channel.
    rx_recv: mpsc::UnboundedReceiver<(ReplicaId, CtrlMsg)>,

    /// Map from replica ID -> sender side of the send channel, shared with
    /// the server acceptor thread.
    tx_sends: flashmap::ReadHandle<ReplicaId, mpsc::UnboundedSender<CtrlMsg>>,

    /// Join handle of the server acceptor thread.
    _server_acceptor_handle: JoinHandle<()>,

    /// Map from replica ID -> replica controller thread join handles, shared
    /// with the server acceptor thread.
    _server_controller_handles: flashmap::ReadHandle<ReplicaId, JoinHandle<()>>,
}

// ServerReigner public API implementation
impl ServerReigner {
    /// Creates a new server-facing controller module. Spawns the server
    /// acceptor thread. Creates a pair of ID assignment channels. Creates
    /// a recv channel for buffering incoming control messages.
    pub async fn new_and_setup(
        srv_addr: SocketAddr,
        tx_id_assign: mpsc::UnboundedSender<()>,
        rx_id_result: mpsc::UnboundedReceiver<(ReplicaId, u8)>,
    ) -> Result<Self, SummersetError> {
        let (tx_recv, rx_recv) = mpsc::unbounded_channel();

        let (tx_sends_write, tx_sends_read) =
            flashmap::new::<ReplicaId, mpsc::UnboundedSender<CtrlMsg>>();

        let (server_controller_handles_write, server_controller_handles_read) =
            flashmap::new::<ReplicaId, JoinHandle<()>>();

        let server_listener = tcp_bind_with_retry(srv_addr, 10).await?;
        let server_acceptor_handle =
            tokio::spawn(Self::server_acceptor_thread(
                tx_id_assign,
                rx_id_result,
                tx_recv,
                server_listener,
                tx_sends_write,
                server_controller_handles_write,
            ));

        Ok(ServerReigner {
            rx_recv,
            tx_sends: tx_sends_read,
            _server_acceptor_handle: server_acceptor_handle,
            _server_controller_handles: server_controller_handles_read,
        })
    }

    /// Returns whether a server ID is connected to me.
    #[allow(dead_code)]
    pub fn has_server(&self, server: ReplicaId) -> bool {
        let tx_sends_guard = self.tx_sends.guard();
        tx_sends_guard.contains_key(&server)
    }

    /// Waits for the next control event message from some server.
    pub async fn recv_ctrl(
        &mut self,
    ) -> Result<(ReplicaId, CtrlMsg), SummersetError> {
        match self.rx_recv.recv().await {
            Some((id, msg)) => Ok((id, msg)),
            None => logged_err!("m"; "recv channel has been closed"),
        }
    }

    /// Sends a control message to specified server.
    pub fn send_ctrl(
        &mut self,
        msg: CtrlMsg,
        server: ReplicaId,
    ) -> Result<(), SummersetError> {
        let tx_sends_guard = self.tx_sends.guard();
        match tx_sends_guard.get(&server) {
            Some(tx_send) => {
                tx_send
                    .send(msg)
                    .map_err(|e| SummersetError(e.to_string()))?;
                Ok(())
            }
            None => {
                logged_err!(
                    "m";
                    "server ID {} not found among active servers",
                    server
                )
            }
        }
    }
}

// ServerReigner server_acceptor thread implementation
impl ServerReigner {
    /// Accepts a new server connection.
    #[allow(clippy::too_many_arguments)]
    async fn accept_new_server(
        mut stream: TcpStream,
        addr: SocketAddr,
        tx_id_assign: &mpsc::UnboundedSender<()>,
        rx_id_result: &mut mpsc::UnboundedReceiver<(ReplicaId, u8)>,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
        tx_sends: &mut flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<CtrlMsg>,
        >,
        server_controller_handles: &mut flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
        tx_exit: mpsc::UnboundedSender<ReplicaId>,
    ) -> Result<(), SummersetError> {
        // communicate with the manager's main thread to get assigned server ID
        tx_id_assign.send(())?;
        let (id, population) = rx_id_result.recv().await.ok_or(
            SummersetError("failed to get server ID assignment".into()),
        )?;

        // first send server ID assignment
        if let Err(e) = stream.write_u8(id).await {
            return logged_err!("m"; "error assigning new server ID: {}", e);
        }

        // then send population
        if let Err(e) = stream.write_u8(population).await {
            return logged_err!("m"; "error sending population: {}", e);
        }

        let mut tx_sends_guard = tx_sends.guard();
        if let Some(sender) = tx_sends_guard.get(&id) {
            if sender.is_closed() {
                // if this server ID has left before, garbage collect it now
                let mut server_controller_handles_guard =
                    server_controller_handles.guard();
                server_controller_handles_guard.remove(id);
                tx_sends_guard.remove(id);
            } else {
                return logged_err!("m"; "duplicate server ID listened: {}", id);
            }
        }
        pf_debug!("m"; "accepted new server {}", id);

        let (tx_send, rx_send) = mpsc::unbounded_channel();
        tx_sends_guard.insert(id, tx_send);

        let server_controller_handle =
            tokio::spawn(Self::server_controller_thread(
                id, addr, stream, tx_recv, rx_send, tx_exit,
            ));
        let mut server_controller_handles_guard =
            server_controller_handles.guard();
        server_controller_handles_guard.insert(id, server_controller_handle);

        server_controller_handles_guard.publish();
        tx_sends_guard.publish();
        Ok(())
    }

    /// Removes handles of a left server connection.
    fn remove_left_server(
        id: ReplicaId,
        tx_sends: &mut flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<CtrlMsg>,
        >,
        server_controller_handles: &mut flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
    ) -> Result<(), SummersetError> {
        let mut tx_sends_guard = tx_sends.guard();
        if !tx_sends_guard.contains_key(&id) {
            return logged_err!("m"; "server {} not found among active ones", id);
        }
        tx_sends_guard.remove(id);

        let mut server_controller_handles_guard =
            server_controller_handles.guard();
        server_controller_handles_guard.remove(id);

        Ok(())
    }

    /// Server acceptor thread function.
    async fn server_acceptor_thread(
        tx_id_assign: mpsc::UnboundedSender<()>,
        mut rx_id_result: mpsc::UnboundedReceiver<(ReplicaId, u8)>,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
        server_listener: TcpListener,
        mut tx_sends: flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<CtrlMsg>,
        >,
        mut server_controller_handles: flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
    ) {
        pf_debug!("m"; "server_acceptor thread spawned");

        let local_addr = server_listener.local_addr().unwrap();
        pf_info!("m"; "accepting servers on '{}'", local_addr);

        // create an exit mpsc channel for getting notified about termination
        // of server controller threads
        let (tx_exit, mut rx_exit) = mpsc::unbounded_channel();

        loop {
            tokio::select! {
                // new client connection
                accepted = server_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!("m"; "error accepting server connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = Self::accept_new_server(
                        stream,
                        addr,
                        &tx_id_assign,
                        &mut rx_id_result,
                        tx_recv.clone(),
                        &mut tx_sends,
                        &mut server_controller_handles,
                        tx_exit.clone(),
                    ).await {
                        pf_error!("m"; "error accepting new server: {}", e);
                    }
                },

                // a server controller thread exits
                id = rx_exit.recv() => {
                    let id = id.unwrap();
                    if let Err(e) = Self::remove_left_server(
                        id,
                        &mut tx_sends,
                        &mut server_controller_handles
                    ) {
                        pf_error!("m"; "error removing left server {}: {}", id, e);
                    }
                },
            }
        }

        // pf_debug!("m"; "server_acceptor thread exitted");
    }
}

// ServerReigner server_controller thread implementation
impl ServerReigner {
    /// Reads a server control message from given TcpStream.
    async fn read_ctrl(
        // first 8 btyes being the message length, and the rest bytes being the
        // message itself
        read_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<CtrlMsg, SummersetError> {
        safe_tcp_read(read_buf, conn_read).await
    }

    /// Writes a control message through given TcpStream.
    fn write_ctrl(
        write_buf: &mut BytesMut,
        write_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        msg: Option<&CtrlMsg>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(write_buf, write_buf_cursor, conn_write, msg)
    }

    /// Server control message listener and sender thread function.
    async fn server_controller_thread(
        id: ReplicaId,
        addr: SocketAddr,
        conn: TcpStream,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
        mut rx_send: mpsc::UnboundedReceiver<CtrlMsg>,
        tx_exit: mpsc::UnboundedSender<ReplicaId>,
    ) {
        pf_debug!("m"; "server_controller thread for {} '{}' spawned", id, addr);

        let (mut conn_read, conn_write) = conn.into_split();
        let mut read_buf = BytesMut::new();
        let mut write_buf = BytesMut::new();
        let mut write_buf_cursor = 0;

        let mut retrying = false;
        loop {
            tokio::select! {
                // gets a message to send to server
                msg = rx_send.recv(), if !retrying => {
                    match msg {
                        Some(msg) => {
                            match Self::write_ctrl(
                                &mut write_buf,
                                &mut write_buf_cursor,
                                &conn_write,
                                Some(&msg)
                            ) {
                                Ok(true) => {
                                    // pf_trace!("m"; "sent -> {} ctrl {:?}", id, msg);
                                }
                                Ok(false) => {
                                    pf_debug!("m"; "should start retrying ctrl send -> {}", id);
                                    retrying = true;
                                }
                                Err(_e) => {
                                    // NOTE: commented out to prevent console lags
                                    // during benchmarking
                                    // pf_error!("m"; "error sending -> {}: {}", id, e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // retrying last unsuccessful reply send
                _ = conn_write.writable(), if retrying => {
                    match Self::write_ctrl(
                        &mut write_buf,
                        &mut write_buf_cursor,
                        &conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!("m"; "finished retrying last ctrl send -> {}", id);
                            retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!("m"; "still should retry last ctrl send -> {}", id);
                        }
                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            // during benchmarking
                            // pf_error!("m"; "error retrying last ctrl send -> {}: {}", id, e);
                        }
                    }
                },

                // receives control message from server
                msg = Self::read_ctrl(&mut read_buf, &mut conn_read) => {
                    match msg {
                        Ok(CtrlMsg::Leave) => {
                            // server leaving, send dummy reply and break
                            let msg = CtrlMsg::LeaveReply;
                            if let Err(_e) = Self::write_ctrl(
                                &mut write_buf,
                                &mut write_buf_cursor,
                                &conn_write,
                                Some(&msg)
                            ) {
                                // NOTE: commented out to prevent console lags
                                // during benchmarking
                                // pf_error!("m"; "error replying -> {}: {}", id, e);
                            } else { // NOTE: skips `WouldBlock` error check here
                                pf_debug!("m"; "server {} has left", id);
                            }
                            break;
                        },

                        Ok(CtrlMsg::NewServerJoin {
                            id,
                            protocol,
                            mut api_addr,
                            mut p2p_addr
                        }) => {
                            // special treatment for NewServerJoin message:
                            // the server probably sent their local addresses
                            // for api_addr and p2p_addr fields. Fill them with
                            // the server's remote IP address known at the
                            // time of accepting connection to make them valid
                            // remote addresses
                            let conn_ip = conn_write.peer_addr().unwrap().ip();
                            api_addr.set_ip(conn_ip);
                            p2p_addr.set_ip(conn_ip);

                            let msg = CtrlMsg::NewServerJoin {
                                id,
                                protocol,
                                api_addr,
                                p2p_addr
                            };
                            // pf_trace!("m"; "recv <- {} ctrl {:?}", id, msg);
                            if let Err(e) = tx_recv.send((id, msg)) {
                                pf_error!("m";
                                          "error sending to tx_recv for {}: {}",
                                          id, e);
                            }
                        },

                        Ok(msg) => {
                            // pf_trace!("m"; "recv <- {} ctrl {:?}", id, msg);
                            if let Err(e) = tx_recv.send((id, msg)) {
                                pf_error!("m";
                                          "error sending to tx_recv for {}: {}",
                                          id, e);
                            }
                        },

                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            // during benchmarking
                            // pf_error!("m"; "error reading ctrl <- {}: {}", id, e);
                            break; // probably the server exitted ungracefully
                        }
                    }
                }
            }
        }

        if let Err(e) = tx_exit.send(id) {
            pf_error!("m"; "error sending exit signal for {}: {}", id, e);
        }
        pf_debug!("m"; "server_controller thread for {} '{}' exitted", id, addr);
    }
}

#[cfg(test)]
mod reigner_tests {
    use super::*;
    use std::sync::Arc;
    use crate::server::ControlHub;
    use tokio::sync::Barrier;
    use tokio::time::{self, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_send_recv() -> Result<(), SummersetError> {
        let setup_bar = Arc::new(Barrier::new(3));
        let setup_bar0 = setup_bar.clone();
        let setup_bar1 = setup_bar.clone();
        let server1_bar = Arc::new(Barrier::new(2));
        let server1_bar1 = server1_bar.clone();
        tokio::spawn(async move {
            // replica 0
            setup_bar0.wait().await;
            let mut hub = ControlHub::new_and_setup(
                "127.0.0.1:41109".parse()?,
                "127.0.0.1:40010".parse()?,
            )
            .await?;
            assert_eq!(hub.me, 0);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40110".parse()?,
                p2p_addr: "127.0.0.1:40210".parse()?,
            })?;
            // recv a message from manager
            assert_eq!(
                hub.recv_ctrl().await?,
                CtrlMsg::ConnectToPeers {
                    population: 2,
                    to_peers: HashMap::new(),
                }
            );
            server1_bar.wait().await;
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 1
            setup_bar1.wait().await;
            server1_bar1.wait().await;
            let mut hub = ControlHub::new_and_setup(
                "127.0.0.1:41119".parse()?,
                "127.0.0.1:40010".parse()?,
            )
            .await?;
            assert_eq!(hub.me, 1);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40111".parse()?,
                p2p_addr: "127.0.0.1:40211".parse()?,
            })?;
            // recv a message from manager
            assert_eq!(
                hub.recv_ctrl().await?,
                CtrlMsg::ConnectToPeers {
                    population: 2,
                    to_peers: HashMap::from([(0, "127.0.0.1:40210".parse()?)])
                }
            );
            Ok::<(), SummersetError>(())
        });
        // manager
        let (tx_id_assign, mut rx_id_assign) = mpsc::unbounded_channel();
        let (tx_id_result, rx_id_result) = mpsc::unbounded_channel();
        let mut reigner = ServerReigner::new_and_setup(
            "127.0.0.1:40010".parse()?,
            tx_id_assign,
            rx_id_result,
        )
        .await?;
        setup_bar.wait().await;
        // recv message from server 0
        rx_id_assign.recv().await;
        tx_id_result.send((0, 2))?;
        let (id, msg) = reigner.recv_ctrl().await?;
        assert_eq!(id, 0);
        assert_eq!(
            msg,
            CtrlMsg::NewServerJoin {
                id: 0,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40110".parse()?,
                p2p_addr: "127.0.0.1:40210".parse()?
            }
        );
        // send reply to server 0
        reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: 2,
                to_peers: HashMap::new(),
            },
            id,
        )?;
        // recv message from server 1
        rx_id_assign.recv().await;
        tx_id_result.send((1, 2))?;
        let (id, msg) = reigner.recv_ctrl().await?;
        assert_eq!(id, 1);
        assert_eq!(
            msg,
            CtrlMsg::NewServerJoin {
                id: 1,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40111".parse()?,
                p2p_addr: "127.0.0.1:40211".parse()?
            }
        );
        // send reply to server 1
        reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: 2,
                to_peers: HashMap::from([(0, "127.0.0.1:40210".parse()?)]),
            },
            id,
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_do_sync() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        let term_bar = Arc::new(Barrier::new(2));
        let term_bar2 = term_bar.clone();
        tokio::spawn(async move {
            // replica
            barrier2.wait().await;
            let mut hub = ControlHub::new_and_setup(
                "127.0.0.1:41209".parse()?,
                "127.0.0.1:40020".parse()?,
            )
            .await?;
            assert_eq!(hub.me, 0);
            // send a message to manager without waiting for its reply
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40120".parse()?,
                p2p_addr: "127.0.0.1:40220".parse()?,
            })?;
            // send a message to manager and wait for reply blockingly
            assert_eq!(
                hub.do_sync_ctrl(CtrlMsg::Leave, |m| m == &CtrlMsg::LeaveReply)
                    .await?,
                CtrlMsg::LeaveReply
            );
            term_bar.wait().await;
            Ok::<(), SummersetError>(())
        });
        // manager
        let (tx_id_assign, mut rx_id_assign) = mpsc::unbounded_channel();
        let (tx_id_result, rx_id_result) = mpsc::unbounded_channel();
        let mut reigner = ServerReigner::new_and_setup(
            "127.0.0.1:40020".parse()?,
            tx_id_assign,
            rx_id_result,
        )
        .await?;
        barrier.wait().await;
        // recv first message from server
        rx_id_assign.recv().await;
        tx_id_result.send((0, 1))?;
        let (id, msg) = reigner.recv_ctrl().await?;
        assert_eq!(id, 0);
        assert_eq!(
            msg,
            CtrlMsg::NewServerJoin {
                id: 0,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40120".parse()?,
                p2p_addr: "127.0.0.1:40220".parse()?
            }
        );
        // send reply to server 0
        reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: 1,
                to_peers: HashMap::new(),
            },
            id,
        )?;
        // recv second message (which is a Leave) from server; reply is sent
        // directly from the controller thread's event loop
        term_bar2.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_server_leave() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // replica 0
            barrier2.wait().await;
            let mut hub = ControlHub::new_and_setup(
                "127.0.0.1:41309".parse()?,
                "127.0.0.1:40030".parse()?,
            )
            .await?;
            assert_eq!(hub.me, 0);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40130".parse()?,
                p2p_addr: "127.0.0.1:40230".parse()?,
            })?;
            // recv a message from manager
            assert_eq!(
                hub.recv_ctrl().await?,
                CtrlMsg::ConnectToPeers {
                    population: 1,
                    to_peers: HashMap::new(),
                }
            );
            // leave and re-join as 0
            hub.send_ctrl(CtrlMsg::Leave)?;
            assert_eq!(hub.recv_ctrl().await?, CtrlMsg::LeaveReply);
            time::sleep(Duration::from_millis(100)).await;
            let mut hub = ControlHub::new_and_setup(
                "127.0.0.1:41319".parse()?,
                "127.0.0.1:40030".parse()?,
            )
            .await?;
            assert_eq!(hub.me, 0);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40130".parse()?,
                p2p_addr: "127.0.0.1:40230".parse()?,
            })?;
            // recv a message from manager
            assert_eq!(
                hub.recv_ctrl().await?,
                CtrlMsg::ConnectToPeers {
                    population: 1,
                    to_peers: HashMap::new(),
                }
            );
            Ok::<(), SummersetError>(())
        });
        // manager
        let (tx_id_assign, mut rx_id_assign) = mpsc::unbounded_channel();
        let (tx_id_result, rx_id_result) = mpsc::unbounded_channel();
        let mut reigner = ServerReigner::new_and_setup(
            "127.0.0.1:40030".parse()?,
            tx_id_assign,
            rx_id_result,
        )
        .await?;
        barrier.wait().await;
        // recv message from server 0
        rx_id_assign.recv().await;
        tx_id_result.send((0, 1))?;
        let (id, msg) = reigner.recv_ctrl().await?;
        assert_eq!(id, 0);
        assert_eq!(
            msg,
            CtrlMsg::NewServerJoin {
                id: 0,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40130".parse()?,
                p2p_addr: "127.0.0.1:40230".parse()?
            }
        );
        // send reply to server 0
        reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: 1,
                to_peers: HashMap::new(),
            },
            id,
        )?;
        rx_id_assign.recv().await;
        tx_id_result.send((0, 1))?;
        // recv message from server 0
        let (id, msg) = reigner.recv_ctrl().await?;
        assert_eq!(id, 0);
        assert_eq!(
            msg,
            CtrlMsg::NewServerJoin {
                id: 0,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:40130".parse()?,
                p2p_addr: "127.0.0.1:40230".parse()?
            }
        );
        // send reply to server 0
        reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: 1,
                to_peers: HashMap::new(),
            },
            id,
        )?;
        Ok(())
    }
}
