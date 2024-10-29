//! Cluster manager server-facing controller module implementation.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::protocols::SmrProtocol;
use crate::server::ReplicaId;
use crate::utils::{
    safe_tcp_read, safe_tcp_write, tcp_bind_with_retry, SummersetError,
};

use bytes::BytesMut;

use serde::{Deserialize, Serialize};

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Control message from/to servers. Control traffic could be bidirectional:
/// some initiated by the manager and some by servers.
// TODO: later add basic lease, membership/view change, link drop, etc.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub(crate) enum CtrlMsg {
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
pub(crate) struct ServerReigner {
    /// Receiver side of the recv channel.
    rx_recv: mpsc::UnboundedReceiver<(ReplicaId, CtrlMsg)>,

    /// Map from replica ID -> sender side of the send channel, shared with
    /// the server acceptor task.
    tx_sends: flashmap::ReadHandle<ReplicaId, mpsc::UnboundedSender<CtrlMsg>>,

    /// Join handle of the server acceptor task.
    _server_acceptor_handle: JoinHandle<()>,

    /// Map from replica ID -> replica controller task join handles, shared
    /// with the server acceptor task.
    _server_controller_handles: flashmap::ReadHandle<ReplicaId, JoinHandle<()>>,
}

// ServerReigner public API implementation
impl ServerReigner {
    /// Creates a new server-facing controller module. Spawns the server
    /// acceptor task. Creates a pair of ID assignment channels. Creates
    /// a recv channel for buffering incoming control messages.
    pub(crate) async fn new_and_setup(
        srv_addr: SocketAddr,
        tx_id_assign: mpsc::UnboundedSender<()>,
        rx_id_result: mpsc::UnboundedReceiver<(ReplicaId, u8)>,
    ) -> Result<Self, SummersetError> {
        let (tx_recv, rx_recv) = mpsc::unbounded_channel();

        let (tx_sends_write, tx_sends_read) =
            flashmap::new::<ReplicaId, mpsc::UnboundedSender<CtrlMsg>>();

        let (server_controller_handles_write, server_controller_handles_read) =
            flashmap::new::<ReplicaId, JoinHandle<()>>();

        let server_listener = tcp_bind_with_retry(srv_addr, 15).await?;
        let mut acceptor = ServerReignerAcceptorTask::new(
            tx_id_assign,
            rx_id_result,
            tx_recv,
            tx_sends_write,
            server_listener,
            server_controller_handles_write,
        );
        let server_acceptor_handle =
            tokio::spawn(async move { acceptor.run().await });

        Ok(ServerReigner {
            rx_recv,
            tx_sends: tx_sends_read,
            _server_acceptor_handle: server_acceptor_handle,
            _server_controller_handles: server_controller_handles_read,
        })
    }

    /// Returns whether a server ID is connected to me.
    #[allow(dead_code)]
    pub(crate) fn has_server(&self, server: ReplicaId) -> bool {
        let tx_sends_guard = self.tx_sends.guard();
        tx_sends_guard.contains_key(&server)
    }

    /// Waits for the next control event message from some server.
    pub(crate) async fn recv_ctrl(
        &mut self,
    ) -> Result<(ReplicaId, CtrlMsg), SummersetError> {
        match self.rx_recv.recv().await {
            Some((id, msg)) => Ok((id, msg)),
            None => logged_err!("recv channel has been closed"),
        }
    }

    /// Sends a control message to specified server.
    pub(crate) fn send_ctrl(
        &mut self,
        msg: CtrlMsg,
        server: ReplicaId,
    ) -> Result<(), SummersetError> {
        let tx_sends_guard = self.tx_sends.guard();
        match tx_sends_guard.get(&server) {
            Some(tx_send) => {
                tx_send.send(msg).map_err(SummersetError::msg)?;
                Ok(())
            }
            None => {
                logged_err!(
                    "server ID {} not found among active servers",
                    server
                )
            }
        }
    }
}

/// ServerReigner server acceptor task.
struct ServerReignerAcceptorTask {
    tx_id_assign: mpsc::UnboundedSender<()>,
    rx_id_result: mpsc::UnboundedReceiver<(ReplicaId, u8)>,

    tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
    tx_sends: flashmap::WriteHandle<ReplicaId, mpsc::UnboundedSender<CtrlMsg>>,

    server_listener: TcpListener,
    server_controller_handles: flashmap::WriteHandle<ReplicaId, JoinHandle<()>>,

    tx_exit: mpsc::UnboundedSender<ReplicaId>,
    rx_exit: mpsc::UnboundedReceiver<ReplicaId>,
}

impl ServerReignerAcceptorTask {
    /// Creates the server acceptor task.
    fn new(
        tx_id_assign: mpsc::UnboundedSender<()>,
        rx_id_result: mpsc::UnboundedReceiver<(ReplicaId, u8)>,

        tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
        tx_sends: flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<CtrlMsg>,
        >,

        server_listener: TcpListener,
        server_controller_handles: flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
    ) -> Self {
        // create an exit mpsc channel for getting notified about termination
        // of server controller tasks
        let (tx_exit, rx_exit) = mpsc::unbounded_channel();

        ServerReignerAcceptorTask {
            tx_id_assign,
            rx_id_result,
            tx_recv,
            tx_sends,
            server_listener,
            server_controller_handles,
            tx_exit,
            rx_exit,
        }
    }

    /// Accepts a new server connection.
    async fn accept_new_server(
        &mut self,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        // communicate with the manager's main task to get assigned server ID
        self.tx_id_assign.send(())?;
        let (id, population) =
            self.rx_id_result.recv().await.ok_or(SummersetError::msg(
                "failed to get server ID assignment",
            ))?;

        // first send server ID assignment
        if let Err(e) = stream.write_u8(id).await {
            return logged_err!("error assigning new server ID: {}", e);
        }

        // then send population
        if let Err(e) = stream.write_u8(population).await {
            return logged_err!("error sending population: {}", e);
        }

        let mut tx_sends_guard = self.tx_sends.guard();
        if let Some(sender) = tx_sends_guard.get(&id) {
            if sender.is_closed() {
                // if this server ID has left before, garbage collect it now
                let mut server_controller_handles_guard =
                    self.server_controller_handles.guard();
                server_controller_handles_guard.remove(id);
                tx_sends_guard.remove(id);
            } else {
                return logged_err!("duplicate server ID listened: {}", id);
            }
        }
        pf_debug!("accepted new server {}", id);

        let (tx_send, rx_send) = mpsc::unbounded_channel();
        tx_sends_guard.insert(id, tx_send);

        let mut controller = ServerReignerControllerTask::new(
            id,
            addr,
            stream,
            self.tx_recv.clone(),
            rx_send,
            self.tx_exit.clone(),
        );
        let server_controller_handle =
            tokio::spawn(async move { controller.run().await });
        let mut server_controller_handles_guard =
            self.server_controller_handles.guard();
        server_controller_handles_guard.insert(id, server_controller_handle);

        server_controller_handles_guard.publish();
        tx_sends_guard.publish();
        Ok(())
    }

    /// Removes handles of a left server connection.
    fn remove_left_server(
        &mut self,
        id: ReplicaId,
    ) -> Result<(), SummersetError> {
        let mut tx_sends_guard = self.tx_sends.guard();
        if !tx_sends_guard.contains_key(&id) {
            return logged_err!("server {} not found among active ones", id);
        }
        tx_sends_guard.remove(id);

        let mut server_controller_handles_guard =
            self.server_controller_handles.guard();
        server_controller_handles_guard.remove(id);

        Ok(())
    }

    /// Starts the server acceptor task loop.
    async fn run(&mut self) {
        pf_debug!("server_acceptor task spawned");

        let local_addr = self.server_listener.local_addr().unwrap();
        pf_info!("accepting servers on '{}'", local_addr);

        loop {
            tokio::select! {
                // new client connection
                accepted = self.server_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!("error accepting server connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = self.accept_new_server(
                        stream,
                        addr,
                    ).await {
                        pf_error!("error accepting new server: {}", e);
                    }
                },

                // a server controller task exits
                id = self.rx_exit.recv() => {
                    let id = id.unwrap();
                    if let Err(e) = self.remove_left_server(id) {
                        pf_error!("error removing left server {}: {}", id, e);
                    }
                },
            }
        }

        // pf_debug!("server_acceptor task exited");
    }
}

/// ServerReigner per-server controller task.
struct ServerReignerControllerTask {
    id: ReplicaId,
    addr: SocketAddr,

    conn_read: OwnedReadHalf,
    conn_write: OwnedWriteHalf,

    tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
    read_buf: BytesMut,

    rx_send: mpsc::UnboundedReceiver<CtrlMsg>,
    write_buf: BytesMut,
    write_buf_cursor: usize,
    retrying: bool,

    tx_exit: mpsc::UnboundedSender<ReplicaId>,
}

impl ServerReignerControllerTask {
    /// Creates a per-server controller task.
    fn new(
        id: ReplicaId,
        addr: SocketAddr,
        conn: TcpStream,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, CtrlMsg)>,
        rx_send: mpsc::UnboundedReceiver<CtrlMsg>,
        tx_exit: mpsc::UnboundedSender<ReplicaId>,
    ) -> Self {
        let (conn_read, conn_write) = conn.into_split();

        let read_buf = BytesMut::new();
        let write_buf = BytesMut::new();
        let write_buf_cursor = 0;
        let retrying = false;

        ServerReignerControllerTask {
            id,
            addr,
            conn_read,
            conn_write,
            tx_recv,
            read_buf,
            rx_send,
            write_buf,
            write_buf_cursor,
            retrying,
            tx_exit,
        }
    }

    /// Reads a server control message from given TcpStream.
    /// This is a non-method function to ease `tokio::select!` sharing.
    async fn read_ctrl(
        // first 8 bytes being the message length, and the rest bytes being the
        // message itself
        read_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<CtrlMsg, SummersetError> {
        safe_tcp_read(read_buf, conn_read).await
    }

    /// Writes a control message through given TcpStream.
    /// This is a non-method function to ease `tokio::select!` sharing.
    fn write_ctrl(
        write_buf: &mut BytesMut,
        write_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        msg: Option<&CtrlMsg>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(write_buf, write_buf_cursor, conn_write, msg)
    }

    /// Starts a per-server controller task.
    async fn run(&mut self) {
        pf_debug!(
            "server_controller task for {} '{}' spawned",
            self.id,
            self.addr
        );

        loop {
            tokio::select! {
                // gets a message to send to server
                msg = self.rx_send.recv(), if !self.retrying => {
                    match msg {
                        Some(msg) => {
                            match Self::write_ctrl(
                                &mut self.write_buf,
                                &mut self.write_buf_cursor,
                                &self.conn_write,
                                Some(&msg)
                            ) {
                                Ok(true) => {
                                    // pf_trace!("sent -> {} ctrl {:?}", id, msg);
                                }
                                Ok(false) => {
                                    pf_debug!("should start retrying ctrl send -> {}", self.id);
                                    self.retrying = true;
                                }
                                Err(_e) => {
                                    // NOTE: commented out to prevent console lags
                                    //       during benchmarking
                                    // pf_error!("error sending -> {}: {}", id, e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // retrying last unsuccessful reply send
                _ = self.conn_write.writable(), if self.retrying => {
                    match Self::write_ctrl(
                        &mut self.write_buf,
                        &mut self.write_buf_cursor,
                        &self.conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!("finished retrying last ctrl send -> {}", self.id);
                            self.retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!("still should retry last ctrl send -> {}", self.id);
                        }
                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            //       during benchmarking
                            // pf_error!("error retrying last ctrl send -> {}: {}", id, e);
                        }
                    }
                },

                // receives control message from server
                msg = Self::read_ctrl(&mut self.read_buf, &mut self.conn_read) => {
                    match msg {
                        Ok(CtrlMsg::Leave) => {
                            // server leaving, send dummy reply and break
                            let msg = CtrlMsg::LeaveReply;
                            if let Err(_e) = Self::write_ctrl(
                                &mut self.write_buf,
                                &mut self.write_buf_cursor,
                                &self.conn_write,
                                Some(&msg)
                            ) {
                                // NOTE: commented out to prevent console lags
                                //       during benchmarking
                                // pf_error!("error replying -> {}: {}", id, e);
                            } else { // NOTE: skips `WouldBlock` error check here
                                pf_debug!("server {} has left", self.id);
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
                            let conn_ip = self.conn_write.peer_addr().unwrap().ip();
                            api_addr.set_ip(conn_ip);
                            p2p_addr.set_ip(conn_ip);

                            let msg = CtrlMsg::NewServerJoin {
                                id,
                                protocol,
                                api_addr,
                                p2p_addr
                            };
                            // pf_trace!("recv <- {} ctrl {:?}", id, msg);
                            if let Err(e) = self.tx_recv.send((id, msg)) {
                                pf_error!("error sending to tx_recv for {}: {}",
                                          id, e);
                            }
                        },

                        Ok(msg) => {
                            // pf_trace!("recv <- {} ctrl {:?}", id, msg);
                            if let Err(e) = self.tx_recv.send((self.id, msg)) {
                                pf_error!("error sending to tx_recv for {}: {}",
                                          self.id, e);
                            }
                        },

                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            //       during benchmarking
                            // pf_error!("error reading ctrl <- {}: {}", id, e);
                            break; // probably the server exited ungracefully
                        }
                    }
                }
            }
        }

        if let Err(e) = self.tx_exit.send(self.id) {
            pf_error!("error sending exit signal for {}: {}", self.id, e);
        }
        pf_debug!(
            "server_controller task for {} '{}' exited",
            self.id,
            self.addr
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ControlHub;
    use std::sync::Arc;
    use tokio::sync::Barrier;
    use tokio::time::{self, Duration};

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn api_send_recv() -> Result<(), SummersetError> {
        let setup_bar = Arc::new(Barrier::new(3));
        let setup_bar0 = setup_bar.clone();
        let setup_bar1 = setup_bar.clone();
        let server1_bar = Arc::new(Barrier::new(2));
        let server1_bar1 = server1_bar.clone();
        tokio::spawn(async move {
            // replica 0
            setup_bar0.wait().await;
            let mut hub =
                ControlHub::new_and_setup("127.0.0.1:30019".parse()?).await?;
            assert_eq!(hub.me, 0);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:30000".parse()?,
                p2p_addr: "127.0.0.1:30010".parse()?,
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
            let mut hub =
                ControlHub::new_and_setup("127.0.0.1:30019".parse()?).await?;
            assert_eq!(hub.me, 1);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:30001".parse()?,
                p2p_addr: "127.0.0.1:30011".parse()?,
            })?;
            // recv a message from manager
            assert_eq!(
                hub.recv_ctrl().await?,
                CtrlMsg::ConnectToPeers {
                    population: 2,
                    to_peers: HashMap::from([(0, "127.0.0.1:30010".parse()?)])
                }
            );
            Ok::<(), SummersetError>(())
        });
        // manager
        let (tx_id_assign, mut rx_id_assign) = mpsc::unbounded_channel();
        let (tx_id_result, rx_id_result) = mpsc::unbounded_channel();
        let mut reigner = ServerReigner::new_and_setup(
            "127.0.0.1:30019".parse()?,
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
                api_addr: "127.0.0.1:30000".parse()?,
                p2p_addr: "127.0.0.1:30010".parse()?
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
                api_addr: "127.0.0.1:30001".parse()?,
                p2p_addr: "127.0.0.1:30011".parse()?
            }
        );
        // send reply to server 1
        reigner.send_ctrl(
            CtrlMsg::ConnectToPeers {
                population: 2,
                to_peers: HashMap::from([(0, "127.0.0.1:30010".parse()?)]),
            },
            id,
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_do_sync() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // replica
            barrier2.wait().await;
            let mut hub =
                ControlHub::new_and_setup("127.0.0.1:30119".parse()?).await?;
            assert_eq!(hub.me, 0);
            // send a message to manager without waiting for its reply
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:30100".parse()?,
                p2p_addr: "127.0.0.1:30110".parse()?,
            })?;
            // send a leave to manager and wait for reply blockingly
            barrier2.wait().await;
            assert_eq!(
                hub.do_sync_ctrl(CtrlMsg::Leave, |m| m == &CtrlMsg::LeaveReply)
                    .await?,
                CtrlMsg::LeaveReply
            );
            barrier2.wait().await;
            Ok::<(), SummersetError>(())
        });
        // manager
        let (tx_id_assign, mut rx_id_assign) = mpsc::unbounded_channel();
        let (tx_id_result, rx_id_result) = mpsc::unbounded_channel();
        let mut reigner = ServerReigner::new_and_setup(
            "127.0.0.1:30119".parse()?,
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
                api_addr: "127.0.0.1:30100".parse()?,
                p2p_addr: "127.0.0.1:30110".parse()?
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
        // directly from the controller task's event loop
        barrier.wait().await;
        barrier.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_server_leave() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // replica 0
            barrier2.wait().await;
            let mut hub =
                ControlHub::new_and_setup("127.0.0.1:30219".parse()?).await?;
            assert_eq!(hub.me, 0);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:30200".parse()?,
                p2p_addr: "127.0.0.1:30210".parse()?,
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
            let mut hub =
                ControlHub::new_and_setup("127.0.0.1:30219".parse()?).await?;
            assert_eq!(hub.me, 0);
            // send a message to manager
            hub.send_ctrl(CtrlMsg::NewServerJoin {
                id: hub.me,
                protocol: SmrProtocol::SimplePush,
                api_addr: "127.0.0.1:30200".parse()?,
                p2p_addr: "127.0.0.1:30210".parse()?,
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
            "127.0.0.1:30219".parse()?,
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
                api_addr: "127.0.0.1:30200".parse()?,
                p2p_addr: "127.0.0.1:30210".parse()?
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
                api_addr: "127.0.0.1:30200".parse()?,
                p2p_addr: "127.0.0.1:30210".parse()?
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
