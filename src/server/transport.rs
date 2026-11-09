//! Summerset server internal TCP transport module implementation.
//!
//! NOTE: In concept, all messages are sent through unstable communication
//! channels, and are retried if the sender did not receive an ACK in a timely
//! manner. Here, we use TCP as the communication protocol to get the same
//! effect of "every message a sender wants to send will be retried until
//! eventually delivered".

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use get_size::GetSize;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};
use tokio::time::{self, Duration};
use wincode::{SchemaRead, SchemaWrite};

use crate::server::{LeaseGid, LeaseMsg, LeaseNotice, LeaseNum, ReplicaId};
use crate::utils::{
    Bitmap, Codec, HANDSHAKE_TIMEOUT, SummersetError, faults, safe_tcp_read, safe_tcp_write,
    tcp_bind_with_retry, tcp_connect_with_retry,
};

/// Peer-peer message wrapper type that includes leave notification variants.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, SchemaWrite, Deserialize, SchemaRead, GetSize)]
enum PeerMessage<Msg> {
    /// Normal protocol-specific message.
    Msg { msg: Msg },

    /// Lease-related message.
    LeaseMsg {
        /// Lease group ID that differentiates between lease groups for
        /// different lease purposes.
        lease_gid: LeaseGid,
        lease_num: LeaseNum,
        lease_msg: LeaseMsg,
    },

    /// Server leave notification.
    Leave,

    /// Reply to leave notification.
    LeaveReply,
}

/// Server internal TCP transport module.
pub(crate) struct TransportHub<Msg> {
    /// My replica ID.
    me: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Receiver side of the recv channel.
    rx_recv: mpsc::UnboundedReceiver<(ReplicaId, PeerMessage<Msg>)>,

    /// Map from peer ID -> sender side of the send channel, shared with the
    /// peer acceptor task.
    tx_sends: Arc<DashMap<ReplicaId, mpsc::UnboundedSender<PeerMessage<Msg>>>>,

    /// Join handle of the peer acceptor task.
    _peer_acceptor_handle: JoinHandle<()>,

    /// Sender side of the connect channel, used when proactively connecting
    /// to some peer.
    tx_connect: mpsc::UnboundedSender<(ReplicaId, SocketAddr)>,

    /// Receiver side of the connack channel, used when proactively connecting
    /// to some peer, carrying the result of connecting.
    rx_connack: mpsc::UnboundedReceiver<(ReplicaId, Result<(), SummersetError>)>,

    /// Map from peer ID -> peer messenger task join handles, shared with
    /// the peer acceptor task.
    _peer_messenger_handles: Arc<DashMap<ReplicaId, JoinHandle<()>>>,
}

// TransportHub public API implementation
impl<Msg> TransportHub<Msg>
where
    Msg:
        fmt::Debug + Clone + Serialize + DeserializeOwned + Codec + GetSize + Send + Sync + 'static,
{
    /// Creates a new server internal TCP transport hub. Spawns the peer
    /// acceptor task. Creates a recv channel for listening on peers'
    /// messages.
    pub(crate) async fn new_and_setup(
        me: ReplicaId,
        population: u8,
        p2p_addr: SocketAddr,
        // if non-null, a shortcut channel to feed lease messages directly in:
        tx_leases: HashMap<LeaseGid, mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,
    ) -> Result<Self, SummersetError> {
        if population <= me {
            return logged_err!("invalid population {}", population);
        }

        let (tx_recv, rx_recv) = mpsc::unbounded_channel::<(ReplicaId, PeerMessage<Msg>)>();

        let tx_sends: Arc<DashMap<ReplicaId, mpsc::UnboundedSender<PeerMessage<Msg>>>> =
            Arc::new(DashMap::new());

        let peer_messenger_handles: Arc<DashMap<ReplicaId, JoinHandle<()>>> =
            Arc::new(DashMap::new());

        // the connect & connack channels are used to notify the peer acceptor
        // task to proactively connect to some peer
        let (tx_connect, rx_connect) = mpsc::unbounded_channel();
        let (tx_connack, rx_connack) = mpsc::unbounded_channel();

        let peer_listener = tcp_bind_with_retry(p2p_addr, 15).await?;
        let mut acceptor = TransportHubAcceptorTask::new(
            me,
            tx_recv.clone(),
            peer_listener,
            tx_sends.clone(),
            peer_messenger_handles.clone(),
            rx_connect,
            tx_connack,
            tx_leases,
        );
        let peer_acceptor_handle = tokio::spawn(async move { acceptor.run().await });

        Ok(TransportHub {
            me,
            population,
            rx_recv,
            tx_sends,
            _peer_acceptor_handle: peer_acceptor_handle,
            tx_connect,
            rx_connack,
            _peer_messenger_handles: peer_messenger_handles,
        })
    }

    /// Connects to a peer replica proactively, and spawns the corresponding
    /// messenger task.
    pub(crate) async fn connect_to_peer(
        &mut self,
        id: ReplicaId,
        peer_addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        self.tx_connect.send((id, peer_addr))?;
        match self.rx_connack.recv().await {
            Some((ack_id, result)) if ack_id == id => result,
            Some((ack_id, _)) => {
                logged_err!("peer ID mismatch: expected {}, got {}", id, ack_id)
            }
            None => logged_err!("connack channel closed"),
        }
    }

    /// Waits for at least enough number of peers have been connected to me to
    /// form a group of specified size.
    pub(crate) async fn wait_for_group(&self, group: u8) -> Result<(), SummersetError> {
        if group == 0 {
            logged_err!("invalid group size {}", group)
        } else {
            while self.current_peers()?.count() + 1 < group {
                time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        }
    }

    /// Gets a bitmap where currently connected peers are set true.
    pub(crate) fn current_peers(&self) -> Result<Bitmap, SummersetError> {
        let mut peers = Bitmap::new(self.population, false);
        for entry in self.tx_sends.iter() {
            let id = *entry.key();
            if let Err(e) = peers.set(id, true) {
                return logged_err!("error setting peer {}: {}", id, e);
            }
        }
        Ok(peers)
    }

    /// Sends a message into a peer's send channel, subject to injected faults
    /// that may drop, duplicate, or delay it.
    fn send_faulty(
        tx_send: &mpsc::UnboundedSender<PeerMessage<Msg>>,
        msg: PeerMessage<Msg>,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        let Some(delays) = faults::msg_copies_delays(peer) else {
            return tx_send.send(msg).map_err(SummersetError::msg);
        };
        for delay in delays {
            let (tx_send, msg) = (tx_send.clone(), msg.clone());
            if delay.is_zero() {
                tx_send.send(msg).map_err(SummersetError::msg)?;
            } else {
                tokio::spawn(async move {
                    time::sleep(delay).await;
                    let _ = tx_send.send(msg);
                });
            }
        }
        Ok(())
    }

    /// Sends a message to a specified peer by sending to the send channel.
    fn send_msg_inner(
        &mut self,
        msg: PeerMessage<Msg>,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        if let Some(tx_send) = self.tx_sends.get(&peer) {
            Self::send_faulty(tx_send.value(), msg, peer)?;
        } else {
            // NOTE: commented out to avoid spurious error messages
            // pf_error!(
            //     "peer ID {} not found among connected ones",
            //     peer
            // );
        }

        Ok(())
    }

    /// Broadcasts message to specified peers by sending to the send channel.
    /// If `target` is `None`, broadcast to all current peers.
    #[allow(clippy::needless_pass_by_value)]
    fn bcast_msg_inner(
        &mut self,
        msg: PeerMessage<Msg>,
        target: Option<Bitmap>,
    ) -> Result<(), SummersetError> {
        let (mut attempted, mut failed) = (0, 0);
        for entry in self.tx_sends.iter() {
            let peer = *entry.key();
            if peer == self.me {
                continue;
            }
            if let Some(ref target) = target
                && (peer >= target.size() || !target.get(peer)?)
            {
                continue;
            }

            // not skipped; a closed channel means this peer's connection is
            // being torn down, which must not fail the rest of the broadcast
            attempted += 1;
            if Self::send_faulty(self.tx_sends.get(&peer).unwrap().value(), msg.clone(), peer)
                .is_err()
            {
                pf_debug!("peer {} connection closing, msg dropped", peer);
                failed += 1;
            }
        }

        // error if the broadcast reached none of the target peers
        if attempted > 0 && failed == attempted {
            return Err(SummersetError::msg(format!(
                "broadcast failed on all {} target peers",
                attempted
            )));
        }
        Ok(())
    }

    /// Sends a message to a specified peer by sending to the send channel.
    pub(crate) fn send_msg(&mut self, msg: Msg, peer: ReplicaId) -> Result<(), SummersetError> {
        self.send_msg_inner(PeerMessage::Msg { msg }, peer)
    }

    /// Broadcasts message to specified peers by sending to the send channel.
    /// If `target` is `None`, broadcast to all current peers.
    pub(crate) fn bcast_msg(
        &mut self,
        msg: Msg,
        target: Option<Bitmap>,
    ) -> Result<(), SummersetError> {
        self.bcast_msg_inner(PeerMessage::Msg { msg }, target)
    }

    /// Sends a lease-related message to a specified peer by sending to the
    /// send channel.
    pub(crate) fn send_lease_msg(
        &mut self,
        lease_gid: LeaseGid,
        lease_num: LeaseNum,
        lease_msg: LeaseMsg,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        self.send_msg_inner(
            PeerMessage::LeaseMsg {
                lease_gid,
                lease_num,
                lease_msg,
            },
            peer,
        )
    }

    /// Broadcasts lease-related message to specified peers by sending to the
    /// send channel. If `target` is `None`, broadcast to all current peers.
    pub(crate) fn bcast_lease_msg(
        &mut self,
        lease_gid: LeaseGid,
        lease_num: LeaseNum,
        lease_msg: LeaseMsg,
        target: Option<Bitmap>,
    ) -> Result<(), SummersetError> {
        self.bcast_msg_inner(
            PeerMessage::LeaseMsg {
                lease_gid,
                lease_num,
                lease_msg,
            },
            target,
        )
    }

    /// Receives a message from some peer by receiving from the recv channel.
    /// Returns a pair of `(peer_id, msg)` on success.
    pub(crate) async fn recv_msg(&mut self) -> Result<(ReplicaId, Msg), SummersetError> {
        loop {
            match self.rx_recv.recv().await {
                Some((id, PeerMessage::Msg { msg })) => return Ok((id, msg)),
                // e.g., replies to my leave notifications, which are not
                // waited for
                Some((id, _)) => pf_debug!("ignoring non-Msg peer message <- {}", id),
                None => return logged_err!("recv channel has been closed"),
            }
        }
    }

    /// Try to receive the next message using `try_recv()`.
    #[allow(dead_code)]
    pub(crate) fn try_recv_msg(&mut self) -> Result<(ReplicaId, Msg), SummersetError> {
        match self.rx_recv.try_recv() {
            Ok((id, peer_msg)) => {
                if let PeerMessage::Msg { msg } = peer_msg {
                    Ok((id, msg))
                } else {
                    logged_err!("unexpected peer message type")
                }
            }
            Err(e) => Err(SummersetError::msg(e)),
        }
    }

    /// Broadcasts leave notifications to all peers and waits for replies.
    #[allow(unused_assignments, clippy::unused_async)]
    pub(crate) async fn leave(&mut self) -> Result<(), SummersetError> {
        if faults::take_crash_leave() {
            return Ok(()); // a crashing server leaves without notice
        }

        #[allow(unused_variables)]
        let mut num_peers = 0;
        for entry in self.tx_sends.iter() {
            if *entry.key() == self.me {
                continue;
            }

            // not skipped; sends via the entry itself, as looking up the map
            // again while iterating it may deadlock
            entry
                .value()
                .send(PeerMessage::Leave)
                .map_err(SummersetError::msg)?;
            num_peers += 1;
        }

        // NOTE: commenting out the following to avoid rare blocking during
        //       tester resets
        // let mut replies = Bitmap::new(self.population, false);
        // while replies.count() < num_peers {
        //     match self.rx_recv.recv().await {
        //         Some((id, peer_msg)) => match peer_msg {
        //             PeerMessage::LeaveReply => replies.set(id, true)?,
        //             _ => continue, // ignore all other types of messages
        //         },
        //         None => {
        //             return logged_err!("recv channel has been closed");
        //         }
        //     }
        // }

        Ok(())
    }
}

/// `TransportHub` peer acceptor task.
struct TransportHubAcceptorTask<Msg> {
    me: ReplicaId,

    tx_recv: mpsc::UnboundedSender<(ReplicaId, PeerMessage<Msg>)>,
    tx_sends: Arc<DashMap<ReplicaId, mpsc::UnboundedSender<PeerMessage<Msg>>>>,

    peer_listener: TcpListener,
    peer_messenger_handles: Arc<DashMap<ReplicaId, JoinHandle<()>>>,

    rx_connect: mpsc::UnboundedReceiver<(ReplicaId, SocketAddr)>,
    tx_connack: mpsc::UnboundedSender<(ReplicaId, Result<(), SummersetError>)>,

    tx_leases: HashMap<LeaseGid, mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,

    tx_exit: mpsc::UnboundedSender<(ReplicaId, task::Id)>,
    rx_exit: mpsc::UnboundedReceiver<(ReplicaId, task::Id)>,
}

impl<Msg> TransportHubAcceptorTask<Msg>
where
    Msg: fmt::Debug + Clone + Serialize + DeserializeOwned + Codec + Send + Sync + 'static,
{
    /// Creates the peer acceptor task.
    #[allow(clippy::too_many_arguments)]
    fn new(
        me: ReplicaId,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, PeerMessage<Msg>)>,
        peer_listener: TcpListener,
        tx_sends: Arc<DashMap<ReplicaId, mpsc::UnboundedSender<PeerMessage<Msg>>>>,
        peer_messenger_handles: Arc<DashMap<ReplicaId, JoinHandle<()>>>,
        rx_connect: mpsc::UnboundedReceiver<(ReplicaId, SocketAddr)>,
        tx_connack: mpsc::UnboundedSender<(ReplicaId, Result<(), SummersetError>)>,
        tx_leases: HashMap<LeaseGid, mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,
    ) -> Self {
        // create an exit mpsc channel for getting notified about termination
        // of peer messenger tasks
        let (tx_exit, rx_exit) = mpsc::unbounded_channel();

        TransportHubAcceptorTask {
            me,
            tx_recv,
            tx_sends,
            peer_listener,
            peer_messenger_handles,
            rx_connect,
            tx_connack,
            tx_leases,
            tx_exit,
            rx_exit,
        }
    }

    /// Connects to a peer proactively.
    async fn connect_new_peer(
        &mut self,
        id: ReplicaId,
        conn_addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        if self.peer_messenger_handles.contains_key(&id) {
            return logged_err!("duplicate peer ID to connect: {}", id);
        }

        pf_debug!("connecting to peer {} '{}'...", id, conn_addr);
        let mut stream = tcp_connect_with_retry(conn_addr, 150).await?;
        stream.write_u8(self.me).await?; // send my ID

        let (tx_send, rx_send) = mpsc::unbounded_channel();
        self.tx_sends.insert(id, tx_send);

        let mut messenger = TransportHubMessengerTask::new(
            id,
            conn_addr,
            stream,
            rx_send,
            self.tx_recv.clone(),
            self.tx_leases.clone(),
            self.tx_exit.clone(),
        );
        let peer_messenger_handle = tokio::spawn(async move { messenger.run().await });
        self.peer_messenger_handles
            .insert(id, peer_messenger_handle);

        pf_debug!("connected to peer {}", id);
        Ok(())
    }

    /// Accepts a new peer connection.
    async fn accept_new_peer(
        &mut self,
        mut stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        let id = time::timeout(HANDSHAKE_TIMEOUT, stream.read_u8()).await?; // receive peer's ID
        if let Err(e) = id {
            return logged_err!("error receiving new peer ID: {}", e);
        }
        let id = id.unwrap();

        // a peer reconnecting under the same ID (e.g., restarted after a
        // crash) supersedes its previous connection, which may not have
        // noticed the crash yet
        if let Some((_, messenger)) = self.peer_messenger_handles.remove(&id) {
            messenger.abort();
        }

        let (tx_send, rx_send) = mpsc::unbounded_channel();
        self.tx_sends.insert(id, tx_send);

        let mut messenger = TransportHubMessengerTask::new(
            id,
            addr,
            stream,
            rx_send,
            self.tx_recv.clone(),
            self.tx_leases.clone(),
            self.tx_exit.clone(),
        );
        let peer_messenger_handle = tokio::spawn(async move { messenger.run().await });
        self.peer_messenger_handles
            .insert(id, peer_messenger_handle);

        pf_debug!("waited on peer {}", id);
        Ok(())
    }

    /// Removes handles of a left peer connection, unless a newer connection
    /// of the same peer has superseded it.
    fn remove_left_peer(&mut self, id: ReplicaId, messenger: task::Id) {
        if self
            .peer_messenger_handles
            .remove_if(&id, |_, handle| handle.id() == messenger)
            .is_some()
        {
            self.tx_sends.remove(&id);
        }
    }

    /// Starts the peer acceptor task loop.
    async fn run(&mut self) {
        pf_debug!("peer_acceptor task spawned");

        let local_addr = self.peer_listener.local_addr().unwrap();
        pf_info!("accepting peers on '{}'", local_addr);

        loop {
            tokio::select! {
                // proactive connection request
                to_connect = self.rx_connect.recv() => {
                    if to_connect.is_none() {
                        pf_error!("connect channel closed");
                        break; // channel gets closed and no messages remain
                    }
                    let (peer, conn_addr) = to_connect.unwrap();
                    // always acks with the result, so that the requester never
                    // hangs waiting
                    let result = self.connect_new_peer(peer, conn_addr).await;
                    if let Err(e) = self.tx_connack.send((peer, result)) {
                        pf_error!("error sending to tx_connack: {}", e);
                    }
                },

                // new peer connection accepted
                accepted = self.peer_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!("error accepting peer connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = self.accept_new_peer(
                        stream,
                        addr,
                    ).await {
                        pf_error!("error accepting new peer: {}", e);
                    }
                },

                // a peer messenger task exits
                exited = self.rx_exit.recv() => {
                    let (id, messenger) = exited.unwrap();
                    self.remove_left_peer(id, messenger);
                },
            }
        }

        // pf_debug!("peer_acceptor task exited");
    }
}

/// `TransportHub` per-peer messenger task.
struct TransportHubMessengerTask<Msg> {
    /// Corresponding peer's ID.
    id: ReplicaId,
    /// Corresponding peer's address.
    addr: SocketAddr,

    conn_read: OwnedReadHalf,
    conn_write: OwnedWriteHalf,

    rx_send: mpsc::UnboundedReceiver<PeerMessage<Msg>>,
    read_buf: BytesMut,

    tx_recv: mpsc::UnboundedSender<(ReplicaId, PeerMessage<Msg>)>,
    write_buf: BytesMut,
    write_buf_cursor: usize,
    retrying: bool,

    tx_leases: HashMap<LeaseGid, mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,

    tx_exit: mpsc::UnboundedSender<(ReplicaId, task::Id)>,
}

// TransportHub peer_messenger task implementation
impl<Msg> TransportHubMessengerTask<Msg>
where
    Msg: fmt::Debug + Clone + Serialize + DeserializeOwned + Codec + Send + Sync + 'static,
{
    /// Creates a per-peer messenger task.
    fn new(
        id: ReplicaId,
        addr: SocketAddr,
        conn: TcpStream,
        rx_send: mpsc::UnboundedReceiver<PeerMessage<Msg>>,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, PeerMessage<Msg>)>,
        tx_leases: HashMap<LeaseGid, mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,
        tx_exit: mpsc::UnboundedSender<(ReplicaId, task::Id)>,
    ) -> Self {
        let (conn_read, conn_write) = conn.into_split();

        let read_buf = BytesMut::with_capacity(8 + 1024);
        let write_buf = BytesMut::with_capacity(8 + 1024);
        let write_buf_cursor = 0;
        let retrying = false;

        TransportHubMessengerTask {
            id,
            addr,
            conn_read,
            conn_write,
            rx_send,
            read_buf,
            tx_recv,
            write_buf,
            write_buf_cursor,
            retrying,
            tx_leases,
            tx_exit,
        }
    }

    /// Writes a message through given `TcpStream`.
    /// This is a non-method function to ease `tokio::select!` sharing.
    fn write_msg(
        write_buf: &mut BytesMut,
        write_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        msg: Option<&PeerMessage<Msg>>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(write_buf, write_buf_cursor, conn_write, msg)
    }

    /// Reads a message from given `TcpStream`.
    /// This is a non-method function to ease `tokio::select!` sharing.
    async fn read_msg(
        // first 8 bytes being the message length, and the rest bytes being the
        // message itself
        read_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<PeerMessage<Msg>, SummersetError> {
        safe_tcp_read(read_buf, conn_read).await
    }

    /// Starts a per-peer messenger task loop.
    #[allow(clippy::too_many_lines)]
    async fn run(&mut self) {
        pf_debug!(
            "peer_messenger task for {} '{}' spawned",
            self.id,
            self.addr
        );

        loop {
            tokio::select! {
                // gets a message to send out
                msg = self.rx_send.recv(), if !self.retrying => {
                    match msg {
                        Some(PeerMessage::Leave) => {
                            // I decide to leave, notify peers
                            if let Err(_e) = Self::write_msg(
                                &mut self.write_buf,
                                &mut self.write_buf_cursor,
                                &self.conn_write,
                                Some(&PeerMessage::Leave),
                            ) {
                                // NOTE: commented out to prevent console lags
                                //       during benchmarking
                                // pf_error!("error sending -> {}: {}", id, e);
                            } else { // NOTE: skips `WouldBlock` error check here
                                pf_debug!("sent leave notification -> {}", self.id);
                            }
                        },

                        Some(PeerMessage::LeaveReply) => {
                            pf_error!("proactively sending LeaveReply msg");
                        },

                        Some(PeerMessage::LeaseMsg { .. } | PeerMessage::Msg { .. }) => {
                            match Self::write_msg(
                                &mut self.write_buf,
                                &mut self.write_buf_cursor,
                                &self.conn_write,
                                Some(msg.as_ref().unwrap()),
                            ) {
                                Ok(true) => {
                                    // pf_trace!("sent -> {} msg {:?}", id, msg);
                                }
                                Ok(false) => {
                                    pf_debug!("should start retrying msg send -> {}", self.id);
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

                // retrying last unsuccessful send
                _ = self.conn_write.writable(), if self.retrying => {
                    match Self::write_msg(
                        &mut self.write_buf,
                        &mut self.write_buf_cursor,
                        &self.conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!("finished retrying last msg send -> {}", self.id);
                            self.retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!("still should retry last msg send -> {}", self.id);
                        }
                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            //       during benchmarking
                            // pf_error!("error retrying last msg send -> {}: {}", id, e);
                        }
                    }
                },

                // receives new message from peer
                msg = Self::read_msg(&mut self.read_buf, &mut self.conn_read) => {
                    match msg {
                        Ok(PeerMessage::Leave) => {
                            // peer leaving, send dummy reply and break
                            if let Err(_e) = Self::write_msg(
                                &mut self.write_buf,
                                &mut self.write_buf_cursor,
                                &self.conn_write,
                                Some(&PeerMessage::LeaveReply),
                            ) {
                                // NOTE: commented out to prevent console lags
                                //       during benchmarking
                                // pf_error!("error sending -> {}: {}", id, e);
                            } else { // NOTE: skips `WouldBlock` error check here
                                pf_debug!("peer {} has left", self.id);
                            }
                            break;
                        },

                        Ok(PeerMessage::LeaveReply) => {
                            // my leave notification is acked by peer, break
                            if let Err(e) = self.tx_recv.send((self.id, PeerMessage::LeaveReply)) {
                                pf_error!("error sending to tx_recv for {}: {}", self.id, e);
                            }
                            break;
                        }

                        Ok(PeerMessage::LeaseMsg { lease_gid, lease_num, lease_msg }) => {
                            // pf_trace!("recv <- {} msg {:?}", id, msg);
                            if let Some(tx_lease) = self.tx_leases.get(&lease_gid) {
                                if let Err(e) = tx_lease.send((
                                    lease_num,
                                    LeaseNotice::RecvLeaseMsg { peer: self.id, msg: lease_msg }
                                )) {
                                    pf_error!(
                                        "error sending to tx_lease {} for {}: {}",
                                        lease_gid, self.id, e
                                    );
                                }
                            } else {
                                pf_error!(
                                    "received LeaseMsg <- {} but unknown gid {}",
                                    self.id, lease_gid
                                );
                            }
                        },

                        Ok(PeerMessage::Msg { .. }) => {
                            // pf_trace!("recv <- {} msg {:?}", id, msg);
                            if let Err(e) = self.tx_recv.send((self.id, msg.unwrap())) {
                                pf_error!("error sending to tx_recv for {}: {}", self.id, e);
                            }
                        },

                        Err(_e) => {
                            // NOTE: commented out to prevent console lags
                            //       during benchmarking
                            // pf_error!("error receiving msg <- {}: {}", id, e);
                            break; // probably the peer exited ungracefully
                        }
                    }
                }
            }
        }

        if let Err(e) = self.tx_exit.send((self.id, task::id())) {
            pf_error!("error sending exit signal for {}: {}", self.id, e);
        } else {
            // keep the send channel open (discarding messages to the departed
            // peer) until the hub removes its sender, so that sends to this
            // peer in the meantime do not fail with a closed channel
            while self.rx_send.recv().await.is_some() {}
        }
        pf_debug!("peer_messenger task for {} '{}' exited", self.id, self.addr);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use tokio::sync::Barrier;

    use super::*;
    use crate::utils::test_addr_at_offset;

    #[derive(
        Debug, Clone, PartialEq, Eq, Serialize, SchemaWrite, Deserialize, SchemaRead, GetSize,
    )]
    struct TestMsg(String);

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn api_send_recv() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(3));
        let barrier1 = barrier.clone();
        let barrier2 = barrier.clone();
        let replica1 = tokio::spawn(async move {
            // replica 1
            let (tx_lease, mut rx_lease) = mpsc::unbounded_channel();
            let mut hub: TransportHub<TestMsg> = TransportHub::new_and_setup(
                1,
                3,
                test_addr_at_offset(11),
                HashMap::from([(0, tx_lease)]),
            )
            .await?;
            barrier1.wait().await;
            hub.connect_to_peer(2, test_addr_at_offset(12)).await?;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert_eq!(id, 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            hub.send_msg(TestMsg("world".into()), 0)?;
            // recv another message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert_eq!(id, 0);
            assert_eq!(msg, TestMsg("nice".into()));
            // send another message to 0
            hub.send_msg(TestMsg("job!".into()), 0)?;
            // recv a lease message from 0
            let (num, msg) = rx_lease.recv().await.unwrap();
            assert_eq!(num, 7);
            assert_eq!(
                msg,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard { accept_bar: None }
                }
            );
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert_eq!(id, 0);
            assert_eq!(msg, TestMsg("terminate".into()));
            Ok::<(), SummersetError>(())
        });
        let replica2 = tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> =
                TransportHub::new_and_setup(2, 3, test_addr_at_offset(12), HashMap::new()).await?;
            barrier2.wait().await;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert_eq!(id, 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            hub.send_msg(TestMsg("world".into()), 0)?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert_eq!(id, 0);
            assert_eq!(msg, TestMsg("terminate".into()));
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> =
            TransportHub::new_and_setup(0, 3, test_addr_at_offset(10), HashMap::new()).await?;
        barrier.wait().await;
        hub.connect_to_peer(1, test_addr_at_offset(11)).await?;
        hub.connect_to_peer(2, test_addr_at_offset(12)).await?;
        // send a message to 1 and 2
        hub.bcast_msg(TestMsg("hello".into()), None)?;
        // recv a message from both 1 and 2
        let (id, msg) = hub.recv_msg().await?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("world".into()));
        let (id, msg) = hub.recv_msg().await?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("world".into()));
        // send another message to 1 only
        hub.send_msg(TestMsg("nice".into()), 1)?;
        // recv another message from 1
        let (id, msg) = hub.recv_msg().await?;
        assert_eq!(id, 1);
        assert_eq!(msg, TestMsg("job!".into()));
        // send a lease message to 1 only
        let mut map = Bitmap::new(3, false);
        map.set(1, true)?;
        hub.bcast_lease_msg(0, 7, LeaseMsg::Guard { accept_bar: None }, Some(map))?;
        // send termination message to 1 and 2
        hub.bcast_msg(TestMsg("terminate".into()), None)?;
        replica1.await??;
        replica2.await??;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_server_leave() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();
        let replica1 = tokio::spawn(async move {
            // replica 1/2
            let mut hub: TransportHub<TestMsg> =
                TransportHub::new_and_setup(1, 3, test_addr_at_offset(111), HashMap::new()).await?;
            barrier2.wait().await;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert_eq!(id, 0);
            assert!(hub.current_peers()?.get(id)?);
            assert_eq!(msg, TestMsg("goodbye".into()));
            // leave and come back as 2
            hub.leave().await?;
            time::sleep(Duration::from_millis(100)).await;
            let mut hub: TransportHub<TestMsg> =
                TransportHub::new_and_setup(2, 3, test_addr_at_offset(112), HashMap::new()).await?;
            hub.connect_to_peer(0, test_addr_at_offset(110)).await?;
            // send a message to 0
            hub.send_msg(TestMsg("hello".into()), 0)?;
            // stay connected until 0 has checked
            barrier2.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> =
            TransportHub::new_and_setup(0, 3, test_addr_at_offset(110), HashMap::new()).await?;
        barrier.wait().await;
        hub.connect_to_peer(1, test_addr_at_offset(111)).await?;
        assert!(hub.current_peers()?.get(1)?);
        assert!(!hub.current_peers()?.get(2)?);
        // send a message to 1
        hub.send_msg(TestMsg("goodbye".into()), 1)?;
        // recv a message from 2
        let (id, msg) = hub.recv_msg().await?;
        assert_eq!(id, 2);
        assert_eq!(msg, TestMsg("hello".into()));
        assert!(!hub.current_peers()?.get(1)?);
        assert!(hub.current_peers()?.get(2)?);
        barrier.wait().await;
        replica1.await??;
        Ok(())
    }

    #[tokio::test]
    async fn connect_failure_acked() -> Result<(), SummersetError> {
        let _hub1: TransportHub<TestMsg> =
            TransportHub::new_and_setup(1, 2, test_addr_at_offset(131), HashMap::new()).await?;
        let mut hub0: TransportHub<TestMsg> =
            TransportHub::new_and_setup(0, 2, test_addr_at_offset(130), HashMap::new()).await?;
        hub0.connect_to_peer(1, test_addr_at_offset(131)).await?;
        // connecting to the same peer again fails, instead of hanging
        let connect = hub0.connect_to_peer(1, test_addr_at_offset(131));
        let res = time::timeout(Duration::from_secs(5), connect).await;
        assert!(res.map_err(SummersetError::msg)?.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn peer_reconnect_supersedes() -> Result<(), SummersetError> {
        let mut hub0: TransportHub<TestMsg> =
            TransportHub::new_and_setup(0, 2, test_addr_at_offset(140), HashMap::new()).await?;
        let mut stale: TransportHub<TestMsg> =
            TransportHub::new_and_setup(1, 2, test_addr_at_offset(141), HashMap::new()).await?;
        stale.connect_to_peer(0, test_addr_at_offset(140)).await?;
        // peer 1 restarts while its previous connection is still open, and
        // the new connection takes over
        let mut hub1: TransportHub<TestMsg> =
            TransportHub::new_and_setup(1, 2, test_addr_at_offset(142), HashMap::new()).await?;
        hub1.connect_to_peer(0, test_addr_at_offset(140)).await?;
        hub1.send_msg(TestMsg("hello".into()), 0)?;
        let recv = time::timeout(Duration::from_secs(5), hub0.recv_msg()).await;
        assert_eq!(
            recv.map_err(SummersetError::msg)??,
            (1, TestMsg("hello".into()))
        );
        hub0.send_msg(TestMsg("world".into()), 1)?;
        let recv = time::timeout(Duration::from_secs(5), hub1.recv_msg()).await;
        assert_eq!(
            recv.map_err(SummersetError::msg)??,
            (0, TestMsg("world".into()))
        );
        assert!(stale.try_recv_msg().is_err()); // got nothing
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_handshake_times_out() -> Result<(), SummersetError> {
        let hub: TransportHub<TestMsg> =
            TransportHub::new_and_setup(0, 2, test_addr_at_offset(132), HashMap::new()).await?;
        // a connection that never sends its ID stalls accepting others only
        // until the handshake deadline
        let _stalled = TcpStream::connect(test_addr_at_offset(132)).await?;
        let mut peer = TcpStream::connect(test_addr_at_offset(132)).await?;
        peer.write_u8(1).await?;
        time::timeout(Duration::from_secs(60), hub.wait_for_group(2))
            .await
            .map_err(SummersetError::msg)??;
        Ok(())
    }
}
