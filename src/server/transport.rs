//! Summerset server internal TCP transport module implementation.

use std::fmt;
use std::net::SocketAddr;

use crate::utils::{SummersetError, Bitmap, safe_tcp_read, safe_tcp_write};
use crate::server::ReplicaId;

use bytes::BytesMut;

use serde::{Serialize, de::DeserializeOwned};

use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Server internal TCP transport module.
pub struct TransportHub<Msg> {
    /// My replica ID.
    me: ReplicaId,

    /// Total number of replicas in cluster.
    population: u8,

    /// Receiver side of the recv channel.
    rx_recv: mpsc::UnboundedReceiver<(ReplicaId, Msg)>,

    /// Map from peer ID -> sender side of the send channel, shared with the
    /// peer acceptor thread.
    tx_sends: flashmap::ReadHandle<ReplicaId, mpsc::UnboundedSender<Msg>>,

    /// Join handle of the peer acceptor thread.
    _peer_acceptor_handle: JoinHandle<()>,

    /// Sender side of the connect channel, used when proactively connecting
    /// to some peer.
    tx_connect: mpsc::UnboundedSender<(ReplicaId, SocketAddr)>,

    /// Receiver side of the connack channel, used when proactively connecting
    /// to some peer.
    rx_connack: mpsc::UnboundedReceiver<ReplicaId>,

    /// Map from peer ID -> peer messenger thread join handles, shared with
    /// the peer acceptor thread.
    _peer_messenger_handles: flashmap::ReadHandle<ReplicaId, JoinHandle<()>>,
}

// TransportHub public API implementation
impl<Msg> TransportHub<Msg>
where
    Msg: fmt::Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    /// Creates a new server internal TCP transport hub. Spawns the peer
    /// acceptor thread. Creates a recv channel for listening on peers'
    /// messages.
    pub async fn new_and_setup(
        me: ReplicaId,
        population: u8,
        p2p_addr: SocketAddr,
    ) -> Result<Self, SummersetError> {
        if population <= me {
            return logged_err!(me; "invalid population {}", population);
        }

        let (tx_recv, rx_recv) = mpsc::unbounded_channel();

        let (tx_sends_write, tx_sends_read) =
            flashmap::new::<ReplicaId, mpsc::UnboundedSender<Msg>>();

        let (peer_messenger_handles_write, peer_messenger_handles_read) =
            flashmap::new::<ReplicaId, JoinHandle<()>>();

        // the connect & connack channel is used to notify the peer acceptor
        // thread to proactively connect to some peer
        let (tx_connect, rx_connect) = mpsc::unbounded_channel();
        let (tx_connack, rx_connack) = mpsc::unbounded_channel();

        let peer_listener = TcpListener::bind(p2p_addr).await?;
        let peer_acceptor_handle = tokio::spawn(Self::peer_acceptor_thread(
            me,
            tx_recv.clone(),
            peer_listener,
            tx_sends_write,
            peer_messenger_handles_write,
            rx_connect,
            tx_connack,
        ));

        Ok(TransportHub {
            me,
            population,
            rx_recv,
            tx_sends: tx_sends_read,
            _peer_acceptor_handle: peer_acceptor_handle,
            tx_connect,
            rx_connack,
            _peer_messenger_handles: peer_messenger_handles_read,
        })
    }

    /// Connects to a peer replica proactively, and spawns the corresponding
    /// messenger thread.
    pub async fn connect_to_peer(
        &mut self,
        id: ReplicaId,
        peer_addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        self.tx_connect.send((id, peer_addr))?;
        match self.rx_connack.recv().await {
            Some(ack_id) => {
                if ack_id != id {
                    logged_err!(self.me; "peer ID mismatch: expected {}, got {}",
                                         id, ack_id)
                } else {
                    Ok(())
                }
            }
            None => logged_err!(self.me; "connack channel closed"),
        }
    }

    /// Waits for at least enough number of peers have been connected to me to
    /// form a group of specified size.
    pub async fn wait_for_group(
        &self,
        group: u8,
    ) -> Result<(), SummersetError> {
        if group == 0 {
            logged_err!(self.me; "invalid group size {}", group)
        } else {
            while self.current_peers()?.count() + 1 < group {
                time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        }
    }

    /// Gets a bitmap where currently connected peers are set true.
    pub fn current_peers(&self) -> Result<Bitmap, SummersetError> {
        let tx_sends_guard = self.tx_sends.guard();
        let mut peers = Bitmap::new(self.population, false);
        for &id in tx_sends_guard.keys() {
            if let Err(e) = peers.set(id, true) {
                return logged_err!(self.me; "error setting peer {}: {}",
                                            id, e);
            }
        }
        Ok(peers)
    }

    /// Sends a message to a specified peer by sending to the send channel.
    pub fn send_msg(
        &mut self,
        msg: Msg,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        let tx_sends_guard = self.tx_sends.guard();
        match tx_sends_guard.get(&peer) {
            Some(tx_send) => {
                tx_send
                    .send(msg)
                    .map_err(|e| SummersetError(e.to_string()))?;
            }
            None => {
                pf_error!(
                    self.me;
                    "peer ID {} not found among connected ones",
                    peer
                );
            }
        }

        Ok(())
    }

    /// Broadcasts message to specified peers by sending to the send channel.
    /// If `target` is `None`, broadcast to all current peers.
    pub fn bcast_msg(
        &mut self,
        msg: Msg,
        target: Option<Bitmap>,
    ) -> Result<(), SummersetError> {
        let tx_sends_guard = self.tx_sends.guard();
        for &peer in tx_sends_guard.keys() {
            if peer == self.me {
                continue;
            }
            if let Some(ref target) = target {
                if peer >= target.size() || !target.get(peer)? {
                    continue;
                }
            }

            // not skipped
            tx_sends_guard
                .get(&peer)
                .unwrap()
                .send(msg.clone())
                .map_err(|e| SummersetError(e.to_string()))?;
        }

        Ok(())
    }

    /// Receives a message from some peer by receiving from the recv channel.
    /// Returns a pair of `(peer_id, msg)` on success.
    pub async fn recv_msg(
        &mut self,
    ) -> Result<(ReplicaId, Msg), SummersetError> {
        match self.rx_recv.recv().await {
            Some((id, msg)) => Ok((id, msg)),
            None => logged_err!(self.me; "recv channel has been closed"),
        }
    }
}

// TransportHub peer_acceptor thread implementation
impl<Msg> TransportHub<Msg>
where
    Msg: fmt::Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    /// Connects to a peer proactively.
    async fn connect_new_peer(
        me: ReplicaId,
        id: ReplicaId,
        addr: SocketAddr,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, Msg)>,
        tx_sends: &mut flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<Msg>,
        >,
        peer_messenger_handles: &mut flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
        tx_exit: mpsc::UnboundedSender<ReplicaId>,
    ) -> Result<(), SummersetError> {
        pf_debug!(me; "connecting to peer {} '{}'...", id, addr);
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(me).await?; // send my ID

        let mut peer_messenger_handles_guard = peer_messenger_handles.guard();
        if peer_messenger_handles_guard.contains_key(&id) {
            return logged_err!(me; "duplicate peer ID to connect: {}", id);
        }

        let mut tx_sends_guard = tx_sends.guard();
        let (tx_send, rx_send) = mpsc::unbounded_channel();
        tx_sends_guard.insert(id, tx_send);

        let peer_messenger_handle = tokio::spawn(Self::peer_messenger_thread(
            me, id, addr, stream, rx_send, tx_recv, tx_exit,
        ));
        peer_messenger_handles_guard.insert(id, peer_messenger_handle);

        pf_debug!(me; "connected to peer {}", id);
        Ok(())
    }

    /// Accepts a new peer connection.
    async fn accept_new_peer(
        me: ReplicaId,
        mut stream: TcpStream,
        addr: SocketAddr,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, Msg)>,
        tx_sends: &mut flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<Msg>,
        >,
        peer_messenger_handles: &mut flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
        tx_exit: mpsc::UnboundedSender<ReplicaId>,
    ) -> Result<(), SummersetError> {
        let id = stream.read_u8().await; // receive peer's ID
        if let Err(e) = id {
            return logged_err!(me; "error receiving new peer ID: {}", e);
        }
        let id = id.unwrap();

        let mut peer_messenger_handles_guard = peer_messenger_handles.guard();
        if peer_messenger_handles_guard.contains_key(&id) {
            return logged_err!(me; "duplicate peer ID listened: {}", id);
        }

        let mut tx_sends_guard = tx_sends.guard();
        let (tx_send, rx_send) = mpsc::unbounded_channel();
        tx_sends_guard.insert(id, tx_send);

        let peer_messenger_handle = tokio::spawn(Self::peer_messenger_thread(
            me, id, addr, stream, rx_send, tx_recv, tx_exit,
        ));
        peer_messenger_handles_guard.insert(id, peer_messenger_handle);

        pf_debug!(me; "waited on peer {}", id);
        Ok(())
    }

    /// Removes handles of a left peer connection.
    fn remove_left_peer(
        me: ReplicaId,
        id: ReplicaId,
        tx_sends: &mut flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<Msg>,
        >,
        peer_messenger_handles: &mut flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
    ) -> Result<(), SummersetError> {
        let mut tx_sends_guard = tx_sends.guard();
        if !tx_sends_guard.contains_key(&id) {
            return logged_err!(me; "peer {} not found among connected ones", id);
        }
        tx_sends_guard.remove(id);

        let mut peer_messenger_handles_guard = peer_messenger_handles.guard();
        peer_messenger_handles_guard.remove(id);

        Ok(())
    }

    /// Peer acceptor thread function.
    async fn peer_acceptor_thread(
        me: ReplicaId,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, Msg)>,
        peer_listener: TcpListener,
        mut tx_sends: flashmap::WriteHandle<
            ReplicaId,
            mpsc::UnboundedSender<Msg>,
        >,
        mut peer_messenger_handles: flashmap::WriteHandle<
            ReplicaId,
            JoinHandle<()>,
        >,
        mut rx_connect: mpsc::UnboundedReceiver<(ReplicaId, SocketAddr)>,
        tx_connack: mpsc::UnboundedSender<ReplicaId>,
    ) {
        pf_debug!(me; "peer_acceptor thread spawned");

        let local_addr = peer_listener.local_addr().unwrap();
        pf_info!(me; "accepting peers on '{}'", local_addr);

        // create an exit mpsc channel for getting notified about termination
        // of peer messenger threads
        let (tx_exit, mut rx_exit) = mpsc::unbounded_channel();

        loop {
            tokio::select! {
                // proactive connection request
                to_connect = rx_connect.recv() => {
                    if to_connect.is_none() {
                        pf_error!(me; "connect channel closed");
                        continue;
                    }
                    let (peer, addr) = to_connect.unwrap();
                    if let Err(e) = Self::connect_new_peer(
                        me,
                        peer,
                        addr,
                        tx_recv.clone(),
                        &mut tx_sends,
                        &mut peer_messenger_handles,
                        tx_exit.clone()
                    ).await {
                        pf_error!(me; "error connecting to new peer: {}", e);
                    } else if let Err(e) = tx_connack.send(peer) {
                        pf_error!(me; "error sending to tx_connack: {}", e);
                    }
                },

                // new peer connection accepted
                accepted = peer_listener.accept() => {
                    if let Err(e) = accepted {
                        pf_warn!(me; "error accepting peer connection: {}", e);
                        continue;
                    }
                    let (stream, addr) = accepted.unwrap();
                    if let Err(e) = Self::accept_new_peer(
                        me,
                        stream,
                        addr,
                        tx_recv.clone(),
                        &mut tx_sends,
                        &mut peer_messenger_handles,
                        tx_exit.clone()
                    ).await {
                        pf_error!(me; "error accepting new peer: {}", e);
                    }
                },

                // a peer messenger thread exits
                id = rx_exit.recv() => {
                    let id = id.unwrap();
                    if let Err(e) = Self::remove_left_peer(
                        me,
                        id,
                        &mut tx_sends,
                        &mut peer_messenger_handles
                    ) {
                        pf_error!(me; "error removing left peer {}: {}", id, e);
                    }
                },
            }
        }

        // pf_debug!(me; "peer_acceptor thread exitted");
    }
}

// TransportHub peer_messenger thread implementation
impl<Msg> TransportHub<Msg>
where
    Msg: fmt::Debug
        + Clone
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    /// Writes a message through given TcpStream.
    fn write_msg(
        write_buf: &mut BytesMut,
        write_buf_cursor: &mut usize,
        conn_write: &OwnedWriteHalf,
        msg: Option<&Msg>,
    ) -> Result<bool, SummersetError> {
        safe_tcp_write(write_buf, write_buf_cursor, conn_write, msg)
    }

    /// Reads a message from given TcpStream.
    async fn read_msg(
        // first 8 btyes being the message length, and the rest bytes being the
        // message itself
        read_buf: &mut BytesMut,
        conn_read: &mut OwnedReadHalf,
    ) -> Result<Msg, SummersetError> {
        safe_tcp_read(read_buf, conn_read).await
    }

    /// Peer messenger thread function.
    async fn peer_messenger_thread(
        me: ReplicaId,
        id: ReplicaId,    // corresonding peer's ID
        addr: SocketAddr, // corresponding peer's address
        conn: TcpStream,
        mut rx_send: mpsc::UnboundedReceiver<Msg>,
        tx_recv: mpsc::UnboundedSender<(ReplicaId, Msg)>,
        tx_exit: mpsc::UnboundedSender<ReplicaId>,
    ) {
        pf_debug!(me; "peer_messenger thread for {} ({}) spawned", id, addr);

        let (mut conn_read, conn_write) = conn.into_split();
        let mut read_buf = BytesMut::with_capacity(8 + 1024);
        let mut write_buf = BytesMut::with_capacity(8 + 1024);
        let mut write_buf_cursor = 0;

        let mut retrying = false;
        loop {
            tokio::select! {
                // gets a message to send out
                msg = rx_send.recv(), if !retrying => {
                    match msg {
                        Some(msg) => {
                            match Self::write_msg(
                                &mut write_buf,
                                &mut write_buf_cursor,
                                &conn_write,
                                Some(&msg),
                            ) {
                                Ok(true) => {
                                    // pf_trace!(me; "sent -> {} msg {:?}", id, msg);
                                }
                                Ok(false) => {
                                    pf_debug!(me; "should start retrying msg send -> {}", id);
                                    retrying = true;
                                }
                                Err(e) => {
                                    pf_error!(me; "error sending -> {}: {}", id, e);
                                }
                            }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // receives new message from peer
                msg = Self::read_msg(&mut read_buf, &mut conn_read) => {
                    match msg {
                        Ok(msg) => {
                            // pf_trace!(me; "recv <- {} msg {:?}", id, msg);
                            if let Err(e) = tx_recv.send((id, msg)) {
                                pf_error!(me; "error sending to tx_recv for {}: {}", id, e);
                            }
                        },

                        Err(e) => {
                            pf_error!(me; "error receiving msg <- {}: {}", id, e);
                            break; // probably the peer exitted ungracefully
                        }
                    }
                },

                // retrying last unsuccessful message send
                _ = conn_write.writable(), if retrying => {
                    match Self::write_msg(
                        &mut write_buf,
                        &mut write_buf_cursor,
                        &conn_write,
                        None
                    ) {
                        Ok(true) => {
                            pf_debug!(me; "finished retrying last msg send -> {}", id);
                            retrying = false;
                        }
                        Ok(false) => {
                            pf_debug!(me; "still should retry last msg send -> {}", id);
                        }
                        Err(e) => {
                            pf_error!(me; "error retrying last msg send -> {}: {}", id, e);
                        }
                    }
                }
            }
        }

        if let Err(e) = tx_exit.send(id) {
            pf_error!(me; "error sending exit signal for {}: {}", id, e);
        }
        pf_debug!(me; "peer_messenger thread for {} ({}) exitted", id, addr);
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use std::sync::Arc;
    use serde::{Serialize, Deserialize};
    use tokio::sync::Barrier;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestMsg(String);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_send_recv() -> Result<(), SummersetError> {
        let barrier = Arc::new(Barrier::new(3));
        let barrier1 = barrier.clone();
        let barrier2 = barrier.clone();
        tokio::spawn(async move {
            // replica 1
            let mut hub: TransportHub<TestMsg> =
                TransportHub::new_and_setup(1, 3, "127.0.0.1:54801".parse()?)
                    .await?;
            barrier1.wait().await;
            hub.connect_to_peer(2, "127.0.0.1:54802".parse()?).await?;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            hub.send_msg(TestMsg("world".into()), 0)?;
            // recv another message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("nice".into()));
            // send another message to 0
            hub.send_msg(TestMsg("job!".into()), 0)?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("terminate".into()));
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> =
                TransportHub::new_and_setup(2, 3, "127.0.0.1:54802".parse()?)
                    .await?;
            barrier2.wait().await;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            hub.send_msg(TestMsg("world".into()), 0)?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("terminate".into()));
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> =
            TransportHub::new_and_setup(0, 3, "127.0.0.1:54800".parse()?)
                .await?;
        barrier.wait().await;
        hub.connect_to_peer(1, "127.0.0.1:54801".parse()?).await?;
        hub.connect_to_peer(2, "127.0.0.1:54802".parse()?).await?;
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
        let mut map = Bitmap::new(3, false);
        map.set(1, true)?;
        hub.bcast_msg(TestMsg("nice".into()), Some(map))?;
        // recv another message from 1
        let (id, msg) = hub.recv_msg().await?;
        assert_eq!(id, 1);
        assert_eq!(msg, TestMsg("job!".into()));
        // send termination message to 1 and 2
        hub.bcast_msg(TestMsg("terminate".into()), None)?;
        Ok(())
    }
}
