//! Summerset server internal TCP transport module implementation.

use std::fmt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use crate::utils::{SummersetError, ReplicaMap};
use crate::server::ReplicaId;

use bytes::{Bytes, BytesMut};

use serde::{Serialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

/// Server internal TCP transport module.
pub struct TransportHub<Msg> {
    /// My replica ID.
    me: ReplicaId,

    /// Cluster size (total number of replicas).
    population: u8,

    /// Capacity of a send channel.
    chan_send_cap: usize,

    /// Map from peer ID -> Sender side of the send channel.
    tx_sends: HashMap<ReplicaId, mpsc::Sender<Msg>>,

    /// Sender side of the recv channel, stored here so that we can clone it
    /// to a message recver thread when a new peer is connected.
    tx_recv: Option<mpsc::Sender<(ReplicaId, Msg)>>,

    /// Receiver side of the recv channel.
    rx_recv: Option<mpsc::Receiver<(ReplicaId, Msg)>>,

    /// TCP listeners for peer connections.
    peer_listeners: HashMap<ReplicaId, TcpListener>,

    /// Map from peer ID -> peer messenger thread join handles.
    peer_messenger_handles: HashMap<ReplicaId, JoinHandle<()>>,
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
    /// Creates a new server internal TCP transport hub.
    pub fn new(me: ReplicaId, population: u8) -> Self {
        TransportHub {
            me,
            population,
            chan_send_cap: 0,
            tx_sends: HashMap::new(),
            tx_recv: None,
            rx_recv: None,
            peer_listeners: HashMap::new(),
            peer_messenger_handles: HashMap::new(),
        }
    }

    /// Spawns the message sender thread. Creates a send channel for sending
    /// messages to peers and a recv channel for listening on peers' messages.
    /// Creates TCP listeners for peers with higher IDs.
    pub async fn setup(
        &mut self,
        conn_addrs: &HashMap<ReplicaId, SocketAddr>,
        chan_send_cap: usize,
        chan_recv_cap: usize,
    ) -> Result<(), SummersetError> {
        if self.tx_recv.is_some() {
            return logged_err!(self.me; "setup already done");
        }
        if conn_addrs.len() != (self.population as usize) - 1 {
            return logged_err!(
                self.me;
                "invalid size of conn_addrs {}",
                conn_addrs.len()
            );
        }
        if chan_send_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_send_cap {}",
                chan_send_cap
            );
        }
        if chan_recv_cap == 0 {
            return logged_err!(
                self.me;
                "invalid chan_recv_cap {}",
                chan_recv_cap
            );
        }

        self.chan_send_cap = chan_send_cap;

        let (tx_recv, rx_recv) = mpsc::channel(chan_recv_cap);
        self.tx_recv = Some(tx_recv);
        self.rx_recv = Some(rx_recv);

        for (&id, &addr) in conn_addrs {
            assert_ne!(id, self.me);
            if id > self.me {
                self.peer_listeners
                    .insert(id, TcpListener::bind(addr).await?);
            }
        }

        Ok(())
    }

    /// Connects to a new peer replica with lower ID actively, and spawns the
    /// corresponding message recver thread. Should be called after `setup`.
    // TODO: maybe make this function pub
    async fn connect_peer(
        &mut self,
        id: ReplicaId,
        peer_addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        if self.tx_recv.is_none() {
            return logged_err!(self.me; "connect_peer called before setup");
        }
        if id >= self.me {
            return logged_err!(self.me; "invalid peer ID {} to connect", id);
        }
        if self.peer_messenger_handles.contains_key(&id) {
            return logged_err!(
                self.me;
                "peer ID {} already in peer_messenger_handles",
                id
            );
        }

        // on MacOS, it seems that `connect()` might report `EADDRINUSE` if
        // the socket is bound to a specific port, hence the following piece
        // is currently commented out. A proactive connector will therefore
        // use a kernel-assigned ephemeral port instead of a custom port
        // let socket = TcpSocket::new_v4()?;
        // socket.set_reuseaddr(true)?;
        // socket.bind(local_addr)?;

        pf_debug!(self.me; "connecting to peer {} '{}'...", id, peer_addr);
        let mut stream = TcpStream::connect(peer_addr).await?;
        stream.write_u8(self.me).await?; // send my ID

        let (tx_send, rx_send) = mpsc::channel(self.chan_send_cap);
        self.tx_sends.insert(id, tx_send);

        let peer_messenger_handle = tokio::spawn(Self::peer_messenger_thread(
            self.me,
            id,
            peer_addr,
            stream,
            rx_send,
            self.tx_recv.as_ref().unwrap().clone(),
        ));
        self.peer_messenger_handles
            .insert(id, peer_messenger_handle);

        pf_debug!(self.me; "connected to peer {}", id);
        Ok(())
    }

    /// Waits for a connection attempt from some peer with higher ID, and
    /// spawns the corresponding message recver thread. Should be called
    /// after `setup`.
    // TODO: maybe make this function pub
    async fn wait_on_peer(
        &mut self,
        id: ReplicaId,
    ) -> Result<(), SummersetError> {
        if self.tx_recv.is_none() {
            return logged_err!(self.me; "wait_on_peer called before setup");
        }
        if id <= self.me {
            return logged_err!(self.me; "invalid peer ID {} to connect", id);
        }
        if self.peer_messenger_handles.contains_key(&id) {
            return logged_err!(
                self.me;
                "peer ID {} already in peer_messenger_handles",
                id
            );
        }
        if !self.peer_listeners.contains_key(&id) {
            return logged_err!(self.me; "peer ID {} not in peer_listeners", id);
        }

        pf_debug!(self.me; "waiting on '{}' for peer {}...",
                           self.peer_listeners[&id].local_addr()?, id);
        let (mut stream, peer_addr) = self.peer_listeners[&id].accept().await?;
        let recv_id = stream.read_u8().await?; // receive connecting peer's ID

        if recv_id != id {
            return logged_err!(self.me; "mismatch peer ID {} waited on", id);
        }
        if self.peer_messenger_handles.contains_key(&id) {
            return logged_err!(
                self.me;
                "peer ID {} already in peer_messenger_handles",
                id
            );
        }

        let (tx_send, rx_send) = mpsc::channel(self.chan_send_cap);
        self.tx_sends.insert(id, tx_send);

        let peer_messenger_handle = tokio::spawn(Self::peer_messenger_thread(
            self.me,
            id,
            peer_addr,
            stream,
            rx_send,
            self.tx_recv.as_ref().unwrap().clone(),
        ));
        self.peer_messenger_handles
            .insert(id, peer_messenger_handle);

        pf_debug!(self.me; "waited on peer {}", id);
        Ok(())
    }

    /// Convenience function of a simple deadlock-free strategy for a group of
    /// replicas to establish connection with each other. Returns a set of peer
    /// IDs connected if successful.
    pub async fn group_connect(
        &mut self,
        conn_addrs: &HashMap<ReplicaId, SocketAddr>,
        peer_addrs: &HashMap<ReplicaId, SocketAddr>,
    ) -> Result<HashSet<ReplicaId>, SummersetError> {
        // sort peers ID list
        let mut peer_ids: Vec<ReplicaId> = peer_addrs
            .keys()
            .filter(|&&id| id != self.me && id < self.population)
            .cloned()
            .collect::<Vec<ReplicaId>>();
        peer_ids.sort();
        if peer_ids.is_empty() {
            return logged_err!(
                self.me;
                "invalid peer_addrs keys '{:?}'",
                peer_ids
            );
        }
        if conn_addrs.len() != peer_ids.len() {
            return logged_err!(
                self.me;
                "size of conn_addrs {} != size of peer_addrs {}",
                conn_addrs.len(), peer_ids.len()
            );
        }

        let mid_idx = peer_ids.partition_point(|&id| id < self.me);
        let mut connected: HashSet<ReplicaId> = HashSet::new();

        // for peers with ID smaller than me, connect to them actively in
        // increasing ID order
        for &id in &peer_ids[..mid_idx] {
            let peer_addr = peer_addrs[&id];
            while let Err(e) = self.connect_peer(id, peer_addr).await {
                // retry until connected
                pf_debug!(self.me; "error connecting to peer {}: {}", id, e);
                sleep(Duration::from_millis(100)).await;
            }
            connected.insert(id);
        }

        // for peers with ID larger than me, wait on their connection in
        // increasing ID order
        for &id in &peer_ids[mid_idx..] {
            self.wait_on_peer(id).await?;
            connected.insert(id);
        }

        assert_eq!(connected.len(), peer_ids.len());
        for id in &peer_ids {
            assert!(connected.contains(id));
        }
        pf_info!(self.me; "group connected peers {:?}", peer_ids);
        Ok(connected)
    }

    /// Sends a message to a specified peer by sending to the send channel.
    pub async fn send_msg(
        &mut self,
        msg: Msg,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        if self.tx_recv.is_none() {
            return logged_err!(self.me; "send_msg called before setup");
        }

        match self.tx_sends.get(&peer) {
            Some(tx_send) => {
                tx_send
                    .send(msg)
                    .await
                    .map_err(|e| SummersetError(e.to_string()))?;
            }
            None => {
                pf_warn!(
                    self.me;
                    "peer ID {} not found among connected ones",
                    peer
                );
            }
        }

        Ok(())
    }

    /// Broadcasts message to specified peers by sending to the send channel.
    pub async fn bcast_msg(
        &mut self,
        msg: Msg,
        target: ReplicaMap,
    ) -> Result<(), SummersetError> {
        if self.tx_recv.is_none() {
            return logged_err!(self.me; "bcast_msg called before setup");
        }

        for (peer, to_send) in target.iter() {
            if !to_send {
                continue;
            } else {
                self.send_msg(msg.clone(), peer).await?
            }
        }

        Ok(())
    }

    /// Receives a message from some peer by receiving from the recv channel.
    /// Returns a pair of `(peer_id, msg)` on success.
    pub async fn recv_msg(
        &mut self,
    ) -> Result<(ReplicaId, Msg), SummersetError> {
        if self.tx_recv.is_none() {
            return logged_err!(self.me; "recv_msg called before setup");
        }

        match self.rx_recv {
            Some(ref mut rx_recv) => match rx_recv.recv().await {
                Some((id, msg)) => Ok((id, msg)),
                None => logged_err!(self.me; "recv channel has been closed"),
            },
            None => logged_err!(self.me; "rx_recv not created yet"),
        }
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
    async fn write_msg(
        msg: &Msg,
        conn_write: &mut WriteHalf<'_>,
    ) -> Result<(), SummersetError> {
        let msg_bytes = encode_to_vec(msg)?;
        conn_write.write_u64(msg_bytes.len() as u64).await?; // send length first
        conn_write.write_all(&msg_bytes[..]).await?;
        Ok(())
    }

    /// Reads a message from given TcpStream.
    async fn read_msg(
        // first 8 btyes being the message length, and the rest bytes being the
        // message itself
        msg_buf: &mut BytesMut,
        conn_read: &mut ReadHalf<'_>,
    ) -> Result<Msg, SummersetError> {
        // CANCELLATION SAFETY: we cannot use `read_u64()` and `read_exact()`
        // here because this function is used as a `tokio::select!` branch and
        // that those two methods are not cancellation-safe

        // read length of message first
        assert!(msg_buf.capacity() >= 8);
        while msg_buf.len() < 8 {
            // msg_len not wholesomely read from socket before last cancellation
            conn_read.read_buf(msg_buf).await?;
        }
        let msg_len = u64::from_be_bytes(msg_buf[..8].try_into().unwrap());

        // then read the message itself
        let msg_end = 8 + msg_len as usize;
        if msg_buf.capacity() < msg_end {
            // capacity not big enough, reserve more space
            msg_buf.reserve(msg_end - msg_buf.capacity());
        }
        while msg_buf.len() < msg_end {
            conn_read.read_buf(msg_buf).await?;
        }
        let msg = decode_from_slice(&msg_buf[8..msg_end])?;

        // if reached this point, no further cancellation to this call is
        // possible (because there are no more awaits ahead); discard bytes
        // used in this call
        if msg_buf.len() > msg_end {
            let buf_tail = Bytes::copy_from_slice(&msg_buf[msg_end..]);
            msg_buf.clear();
            msg_buf.extend_from_slice(&buf_tail);
        } else {
            msg_buf.clear();
        }

        Ok(msg)
    }

    /// Peer messenger thread function.
    async fn peer_messenger_thread(
        me: ReplicaId,
        id: ReplicaId,    // corresonding peer's ID
        addr: SocketAddr, // corresponding peer's address
        mut conn: TcpStream,
        mut rx_send: mpsc::Receiver<Msg>,
        tx_recv: mpsc::Sender<(ReplicaId, Msg)>,
    ) {
        pf_debug!(me; "peer_messenger thread for {} ({}) spawned", id, addr);

        let (mut conn_read, mut conn_write) = conn.split();
        let mut msg_buf = BytesMut::with_capacity(8 + 1024);

        loop {
            tokio::select! {
                // select between getting a new message to send out and
                // receiving a new message from peer

                // gets a message to send out
                msg = rx_send.recv() => {
                    match msg {
                        Some(msg) => {
                            if let Err(e) = Self::write_msg(&msg, &mut conn_write)
                                            .await
                                    {
                                        pf_error!(me; "error sending -> {}: {}", id, e);
                                    } else {
                                        // pf_trace!(me; "sent -> {} msg {:?}", id, msg);
                                    }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // receives new message from peer
                msg = Self::read_msg(&mut msg_buf, &mut conn_read) => {
                    match msg {
                        Ok(msg) => {
                            // pf_trace!(me; "recv <- {} msg {:?}", id, msg);
                            if let Err(e) = tx_recv.send((id, msg)).await {
                                pf_error!(
                                    me;
                                    "error sending to tx_recv for {}: {}",
                                    id,
                                    e
                                );
                            }
                        },

                        Err(e) => {
                            pf_error!(me; "error receiving msg <- {}: {}", id, e);
                            break; // probably the peer exitted ungracefully
                        }
                    }
                },
            }
        }

        pf_debug!(me; "peer_messenger thread for {} ({}) exitted", id, addr);
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use std::collections::HashSet;
    use serde::{Serialize, Deserialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestMsg(String);

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn hub_setup() -> Result<(), SummersetError> {
        let mut hub: TransportHub<TestMsg> = TransportHub::new(1, 3);
        let conn_addrs = HashMap::from([
            (0, "127.0.0.1:51810".parse()?),
            (2, "127.0.0.1:51812".parse()?),
        ]);
        assert!(hub.setup(&conn_addrs, 0, 0).await.is_err());
        hub.setup(&conn_addrs, 100, 100).await?;
        assert!(hub.tx_recv.is_some());
        assert!(hub.rx_recv.is_some());
        assert_eq!(hub.peer_listeners.len(), 1);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn group_connect() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let mut hub: TransportHub<TestMsg> = TransportHub::new(1, 3);
            let conn_addrs = HashMap::from([
                (0, "127.0.0.1:53810".parse()?),
                (2, "127.0.0.1:53812".parse()?),
            ]);
            let peer_addrs = HashMap::from([
                (0, "127.0.0.1:53801".parse()?),
                (2, "127.0.0.1:53821".parse()?),
            ]);
            hub.setup(&conn_addrs, 1, 1).await?;
            hub.group_connect(&conn_addrs, &peer_addrs).await?;
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> = TransportHub::new(2, 3);
            let conn_addrs = HashMap::from([
                (0, "127.0.0.1:53820".parse()?),
                (1, "127.0.0.1:53821".parse()?),
            ]);
            let peer_addrs = HashMap::from([
                (0, "127.0.0.1:53802".parse()?),
                (1, "127.0.0.1:53812".parse()?),
            ]);
            hub.setup(&conn_addrs, 1, 1).await?;
            hub.group_connect(&conn_addrs, &peer_addrs).await?;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> = TransportHub::new(0, 3);
        let conn_addrs = HashMap::from([
            (1, "127.0.0.1:53801".parse()?),
            (2, "127.0.0.1:53802".parse()?),
        ]);
        let peer_addrs = HashMap::from([
            (1, "127.0.0.1:53810".parse()?),
            (2, "127.0.0.1:53820".parse()?),
        ]);
        hub.setup(&conn_addrs, 1, 1).await?;
        let connected = hub.group_connect(&conn_addrs, &peer_addrs).await?;
        assert_eq!(connected, HashSet::from([1, 2]));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn api_send_recv() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let mut hub: TransportHub<TestMsg> = TransportHub::new(1, 3);
            let conn_addrs = HashMap::from([
                (0, "127.0.0.1:54810".parse()?),
                (2, "127.0.0.1:54812".parse()?),
            ]);
            let peer_addrs = HashMap::from([
                (0, "127.0.0.1:54801".parse()?),
                (2, "127.0.0.1:54821".parse()?),
            ]);
            hub.setup(&conn_addrs, 1, 1).await?;
            hub.group_connect(&conn_addrs, &peer_addrs).await?;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            hub.send_msg(TestMsg("world".into()), 0).await?;
            // recv another message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("nice".into()));
            // send another message to 0
            hub.send_msg(TestMsg("job!".into()), 0).await?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("terminate".into()));
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> = TransportHub::new(2, 3);
            let conn_addrs = HashMap::from([
                (0, "127.0.0.1:54820".parse()?),
                (1, "127.0.0.1:54821".parse()?),
            ]);
            let peer_addrs = HashMap::from([
                (0, "127.0.0.1:54802".parse()?),
                (1, "127.0.0.1:54812".parse()?),
            ]);
            hub.setup(&conn_addrs, 1, 1).await?;
            hub.group_connect(&conn_addrs, &peer_addrs).await?;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            hub.send_msg(TestMsg("world".into()), 0).await?;
            // recv another message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("nice".into()));
            // send another message to 0
            hub.send_msg(TestMsg("job!".into()), 0).await?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("terminate".into()));
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> = TransportHub::new(0, 3);
        let conn_addrs = HashMap::from([
            (1, "127.0.0.1:54801".parse()?),
            (2, "127.0.0.1:54802".parse()?),
        ]);
        let peer_addrs = HashMap::from([
            (1, "127.0.0.1:54810".parse()?),
            (2, "127.0.0.1:54820".parse()?),
        ]);
        hub.setup(&conn_addrs, 1, 1).await?;
        hub.group_connect(&conn_addrs, &peer_addrs).await?;
        // send a message to 1 and 2
        let mut map = ReplicaMap::new(3, true)?;
        map.set(0, false)?;
        hub.bcast_msg(TestMsg("hello".into()), map.clone()).await?;
        // recv a message from both 1 and 2
        let (id, msg) = hub.recv_msg().await?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("world".into()));
        let (id, msg) = hub.recv_msg().await?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("world".into()));
        // send another message to 1 and 2
        hub.bcast_msg(TestMsg("nice".into()), map.clone()).await?;
        // recv another message from both 1 and 2
        let (id, msg) = hub.recv_msg().await?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("job!".into()));
        let (id, msg) = hub.recv_msg().await?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("job!".into()));
        // send termination message to 1 and 2
        hub.bcast_msg(TestMsg("terminate".into()), map.clone())
            .await?;
        Ok(())
    }
}
