//! Server internal TCP transport module implementation.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::core::utils::{SummersetError, ReplicaId, ReplicaMap};
use crate::core::replica::GenericReplica;

use serde::{Serialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use log::{trace, debug, info, error};

/// Server internal TCP transport module.
#[derive(Debug)]
pub struct TransportHub<'r, Rpl, Msg>
where
    Rpl: 'r + GenericReplica,
    Msg: Serialize + DeserializeOwned,
{
    /// Reference to protocol-specific replica struct.
    replica: &'r Rpl,

    /// Sender side of the send channel.
    tx_send: Option<mpsc::Sender<(Msg, ReplicaMap)>>,

    /// Sender side of the recv channel, stored here so that we can clone it
    /// to a message recver thread whenever a new peer is connected.
    tx_recv: Option<mpsc::Sender<(ReplicaId, Msg)>>,

    /// Receiver side of the recv channel.
    rx_recv: Option<mpsc::Receiver<(ReplicaId, Msg)>>,

    /// TCP Listener for peer connections.
    peer_listener: Option<TcpListener>,

    /// Map from peer ID -> TCP connection used in sending direction, shared
    /// with the message sender thread.
    peer_send_conns: Arc<Mutex<HashMap<ReplicaId, TcpStream>>>,

    /// Map from peer ID -> TCP connection used in recving direction. Each
    /// stream is shared with the corresponding message receiver thread.
    peer_recv_conns: HashMap<ReplicaId, Arc<Mutex<TcpStream>>>,

    /// Join handle of the message sender thread.
    msg_sender_handle: Option<JoinHandle<()>>,

    /// Map from peer ID -> message listener thread join handles.
    msg_recver_handles: HashMap<ReplicaId, JoinHandle<()>>,
}

// TransportHub public API implementation
impl<'r, Rpl, Msg> TransportHub<'r, Rpl, Msg> {
    /// Creates a new server internal TCP transport hub.
    pub fn new(replica: &'r Rpl) -> Self {
        TransportHub {
            replica,
            tx_send: None,
            tx_recv: None,
            rx_recv: None,
            peer_listener: None,
            peer_send_conns: Arc::new(Mutex::new(HashMap::new())),
            peer_recv_conns: HashMap::new(),
            msg_sender_handle: None,
            msg_recver_handles: HashMap::new(),
        }
    }

    /// Spawns the message sender thread. Creates an send channel for sending
    /// messages to peers and an recv channel for listening on peers' messages.
    /// Creates a TCP listener for peer connections.
    pub async fn setup(
        &mut self,
        chan_send_cap: usize,
        chan_recv_cap: usize,
    ) -> Result<(), SummersetError> {
        if let Some(_) = self.msg_sender_handle {
            return logged_err!(
                self.replica.id(),
                "msg_sender thread already spawned"
            );
        }
        if chan_send_cap == 0 {
            return logged_err!(
                self.replica.id(),
                "invalid chan_send_cap {}",
                chan_send_cap
            );
        }
        if chan_recv_cap == 0 {
            return logged_err!(
                self.replica.id(),
                "invalid chan_recv_cap {}",
                chan_recv_cap
            );
        }

        let (tx_send, mut rx_send) = mpsc::channel(chan_send_cap);
        let (tx_recv, mut rx_recv) = mpsc::channel(chan_recv_cap);
        self.tx_send = Some(tx_send);
        self.tx_recv = Some(tx_recv);
        self.rx_recv = Some(rx_recv);

        let msg_sender_handle = tokio::spawn(TransportHub::msg_sender_thread(
            self.replica.id(),
            self.peer_send_conns.clone(),
            rx_send,
        ));
        self.msg_sender_handle = Some(msg_sender_handle);

        let peer_listener = TcpListener::bind(self.replica.addr()).await?;
        self.peer_listener = Some(peer_listener);

        Ok(())
    }

    /// Connects to a new peer replica in the sending direction. Should be
    /// called after `setup`.
    pub async fn connect_peer(
        &mut self,
        id: ReplicaId,
        addr: String,
    ) -> Result<(), SummersetError> {
        let me = self.replica.id();

        if let None = self.msg_sender_handle {
            return logged_err!(me, "connect_peer called before setup");
        }
        if id == me || id >= self.replica.population() {
            return logged_err!(me, "invalid peer ID {} to connect", id);
        }
        if self.peer_send_conns.contains_key(&id) {
            return logged_err!(
                me,
                "peer ID {} already in peer_send_conns",
                id
            );
        }
        if let Err(_) = addr.parse::<SocketAddr>() {
            return logged_err!(me, "invalid addr string '{}'", addr);
        }

        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(me).await?; // send my ID

        let mut send_conns_guard = self.peer_send_conns.lock().unwrap();
        send_conns_guard.insert(id, stream);

        pf_debug!(me, "connected to peer {}", id);
        Ok(())
    }

    /// Waits for a connection attempt from some peer, and spawns the
    /// corresponding message recver thread. Should be called after `setup`.
    /// Returns the connecting peer's ID if successful.
    pub async fn wait_on_peer(&mut self) -> Result<ReplicaId, SummersetError> {
        let me = self.replica.id();

        if let None = self.msg_sender_handle {
            return logged_err!(me, "wait_on_peer called before setup");
        }

        let mut stream = self.peer_listener.accept().await?;
        let id = stream.read_u8().await?; // receive connecting peer's ID

        if id == me || id >= self.replica.population() {
            return logged_err!(me, "invalid peer ID {} waited on", id);
        }
        if self.peer_recv_conns.contains_key(&id) {
            return logged_err!(
                me,
                "peer ID {} already in peer_recv_conns",
                id
            );
        }

        self.peer_recv_conns
            .insert(id, Arc::new(Mutex::new(stream)));

        let msg_recver_handle = tokio::spawn(TransportHub::msg_recver_thread(
            me,
            id,
            self.peer_recv_conns[id].clone(),
            self.tx_recv.clone(),
        ));
        self.msg_recver_handles.insert(id, msg_recver_handle);

        pf_debug!(me, "waited on peer {}", id);
        Ok(())
    }

    /// Convenience function of a simple deadlock-free strategy for a group of
    /// replicas to establish connection with each other. Returns a set of peer
    /// IDs connected if successful.
    pub async fn group_connect(
        &mut self,
        peer_addrs: HashMap<ReplicaId, String>,
    ) -> Result<HashSet<ReplicaId>, SummersetError> {
        let me = self.replica.id();

        // sort peers ID list
        let peer_ids: Vec<ReplicaId> = peer_addrs
            .keys()
            .filter(|&id| id != me && id < self.replica.population())
            .collect()
            .sort();
        if peer_ids.len() == 0 {
            return logged_err!(me, "invalid peer_addrs keys '{}'", peer_ids);
        }

        let mid_idx = peer_ids.partition_point(|&id| id < me);
        let connected: HashSet<ReplicaId> = HashSet::new();

        // for peers with ID smaller than me, do `connect_peer` to them first,
        // followed by `wait_on_peer`, in interleaving order
        for &id in &peer_ids[..mid_idx] {
            let addr = peer_addrs[id];
            while let Err(_) = self.connect_peer(id, addr).await {
                // retry until connected
                sleep(Duration::from_millis(1)).await;
            }

            let oid = self.wait_on_peer().await?;
            if oid != id {
                return logged_err!(me, "unexpected peer ID {} waited on", oid);
            }
            if connected.contains(&oid) {
                return logged_err!(me, "duplicate peer ID {} waited on", oid);
            }

            connected.insert(oid);
        }

        // for peers with ID larger than me, do `wait_on_peer` on anyone,
        // followed `connect_peer` to waited peer, in interleaving order
        for _ in &peer_ids[mid_idx..] {
            let oid = self.wait_on_peer().await?;
            if oid <= me || oid >= self.replica.population() {
                return logged_err!(me, "invalid peer ID {} waited on", oid);
            }
            if connected.contains(&oid) {
                return logged_err!(me, "duplicate peer ID {} waited on", oid);
            }

            let addr = peer_addrs[oid];
            while let Err(_) = self.connect_peer(oid, addr).await {
                // retry until connected
                sleep(Duration::from_millis(1)).await;
            }

            connected.insert(oid);
        }

        assert_eq!(connected.len(), peer_ids.len());
        for id in &peer_ids {
            assert!(connected.contains(id));
        }
        pf_info!(me, "established connection to peers {}", peer_ids);
        Ok(connected)
    }

    /// Sends a message to specified peers by sending to the send channel.
    pub async fn send_msg(
        &mut self,
        msg: Msg,
        target: ReplicaMap,
    ) -> Result<(), SummersetError> {
        if let None = self.msg_sender_handle {
            return logged_err!(
                self.replica.id(),
                "send_msg called before setup"
            );
        }

        match self.tx_send {
            Some(ref tx_send) => Ok(tx_send.send((msg, target)).await?),
            None => logged_err!(self.replica.id(), "tx_send not created yet"),
        }
    }

    /// Receives a message from some peer by receiving from the recv channel.
    /// Returns a pair of `(peer_id, msg)` on success.
    pub async fn recv_msg(
        &mut self,
    ) -> Result<(ReplicaId, Msg), SummersetError> {
        if let None = self.msg_sender_handle {
            return logged_err!(
                self.replica.id(),
                "recv_msg called before setup"
            );
        }

        match self.rx_recv {
            Some(ref mut rx_recv) => match rx_recv.recv().await {
                Some((id, msg)) => Ok((id, msg)),
                None => logged_err!(
                    self.replica.id(),
                    "recv channel has been closed"
                ),
            },
            None => logged_err!(self.replica.id(), "rx_recv not created yet"),
        }
    }
}

// TransportHub msg_sender thread implementation
impl<'r, Rpl, Msg> TransportHub<'r, Rpl, Msg> {
    /// Writes a message through given TcpStream.
    async fn write_msg(
        msg: &Msg,
        conn: &mut TcpStream,
    ) -> Result<(), SummersetError> {
        let msg_bytes = encode_to_vec(msg)?;
        conn.write_all(&msg_bytes[..]).await?;
        Ok(())
    }

    /// Message sender thread function.
    async fn msg_sender_thread(
        me: ReplicaId,
        send_conns: Arc<Mutex<HashMap<ReplicaId, TcpStream>>>,
        mut rx_send: mpsc::Receiver<(Msg, ReplicaMap)>,
    ) {
        pf_debug!(me, "msg_sender thread spawned");

        loop {
            match rx_send.recv().await {
                Some((msg, target)) => {
                    // need tokio::sync::Mutex here since held across await
                    let mut send_conns_guard = send_conns.lock().unwrap();

                    for (id, to_send) in target.iter().enumerate() {
                        if id == me {
                            continue;
                        }
                        if !send_conns_guard.contains_key(&id) {
                            pf_error!(
                                me,
                                "replica ID {} not found in send_conns",
                                id
                            );
                            continue;
                        }

                        if to_send {
                            if let Err(e) = TransportHub::write_msg(
                                &msg,
                                &mut send_conns_guard[id],
                            )
                            .await
                            {
                                pf_error!(me, "error sending to {}: {}", id, e);
                            } else {
                                pf_trace!(me, "sent to {} msg {:?}", id, msg);
                            }
                        }
                    }
                }

                None => break, // channel gets closed and no messages remain
            }
        }

        pf_debug!(me, "msg_sender thread exitted");
    }
}

// TransportHub msg_recver thread implementation
impl<'r, Rpl, Msg> TransportHub<'r, Rpl, Msg> {
    /// Reads a message from given TcpStream.
    async fn read_msg(conn: &mut TcpStream) -> Result<Msg, SummersetError> {
        let msg_buf: Vec<u8> = vec![0; size_of::<Msg>()];
        conn.read_exact(&mut msg_buf[..]).await?;
        let msg = decode_from_slice(&msg_buf)?;
        Ok(msg)
    }

    /// Message recver thread function.
    async fn msg_recver_thread(
        me: ReplicaId,
        id: ReplicaId, // corresponding peer's ID
        recv_conn: Arc<Mutex<TcpStream>>,
        tx_recv: mpsc::Sender<(ReplicaId, Msg)>,
    ) {
        pf_debug!(me, "msg_recver thread for {} spawned", id);

        // need tokio::sync::Mutex here since held across await
        let mut recv_conn_guard = recv_conn.lock().unwrap();

        loop {
            match TransportHub::read_msg(&mut recv_conn_guard).await {
                Ok(msg) => {
                    pf_trace!(me, "recv from {} msg {:?}", id, msg);
                    if let Err(e) = tx_recv.send((id, msg)).await {
                        pf_error!(
                            me,
                            "error sending to tx_recv for {}: {}",
                            id,
                            e
                        );
                    }
                }

                Err(e) => {
                    pf_error!(me, "error receiving msg for {}: {}", id, e)
                }
            }
        }

        pf_debug!(me, "msg_recver thread for {} exitted", id);
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use std::collections::HashSet;
    use crate::core::replica::DummyReplica;
    use serde::{Serialize, Deserialize};

    #[test]
    fn hub_setup() -> Result<(), SummersetError> {
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = TransportHub::new(&replica);
        assert!(tokio_test::block_on(hub.setup(0, 0)).is_err());
        tokio_test::block_on(hub.setup(100, 100))?;
        assert!(hub.tx_send.is_some());
        assert!(hub.tx_recv.is_some());
        assert!(hub.rx_recv.is_some());
        assert!(hub.peer_listener.is_some());
        assert!(hub.msg_sender_handle.is_some());
        Ok(())
    }

    #[test]
    fn manual_connect() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let replica = DummyReplica::new(1, 3, "127.0.0.1:52801".into());
            let mut hub = TransportHub::new(&replica);
            hub.setup(1, 1).await?;
            while let Err(e) =
                hub.connect_peer(0, "127.0.0.1:52800".into()).await
            {
                sleep(Duration::from_millis(1)).await;
            }
        });
        tokio::spawn(async move {
            // replica 2
            let replica = DummyReplica::new(2, 3, "127.0.0.1:52802".into());
            let mut hub = TransportHub::new(&replica);
            hub.setup(1, 1).await?;
            while let Err(e) =
                hub.connect_peer(0, "127.0.0.1:52800".into()).await
            {
                sleep(Duration::from_millis(1)).await;
            }
        });
        // replica 0
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = TransportHub::new(&replica);
        tokio_test::block_on(hub.setup(1, 1))?;
        let peers = HashSet::new();
        peers.insert(tokio_test::block_on(hub.wait_on_peer())?);
        peers.insert(tokio_test::block_on(hub.wait_on_peer())?);
        let ref_peers = HashSet::from([1, 2]);
        assert_eq!(peers, ref_peers);
        Ok(())
    }

    #[test]
    fn group_connect() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let replica = DummyReplica::new(1, 3, "127.0.0.1:52801".into());
            let mut hub = TransportHub::new(&replica);
            hub.setup(1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:52800".into()),
                (2, "127.0.0.1:52802".into()),
            ]))
            .await?;
        });
        tokio::spawn(async move {
            // replica 2
            let replica = DummyReplica::new(2, 3, "127.0.0.1:52802".into());
            let mut hub = TransportHub::new(&replica);
            hub.setup(1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:52800".into()),
                (1, "127.0.0.1:52801".into()),
            ]))
            .await?;
        });
        // replica 0
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = TransportHub::new(&replica);
        tokio_test::block_on(hub.setup(1, 1))?;
        let connected =
            tokio_test::block_on(hub.group_connect(HashMap::from([
                (1, "127.0.0.1:52801".into()),
                (2, "127.0.0.1:52802".into()),
            ])))?;
        assert_eq!(connected, HashSet::from([1, 2]));
        Ok(())
    }

    #[derive(Serialize, Deserialize)]
    struct TestMsg(String);

    #[test]
    fn send_recv_api() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let replica = DummyReplica::new(1, 3, "127.0.0.1:52801".into());
            let mut hub = TransportHub::new(&replica);
            hub.setup(1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:52800".into()),
                (2, "127.0.0.1:52802".into()),
            ]))
            .await?;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            let mut map = ReplicaMap::new(3, false)?;
            map.set(0, true)?;
            hub.send_msg(TestMsg("world".into()), map.clone()).await?;
            // recv another message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("nice".into()));
            // send another message to 0
            hub.send_msg(TestMsg("job!".into()), map.clone()).await?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("terminate".into()));
        });
        tokio::spawn(async move {
            // replica 2
            let replica = DummyReplica::new(2, 3, "127.0.0.1:52802".into());
            let mut hub = TransportHub::new(&replica);
            hub.setup(1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:52800".into()),
                (1, "127.0.0.1:52801".into()),
            ]))
            .await?;
            // recv a message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("hello".into()));
            // send a message to 0
            let mut map = ReplicaMap::new(3, false)?;
            map.set(0, true)?;
            hub.send_msg(TestMsg("world".into()), map.clone()).await?;
            // recv another message from 0
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("nice".into()));
            // send another message to 0
            hub.send_msg(TestMsg("job!".into()), map.clone()).await?;
            // wait for termination message
            let (id, msg) = hub.recv_msg().await?;
            assert!(id == 0);
            assert_eq!(msg, TestMsg("terminate".into()));
        });
        // replica 0
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = TransportHub::new(&replica);
        tokio_test::block_on(hub.setup(1, 1))?;
        tokio_test::block_on(hub.group_connect(HashMap::from([
            (1, "127.0.0.1:52801".into()),
            (2, "127.0.0.1:52802".into()),
        ])))?;
        // send a message to 1 and 2
        tokio_test::block_on(
            hub.send_msg(TestMsg("hello".into()), ReplicaMap::new(3, true)),
        )?;
        // recv a message from both 1 and 2
        let (id, msg) = tokio_test::block_on(hub.recv_msg())?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("world".into()));
        let (id, msg) = tokio_test::block_on(hub.recv_msg())?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("world".into()));
        // send another message to 1 and 2
        tokio_test::block_on(
            hub.send_msg(TestMsg("nice".into()), ReplicaMap::new(3, true)),
        )?;
        // recv another message from both 1 and 2
        let (id, msg) = tokio_test::block_on(hub.recv_msg())?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("job!".into()));
        let (id, msg) = tokio_test::block_on(hub.recv_msg())?;
        assert!(id == 1 || id == 2);
        assert_eq!(msg, TestMsg("job!".into()));
        // send termination message to 1 and 2
        tokio_test::block_on(
            hub.send_msg(TestMsg("terminate".into()), ReplicaMap::new(3, true)),
        )?;
        Ok(())
    }
}
