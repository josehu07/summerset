//! Summerset server internal TCP transport module implementation.

use std::fmt;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use crate::utils::{SummersetError, ReplicaMap};
use crate::server::ReplicaId;

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

    /// TCP listener for peer connections.
    peer_listener: Option<TcpListener>,

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
            peer_listener: None,
            peer_messenger_handles: HashMap::new(),
        }
    }

    /// Spawns the message sender thread. Creates an send channel for sending
    /// messages to peers and an recv channel for listening on peers' messages.
    /// Creates a TCP listener for peer connections.
    pub async fn setup(
        &mut self,
        smr_addr: SocketAddr,
        chan_send_cap: usize,
        chan_recv_cap: usize,
    ) -> Result<(), SummersetError> {
        if let Some(_) = self.peer_listener {
            return logged_err!(self.me; "setup already done");
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

        let peer_listener = TcpListener::bind(smr_addr).await?;
        self.peer_listener = Some(peer_listener);

        Ok(())
    }

    /// Connects to a new peer replica actively.
    pub async fn connect_peer(
        &mut self,
        id: ReplicaId,
        addr: SocketAddr,
    ) -> Result<(), SummersetError> {
        if let None = self.peer_listener {
            return logged_err!(self.me; "connect_peer called before setup");
        }
        if id == self.me || id >= self.population {
            return logged_err!(self.me; "invalid peer ID {} to connect", id);
        }
        if self.peer_messenger_handles.contains_key(&id) {
            return logged_err!(
                self.me;
                "peer ID {} already in peer_messenger_handles",
                id
            );
        }

        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(self.me).await?; // send my ID

        let (tx_send, rx_send) = mpsc::channel(self.chan_send_cap);
        self.tx_sends.insert(id, tx_send);

        let peer_messenger_handle = tokio::spawn(Self::peer_messenger_thread(
            self.me,
            id,
            addr,
            stream,
            rx_send,
            self.tx_recv.as_ref().unwrap().clone(),
        ));
        self.peer_messenger_handles
            .insert(id, peer_messenger_handle);

        pf_debug!(self.me; "connected to peer {}", id);
        Ok(())
    }

    /// Waits for a connection attempt from some peer, and spawns the
    /// corresponding message recver thread. Should be called after `setup`.
    /// Returns the connecting peer's ID if successful.
    pub async fn wait_on_peer(&mut self) -> Result<ReplicaId, SummersetError> {
        if let None = self.peer_listener {
            return logged_err!(self.me; "wait_on_peer called before setup");
        }

        let (mut stream, addr) =
            self.peer_listener.as_ref().unwrap().accept().await?;
        let id = stream.read_u8().await?; // receive connecting peer's ID

        if id == self.me || id >= self.population {
            return logged_err!(self.me; "invalid peer ID {} waited on", id);
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
            addr,
            stream,
            rx_send,
            self.tx_recv.as_ref().unwrap().clone(),
        ));
        self.peer_messenger_handles
            .insert(id, peer_messenger_handle);

        pf_debug!(self.me; "waited on peer {}", id);
        Ok(id)
    }

    /// Convenience function of a simple deadlock-free strategy for a group of
    /// replicas to establish connection with each other. Returns a set of peer
    /// IDs connected if successful.
    pub async fn group_connect(
        &mut self,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
    ) -> Result<HashSet<ReplicaId>, SummersetError> {
        // sort peers ID list
        let mut peer_ids: Vec<ReplicaId> = peer_addrs
            .keys()
            .filter(|&&id| id != self.me && id < self.population)
            .cloned()
            .collect::<Vec<ReplicaId>>();
        peer_ids.sort();
        if peer_ids.len() == 0 {
            return logged_err!(
                self.me;
                "invalid peer_addrs keys '{:?}'",
                peer_ids
            );
        }

        let mid_idx = peer_ids.partition_point(|&id| id < self.me);
        let mut connected: HashSet<ReplicaId> = HashSet::new();

        // for peers with ID smaller than me, connect to them actively
        for &id in &peer_ids[..mid_idx] {
            let addr = peer_addrs[&id];
            while let Err(_) = self.connect_peer(id, addr).await {
                // retry until connected
                sleep(Duration::from_millis(1)).await;
            }
            connected.insert(id);
        }

        // for peers with ID larger than me, wait on their connection
        for _ in &peer_ids[mid_idx..] {
            let id = self.wait_on_peer().await?;
            if id <= self.me || id >= self.population {
                return logged_err!(
                    self.me;
                    "unexpected peer ID {} waited on",
                    id
                );
            }
            if connected.contains(&id) {
                return logged_err!(
                    self.me;
                    "duplicate peer ID {} waited on",
                    id
                );
            }
            connected.insert(id);
        }

        assert_eq!(connected.len(), peer_ids.len());
        for id in &peer_ids {
            assert!(connected.contains(id));
        }
        pf_info!(self.me; "group connected peers {:?}", peer_ids);
        Ok(connected)
    }

    /// Sends a message to specified peers by sending to the send channel.
    pub async fn send_msg(
        &mut self,
        msg: Msg,
        target: ReplicaMap,
    ) -> Result<(), SummersetError> {
        if let None = self.peer_listener {
            return logged_err!(self.me; "send_msg called before setup");
        }

        for (peer, to_send) in target.iter() {
            if !to_send {
                continue;
            }

            match self.tx_sends.get(&(peer as u8)) {
                Some(tx_send) => {
                    tx_send
                        .send(msg.clone())
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
        }

        Ok(())
    }

    /// Receives a message from some peer by receiving from the recv channel.
    /// Returns a pair of `(peer_id, msg)` on success.
    pub async fn recv_msg(
        &mut self,
    ) -> Result<(ReplicaId, Msg), SummersetError> {
        if let None = self.peer_listener {
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
        conn_read: &mut ReadHalf<'_>,
    ) -> Result<Msg, SummersetError> {
        let msg_len = conn_read.read_u64().await?; // receive length first
        let mut msg_buf: Vec<u8> = vec![0; msg_len as usize];
        conn_read.read_exact(&mut msg_buf[..]).await?;
        let msg = decode_from_slice(&msg_buf)?;
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
                                        pf_error!(me; "error sending to {}: {}", id, e);
                                    } else {
                                        pf_trace!(me; "sent to {} msg {:?}", id, msg);
                                    }
                        },
                        None => break, // channel gets closed and no messages remain
                    }
                },

                // receives new message from peer
                msg = Self::read_msg(&mut conn_read) => {
                    match msg {
                        Ok(msg) => {
                            pf_trace!(me; "recv from {} msg {:?}", id, msg);
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
                            pf_error!(me; "error receiving msg from {}: {}", id, e);
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

    #[test]
    fn hub_setup() -> Result<(), SummersetError> {
        let mut hub: TransportHub<TestMsg> = TransportHub::new(0, 3);
        assert!(tokio_test::block_on(hub.setup(
            "127.0.0.1:52800".parse()?,
            0,
            0
        ))
        .is_err());
        tokio_test::block_on(hub.setup("127.0.0.1:52800".parse()?, 100, 100))?;
        assert!(hub.tx_recv.is_some());
        assert!(hub.rx_recv.is_some());
        assert!(hub.peer_listener.is_some());
        Ok(())
    }

    #[test]
    fn manual_connect() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let mut hub: TransportHub<TestMsg> = TransportHub::new(1, 3);
            hub.setup("127.0.0.1:53801".parse()?, 1, 1).await?;
            while let Err(_) =
                hub.connect_peer(0, "127.0.0.1:53800".parse()?).await
            {
                sleep(Duration::from_millis(1)).await;
            }
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> = TransportHub::new(2, 3);
            hub.setup("127.0.0.1:53802".parse()?, 1, 1).await?;
            while let Err(_) =
                hub.connect_peer(0, "127.0.0.1:53800".parse()?).await
            {
                sleep(Duration::from_millis(1)).await;
            }
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> = TransportHub::new(0, 3);
        tokio_test::block_on(hub.setup("127.0.0.1:53800".parse()?, 1, 1))?;
        let mut peers = HashSet::new();
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
            let mut hub: TransportHub<TestMsg> = TransportHub::new(1, 3);
            hub.setup("127.0.0.1:54801".parse()?, 1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:54800".parse()?),
                (2, "127.0.0.1:54802".parse()?),
            ]))
            .await?;
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> = TransportHub::new(2, 3);
            hub.setup("127.0.0.1:54802".parse()?, 1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:54800".parse()?),
                (1, "127.0.0.1:54801".parse()?),
            ]))
            .await?;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> = TransportHub::new(0, 3);
        tokio_test::block_on(hub.setup("127.0.0.1:54800".parse()?, 1, 1))?;
        let connected =
            tokio_test::block_on(hub.group_connect(HashMap::from([
                (1, "127.0.0.1:54801".parse()?),
                (2, "127.0.0.1:54802".parse()?),
            ])))?;
        assert_eq!(connected, HashSet::from([1, 2]));
        Ok(())
    }

    #[test]
    fn send_recv_api() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let mut hub: TransportHub<TestMsg> = TransportHub::new(1, 3);
            hub.setup("127.0.0.1:55801".parse()?, 1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:55800".parse()?),
                (2, "127.0.0.1:55802".parse()?),
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
            Ok::<(), SummersetError>(())
        });
        tokio::spawn(async move {
            // replica 2
            let mut hub: TransportHub<TestMsg> = TransportHub::new(2, 3);
            hub.setup("127.0.0.1:55802".parse()?, 1, 1).await?;
            hub.group_connect(HashMap::from([
                (0, "127.0.0.1:55800".parse()?),
                (1, "127.0.0.1:55801".parse()?),
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
            Ok::<(), SummersetError>(())
        });
        // replica 0
        let mut hub: TransportHub<TestMsg> = TransportHub::new(0, 3);
        tokio_test::block_on(hub.setup("127.0.0.1:55800".parse()?, 1, 1))?;
        tokio_test::block_on(hub.group_connect(HashMap::from([
            (1, "127.0.0.1:55801".parse()?),
            (2, "127.0.0.1:55802".parse()?),
        ])))?;
        // send a message to 1 and 2
        tokio_test::block_on(
            hub.send_msg(TestMsg("hello".into()), ReplicaMap::new(3, true)?),
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
            hub.send_msg(TestMsg("nice".into()), ReplicaMap::new(3, true)?),
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
            hub.send_msg(
                TestMsg("terminate".into()),
                ReplicaMap::new(3, true)?,
            ),
        )?;
        Ok(())
    }
}
