//! Server internal TCP transport module implementation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::core::utils::{SummersetError, ReplicaId, ReplicaMap};
use crate::core::replica::GenericReplica;

use serde::{Serialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use log::{trace, debug, error};

/// Server internal TCP transport module.
#[derive(Debug)]
pub struct TransportHub<
    'r,
    Rpl: 'r + GenericReplica,
    Msg: Serialize + DeserializeOwned,
> {
    /// Reference to protocol-specific replica struct.
    replica: &'r Rpl,

    /// Sender side of the send channel.
    tx_send: Option<mpsc::Sender<(Msg, ReplicaMap)>>,

    /// Sender side of the recv channel, stored here so that we can clone it
    /// to a message recver thread whenever a new peer is connected.
    tx_recv: Option<mpsc::Sender<Msg>>,

    /// Receiver side of the recv channel.
    rx_recv: Option<mpsc::Receiver<Msg>>,

    /// TCP Listener for peer connections, if there is one.
    peer_listener: Option<TcpListener>,

    /// Map from peer ID -> TCP connection used in sending direction, shared
    /// with the message sender thread.
    peer_send_conns: Arc<Mutex<HashMap<ReplicaId, TcpStream>>>,

    /// Map from peer ID -> TCP connection used in recving direction. Each
    /// stream is shared with the corresponding message receiver thread.
    peer_recv_conns: HashMap<ReplicaId, Arc<Mutex<TcpStream>>>,

    /// Join handle of the message sender thread, if there is one.
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
        if let None = self.msg_sender_handle {
            return logged_err!(
                self.replica.id(),
                "connect_peer called before setup"
            );
        }
        if id >= self.replica.population() {
            return logged_err!(
                self.replica.id(),
                "invalid peer ID {} to connect",
                id
            );
        }
        if self.peer_send_conns.contains_key(&id) {
            return logged_err!(
                self.replica.id(),
                "peer ID {} already in peer_send_conns",
                id
            );
        }

        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(self.replica.id()).await?; // send my ID

        let mut send_conns_guard = self.peer_send_conns.lock().unwrap();
        send_conns_guard.insert(id, stream);

        pf_debug!(self.replica.id(), "connected to peer {}", id);
        Ok(())
    }

    /// Waits for a connection attempt from some peer, and spawns the
    /// corresponding message recver thread. Should be called after `setup`.
    /// Returns the connecting peer's ID if successful.
    pub async fn wait_on_peer(&mut self) -> Result<ReplicaId, SummersetError> {
        if let None = self.msg_sender_handle {
            return logged_err!(
                self.replica.id(),
                "wait_on_peer called before setup"
            );
        }

        let mut stream = self.peer_listener.accept().await?;
        let id = stream.read_u8().await?; // receive connecting peer's ID

        if id >= self.replica.population() {
            return logged_err!(
                self.replica.id(),
                "invalid peer ID {} waited on",
                id
            );
        }
        if self.peer_recv_conns.contains_key(&id) {
            return logged_err!(
                self.replica.id(),
                "peer ID {} already in peer_recv_conns",
                id
            );
        }

        self.peer_recv_conns
            .insert(id, Arc::new(Mutex::new(stream)));

        let msg_recver_handle = tokio::spawn(TransportHub::msg_recver_thread(
            self.replica.id(),
            id,
            self.peer_recv_conns[id].clone(),
            self.tx_recv.clone(),
        ));
        self.msg_recver_handles.insert(id, msg_recver_handle);

        pf_debug!(self.replica.id(), "waited on peer {}", id);
        Ok(())
    }

    /// Sends a message to specified peers by sending to the send channel.
    pub async fn send_msg(
        &mut self,
        msg: Msg,
        target: ReplicaMap,
    ) -> Result<(), SummersetError> {
        match self.tx_send {
            Some(ref tx_send) => tx_send.send((msg, target)).await?,
            None => logged_err!(self.replica.id(), "tx_send not created yet"),
        }
    }

    /// Receives a message from some peer by receiving from the recv channel.
    pub async fn recv_msg(&mut self) -> Result<Msg, SummersetError> {
        match self.rx_recv {
            Some(ref mut rx_recv) => match rx_recv.recv().await {
                Some(msg) => msg,
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
                            }
                            pf_trace!(me, "sent to {} msg {:?}", id, msg);
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
        let msg_buf: Vec<u8> = vec![0; std::mem::size_of::<Msg>()];
        conn.read_exact(&mut msg_buf[..]).await?;
        let msg = decode_from_slice(&msg_buf)?;
        Ok(msg)
    }

    /// Message recver thread function.
    async fn msg_recver_thread(
        me: ReplicaId,
        id: ReplicaId, // corresponding peer's ID
        recv_conn: Arc<Mutex<TcpStream>>,
        tx_recv: mpsc::Sender<Msg>,
    ) {
        pf_debug!(me, "msg_recver thread for {} spawned", id);

        let mut recv_conn_guard = recv_conn.lock().unwrap();

        loop {
            match TransportHub::read_msg(&mut recv_conn_guard).await {
                Ok(msg) => {
                    pf_trace!(me, "recv from {} msg {:?}", id, msg);
                    if let Err(e) = tx_recv.send(msg).await {
                        pf_error!(
                            me,
                            "error sending to tx_recv from {}: {}",
                            id,
                            e
                        );
                    }
                }

                Err(e) => {
                    pf_error!(me, "error receiving msg from {}: {}", id, e)
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
    use tokio::time::{sleep, Duration};

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
    fn connect_peers() -> Result<(), SummersetError> {
        tokio::spawn(async move {
            // replica 1
            let replica = DummyReplica::new(1, 3, "127.0.0.1:52801".into());
            let mut hub = TransportHub::new(&replica);
            tokio_test::block_on(hub.setup(1, 1))?;
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
            tokio_test::block_on(hub.setup(1, 1))?;
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
        let ref_peers = HashSet::new();
        ref_peers.insert(1);
        ref_peers.insert(2);
        assert_eq!(peers, ref_peers);
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
            tokio_test::block_on(hub.setup(2, 2))?;
            let peers = HashSet::new();
            while let Err(e) =
                hub.connect_peer(0, "127.0.0.1:52800".into()).await
            {
                sleep(Duration::from_millis(1)).await;
            }
            peers.insert(hub.wait_on_peer()?);
            peers.insert(hub.wait_on_peer()?);
            while let Err(e) =
                hub.connect_peer(2, "127.0.0.1:52802".into()).await
            {
                sleep(Duration::from_millis(1)).await;
            }
            assert!(peers.contains(&0));
            assert!(peers.contains(&2));
            // connection phase finished
        });
        tokio::spawn(async move {
            // replica 2
            let replica = DummyReplica::new(2, 3, "127.0.0.1:52802".into());
            let mut hub = TransportHub::new(&replica);
            tokio_test::block_on(hub.setup(2, 2))?;
            let peers = HashSet::new();
            while let Err(e) =
                hub.connect_peer(0, "127.0.0.1:52800".into()).await
            {
                sleep(Duration::from_millis(1)).await;
            }
            peers.insert(hub.wait_on_peer()?);
            while let Err(e) =
                hub.connect_peer(1, "127.0.0.1:52801".into()).await
            {
                sleep(Duration::from_millis(1)).await;
            }
            peers.insert(hub.wait_on_peer()?);
            assert!(peers.contains(&0));
            assert!(peers.contains(&1));
            // connection phase finished
        });
        // replica 0
        let replica = DummyReplica::new(0, 3, "127.0.0.1:52800".into());
        let mut hub = TransportHub::new(&replica);
        tokio_test::block_on(hub.setup(2, 2))?;
        let peers = HashSet::new();
        peers.insert(tokio_test::block_on(hub.wait_on_peer())?);
        while let Err(e) =
            tokio_test::block_on(hub.connect_peer(1, "127.0.0.1:52801".into()))
        {
            tokio_test::block_on(sleep(Duration::from_millis(1)));
        }
        peers.insert(tokio_test::block_on(hub.wait_on_peer())?);
        Ok(())
    }
}
