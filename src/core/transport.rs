//! Server internal TCP transport module implementation.

use std::collections::HashMap;

use crate::core::utils::SummersetError;
use crate::core::replica::GenericReplica;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use log::{debug, error};

/// Server internal TCP transport module.
#[derive(Debug)]
pub struct TransportHub<'r, R: 'r + GenericReplica> {
    /// Reference to protocol-specific replica struct.
    replica: &'r R,

    /// TCP Listener for peer connections, if there is one.
    peer_listener: Option<TcpListener>,

    /// Map from peer ID -> TCP connection.
    peer_conns: HashMap<u8, TcpStream>,

    /// Join handle of the message sender thread, if there is one.
    msg_sender_handle: Option<JoinHandle<()>>,

    /// Map from peer ID -> message listener thread join handles.
    msg_recver_handles: HashMap<u8, JoinHandle<()>>,
}

impl<'r, R> TransportHub<'r, R> {
    /// Creates a new server internal TCP transport hub.
    pub fn new(replica: &'r R) -> Result<Self, SummersetError> {
        Ok(TransportHub {
            replica,
            peer_listener: None,
            tx_chan_recv: None,
            peer_conns: HashMap::new(),
            msg_sender_handle: None,
            msg_recver_handles: HashMap::new(),
        })
    }

    /// Spawns the message sender thread. Creates an send channel for sending
    /// messages to peers and an recv channel for listening on peers' messages.
    /// Creates a TCP listener for peer connections as well. Returns a tuple
    /// `(tx_send, rx_recv)` if ok.
    pub async fn setup(
        &mut self,
        chan_send_cap: usize,
        chan_recv_cap: usize,
    ) -> (mpsc::Sender<()>, mpsc::Receiver<()>) {
        if let Some(_) = self.msg_sender_handle {
            return logged_err!(
                self.replica.id(),
                "msg_sender thread already spawned"
            );
        }

        let (tx_send, mut rx_send) = mpsc::channel(chan_send_cap);
        let (tx_recv, mut rx_recv) = mpsc::channel(chan_recv_cap);
        self.tx_chan_recv = Some(tx_recv);

        let msg_sender_handle = tokio::spawn(self.msg_sender_thread(rx_send));
        self.msg_sender_handle = Some(msg_sender_handle);

        let peer_listener = TcpListener::bind(self.replica.addr()).await?;
        self.peer_listener = Some(peer_listener);

        (tx_send, rx_recv)
    }

    /// Connects to a new peer replica, and spawns the corresponding message
    /// recver thread. Should be called after `setup`.
    pub async fn connect_peer(
        &mut self,
        id: u8,
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
        if self.peer_conns.contains_key(&id) {
            return logged_err!(
                self.replica.id(),
                "peer ID {} already in peer_conns",
                id
            );
        }

        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u8(self.replica.id()).await?; // send my ID

        self.peer_conns.insert(id, stream);

        let msg_recver_handle =
            tokio::spawn(self.msg_recver_thread(self.tx_chan_recv.clone()));
        self.peer_recver_handles.insert(id, msg_recver_handle);

        pf_debug!(self.replica.id(), "connected to peer {}", id);
        Ok(())
    }

    /// Wait for a connection attempt from some peer, and spawns the
    /// corresponding message recver thread. Should be called after `setup`.
    /// Returns the connecting peer's ID if successful.
    pub async fn wait_on_peer(&mut self) -> Result<u8, SummersetError> {
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
        if self.peer_conns.contains_key(&id) {
            return logged_err!(
                self.replica.id(),
                "peer ID {} already in peer_conns",
                id
            );
        }

        self.peer_conns.insert(id, stream);

        let msg_recver_handle =
            tokio::spawn(self.msg_recver_thread(self.tx_chan_recv.clone()));
        self.peer_recver_handles.insert(id, msg_recver_handle);

        pf_debug!(self.replica.id(), "waited on peer {}", id);
        Ok(())
    }

    async fn msg_sender_thread() {}
}
