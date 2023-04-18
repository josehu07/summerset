//! Server internal TCP transport module implementation.

use std::collections::HashMap;

use crate::utils::SummersetError;
use crate::replica::GenericReplica;

use tokio::net::{TcpListener, TcpStream};

use log::error;

/// Server internal TCP transport module.
#[derive(Debug)]
pub struct TransportHub<'r, R: 'r + GenericReplica> {
    /// Reference to protocol-specific replica struct.
    replica: &'r R,

    /// Map from peer ID -> address string.
    peer_addrs: HashMap<u8, String>,

    /// Map from peer ID -> TCP connection.
    peer_conns: HashMap<u8, TcpStream>,
}

impl<'r, R> TransportHub<'r, R> {
    /// Create a new server internal TCP transport hub.
    pub fn new(replica: &'r R) -> Result<Self, SummersetError> {
        Ok(TransportHub {
            replica,
            peer_addrs: HashMap::new(),
            peer_conns: HashMap::new(),
        })
    }

    pub async fn connect_peer(
        &mut self,
        id: u8,
        addr: String,
    ) -> Result<(), SummersetError> {
        if self.peer_addrs.contains_key(&id) {
            return logged_err!(
                self.replica.id(),
                "peer ID {} already in peer_addrs",
                id
            );
        }

        let mut stream = TcpStream::connect(format!("http://{}", addr)).await?;
        self.peer_addrs.insert(id, addr);
        self.peer_conns.insert(id, stream);

        Ok(())
    }
}
