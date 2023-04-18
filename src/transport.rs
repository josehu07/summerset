//! Server internal RPC transport module implementation.

use std::collections::HashMap;

use crate::utils::SummersetError;

use tonic::{Request, Response, Status};
use tonic::transport;

/// Server internal RPC transport module.
#[derive(Debug)]
pub struct TransportHub<RpcClient> {
    /// Map from peer ID -> RPC client connection.
    peers: HashMap<u8, RpcClient>,
}

impl<RpcClient> TransportHub<RpcClient> {
    /// Create a new server internal RPC transport hub.
    pub fn new() -> Self {
        TransportHub {
            peers: HashMap::new(),
        }
    }

    pub async fn connect_peer(
        id: u8,
        addr: String,
    ) -> Result<(), SummersetError> {
        let addr_str = format!("http://{}", &addr);
        RpcClient::connect(addr_str).await?;
    }
}
