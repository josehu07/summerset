//! Summerset generic client trait to be implemented by all protocol-specific
//! client stub structs.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::core::utils::SummersetError;
use crate::core::replica::ReplicaId;
use crate::core::external::{ApiRequest, ApiReply};

use async_trait::async_trait;

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Client stub ID type.
pub type ClientId = u64;

/// Client trait to be implement by all protocol-specific client structs.
#[async_trait]
pub trait GenericClient {
    /// Creates a new client and establishes connection to the service.
    async fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
    ) -> Result<Self, SummersetError>;

    /// Send a request to the Summerset service; may contain protocol-specific
    /// logic.
    async fn send_req(&self, req: ApiRequest) -> Result<(), SummersetError>;

    /// Receive a reply from the Summerset service; may contain protocol-
    /// specific logic.
    async fn recv_reply(&self) -> Result<ApiReply, SummersetError>;
}

/// Client stub struct that implements common client-side procedures.
/// Protocol-specific clients should include such a struct as their
/// communication module.
pub struct ClientStub {
    /// My client ID.
    id: ClientId,

    /// Server address connected.
    server: Option<SocketAddr>,

    /// Read-half split of the TCP connection stream.
    conn_read: Option<OwnedReadHalf>,

    /// Write-half split of the TCP connection stream.
    conn_write: Option<OwnedWriteHalf>,
}

impl ClientStub {
    /// Creates a new client stub.
    pub fn new(id: ClientId) -> Self {
        ClientStub {
            id,
            server: None,
            conn_read: None,
            conn_write: None,
        }
    }

    /// Establishes connection to given server address.
    pub async fn connect(
        &mut self,
        server: SocketAddr,
    ) -> Result<(), SummersetError> {
        let mut stream = TcpStream::connect(server).await?;
        stream.write_u64(self.id).await?; // send my client ID

        // split TcpStream into read/write halves
        let (read_half, write_half) = stream.into_split();
        self.conn_read = Some(read_half);
        self.conn_write = Some(write_half);
    }

    /// Send a request to established server connection.
    pub async fn write_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        let req_bytes = encode_to_vec(&req)?;
        let req_len = req_bytes.len();
        self.conn_write.write_u64(req_len).await?; // send length first
        self.conn_write.write_all(&req_bytes[..]).await?;
        Ok(())
    }

    /// Receive a reply from established server connection.
    pub async fn read_reply(&mut self) -> Result<ApiReply, SummersetError> {
        let reply_len = self.conn_read.read_u64().await?;
        let reply_buf: Vec<u8> = vec![0; reply_len];
        self.conn_read.read_exact(&mut reply_buf[..]).await?;
        let reply = decode_from_slice(&reply_buf)?;
        Ok(reply)
    }
}
