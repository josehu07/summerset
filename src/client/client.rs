//! Summerset generic client trait to be implemented by all protocol-specific
//! client stub structs.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, ApiRequest, ApiReply};

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
    /// Creates a new client stub.
    fn new(
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>, // protocol-specific config in TOML format
    ) -> Result<Self, SummersetError>
    where
        Self: Sized;

    /// Establishes connection to the service according to protocol-specific
    /// logic, returning two owned TCP connection halves (for possibly
    /// open-loop clients).
    async fn connect(
        &mut self,
    ) -> Result<(ClientSendStub, ClientRecvStub), SummersetError>;

    /// Procedure to connect to the given server address and splitting the
    /// result TCP stream into read/write halves. Default implementation is
    /// provided here, so most protocol-specific implementations can just use
    /// it out-of-the-box.
    async fn connect_server(
        id: ClientId,
        addr: SocketAddr,
    ) -> Result<(ClientSendStub, ClientRecvStub), SummersetError> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u64(id).await?; // send my client ID

        let (read_half, write_half) = stream.into_split();
        let send_stub = ClientSendStub::new(id, write_half);
        let recv_stub = ClientRecvStub::new(id, read_half);

        Ok((send_stub, recv_stub))
    }
}

/// Client write stub that owns a TCP write half.
pub struct ClientSendStub {
    /// My client ID.
    id: ClientId,

    /// Write-half split of the TCP connection stream.
    conn_write: OwnedWriteHalf,
}

impl ClientSendStub {
    /// Creates a new write stub.
    pub fn new(id: ClientId, conn_write: OwnedWriteHalf) -> Self {
        ClientSendStub { id, conn_write }
    }

    /// Sends a request to established server connection.
    pub async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        let req_bytes = encode_to_vec(&req)?;
        let req_len = req_bytes.len();
        self.conn_write.write_u64(req_len).await?; // send length first
        self.conn_write.write_all(&req_bytes[..]).await?;
        Ok(())
    }
}

/// Client read stub that owns a TCP read half.
pub struct ClientRecvStub {
    /// My client ID.
    id: ClientId,

    /// Read-half split of the TCP connection stream.
    conn_read: OwnedReadHalf,
}

impl ClientRecvStub {
    /// Creates a new read stub.
    pub fn new(id: ClientId, conn_read: OwnedReadHalf) -> Self {
        ClientRecvStub { id, conn_read }
    }

    /// Receives a reply from established server connection.
    pub async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        let reply_len = self.conn_read.read_u64().await?;
        let reply_buf: Vec<u8> = vec![0; reply_len];
        self.conn_read.read_exact(&mut reply_buf[..]).await?;
        let reply = decode_from_slice(&reply_buf)?;
        Ok(reply)
    }
}
