//! Summerset client API communication stub implementation.

use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, ApiReply};
use crate::client::ClientId;

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Client API connection stub.
pub struct ClientApiStub {
    /// My client ID.
    id: ClientId,
}

impl ClientApiStub {
    /// Creates a new API connection stub.
    pub fn new(id: ClientId) -> Self {
        ClientApiStub { id }
    }

    /// Connects to the given server address, returning a split pair of owned
    /// read/write halves on success.
    pub async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<(ClientSendStub, ClientRecvStub), SummersetError> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_u64(self.id).await?; // send my client ID

        let (read_half, write_half) = stream.into_split();
        let send_stub = ClientSendStub::new(self.id, write_half);
        let recv_stub = ClientRecvStub::new(self.id, read_half);

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
    fn new(id: ClientId, conn_write: OwnedWriteHalf) -> Self {
        ClientSendStub { id, conn_write }
    }

    /// Sends a request to established server connection.
    pub async fn send_req(
        &mut self,
        req: ApiRequest,
    ) -> Result<(), SummersetError> {
        let req_bytes = encode_to_vec(&req)?;
        let req_len = req_bytes.len();
        self.conn_write.write_u64(req_len as u64).await?; // send length first
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
    fn new(id: ClientId, conn_read: OwnedReadHalf) -> Self {
        ClientRecvStub { id, conn_read }
    }

    /// Receives a reply from established server connection.
    pub async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        let reply_len = self.conn_read.read_u64().await?;
        let mut reply_buf: Vec<u8> = vec![0; reply_len as usize];
        self.conn_read.read_exact(&mut reply_buf[..]).await?;
        let reply = decode_from_slice(&reply_buf)?;
        Ok(reply)
    }
}
