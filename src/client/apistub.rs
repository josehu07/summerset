//! Summerset client API communication stub implementation.

use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{ApiRequest, ApiReply};
use crate::client::ClientId;

use bytes::{Bytes, BytesMut};

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
        pf_info!(self.id; "connecting to server '{}'...", addr);
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
    _id: ClientId,

    /// Write-half split of the TCP connection stream.
    conn_write: OwnedWriteHalf,
}

impl ClientSendStub {
    /// Creates a new write stub.
    fn new(id: ClientId, conn_write: OwnedWriteHalf) -> Self {
        ClientSendStub {
            _id: id,
            conn_write,
        }
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

        // pf_trace!(self.id; "send req {:?}", req);
        Ok(())
    }
}

/// Client read stub that owns a TCP read half.
pub struct ClientRecvStub {
    /// My client ID.
    _id: ClientId,

    /// Read-half split of the TCP connection stream.
    conn_read: OwnedReadHalf,

    /// Reply read buffer for cancellation safety.
    reply_buf: BytesMut,
}

impl ClientRecvStub {
    /// Creates a new read stub.
    fn new(id: ClientId, conn_read: OwnedReadHalf) -> Self {
        ClientRecvStub {
            _id: id,
            conn_read,
            reply_buf: BytesMut::with_capacity(8 + 1024),
        }
    }

    /// Receives a reply from established server connection.
    pub async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        // CANCELLATION SAFETY: we cannot use `read_u64()` and `read_exact()`
        // here because this function is used as a `tokio::select!` branch and
        // that those two methods are not cancellation-safe

        // read length of reply first
        assert!(self.reply_buf.capacity() >= 8);
        while self.reply_buf.len() < 8 {
            // reply_len not wholesomely read from socket before last cancellation
            self.conn_read.read_buf(&mut self.reply_buf).await?;
        }
        let reply_len =
            u64::from_be_bytes(self.reply_buf[..8].try_into().unwrap());

        // then read the reply itself
        let reply_end = 8 + reply_len as usize;
        if self.reply_buf.capacity() < reply_end {
            // capacity not big enough, reserve more space
            self.reply_buf
                .reserve(reply_end - self.reply_buf.capacity());
        }
        while self.reply_buf.len() < reply_end {
            self.conn_read.read_buf(&mut self.reply_buf).await?;
        }
        let reply = decode_from_slice(&self.reply_buf[8..reply_end])?;

        // if reached this point, no further cancellation to this call is
        // possible (because there are no more awaits ahead); discard bytes
        // used in this call
        if self.reply_buf.len() > reply_end {
            let buf_tail = Bytes::copy_from_slice(&self.reply_buf[reply_end..]);
            self.reply_buf.clear();
            self.reply_buf.extend_from_slice(&buf_tail);
        } else {
            self.reply_buf.clear();
        }

        // pf_trace!(self.id; "recv reply {:?}", reply);
        Ok(reply)
    }
}
