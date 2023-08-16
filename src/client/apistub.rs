//! Summerset client API communication stub implementation.

use std::net::SocketAddr;

use crate::utils::{SummersetError, safe_tcp_read, safe_tcp_write};
use crate::server::{ApiRequest, ApiReply};
use crate::client::ClientId;

use bytes::BytesMut;

use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::io::AsyncWriteExt;

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
    id: ClientId,

    /// Write-half split of the TCP connection stream.
    conn_write: OwnedWriteHalf,

    /// Request write buffer for deadlock avoidance.
    req_buf: BytesMut,

    /// Request write buffer cursor at first unwritten byte.
    req_buf_cursor: usize,
}

impl ClientSendStub {
    /// Creates a new write stub.
    fn new(id: ClientId, conn_write: OwnedWriteHalf) -> Self {
        ClientSendStub {
            id,
            conn_write,
            req_buf: BytesMut::with_capacity(8 + 1024),
            req_buf_cursor: 0,
        }
    }

    /// Sends a request to established server connection. Returns:
    ///   - `Ok(true)` if successful
    ///   - `Ok(false)` if socket full and may block; in this case, the input
    ///                 request is saved and the next calls to `send_req()`
    ///                 must give arg `req == None` to retry until successful
    ///                 (typically after doing a few `recv_reply()`s to free
    ///                 up some buffer space)
    ///   - `Err(err)` if any unexpected error occurs
    pub fn send_req(
        &mut self,
        req: Option<&ApiRequest>,
    ) -> Result<bool, SummersetError> {
        if req.is_none() {
            pf_debug!(self.id; "retrying last unsuccessful send_req");
        }
        let retrying = safe_tcp_write(
            &mut self.req_buf,
            &mut self.req_buf_cursor,
            &self.conn_write,
            req,
        )?;

        // pf_trace!(self.id; "send req {:?}", req);
        if retrying {
            pf_debug!(self.id; "send_req would block; TCP buffer full?");
        }
        Ok(retrying)
    }

    /// Forgets about the write-half TCP connection.
    pub fn forget(self) {
        self.conn_write.forget();
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
        let reply =
            safe_tcp_read(&mut self.reply_buf, &mut self.conn_read).await?;

        // pf_trace!(self.id; "recv reply {:?}", reply);
        Ok(reply)
    }
}
