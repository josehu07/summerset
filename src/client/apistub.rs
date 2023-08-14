//! Summerset client API communication stub implementation.

use std::io::ErrorKind;
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
        // DEADLOCK AVOIDANCE: we avoid using `write_u64()` and `write_all()`
        // here because, in the case of TCP buffers being full, the service and
        // the client may both be blocking on trying to send (write) into the
        // buffers, resulting in a circular deadlock

        // if last write was not successful, cannot make a new request
        if req.is_some() && !self.req_buf.is_empty() {
            return logged_err!(self.id; "attempting new request while should retry");
        } else if req.is_none() && self.req_buf.is_empty() {
            return logged_err!(self.id; "attempting to retry while buffer is empty");
        } else if req.is_some() {
            // sending a new request, fill req_buf
            assert_eq!(self.req_buf_cursor, 0);
            let req_bytes = encode_to_vec(req.unwrap())?;
            let req_len = req_bytes.len();
            self.req_buf.extend_from_slice(&req_len.to_be_bytes());
            assert_eq!(self.req_buf.len(), 8);
            self.req_buf.extend_from_slice(req_bytes.as_slice());
        } else {
            // retrying last unsuccessful write
            assert!(self.req_buf_cursor < self.req_buf.len());
            pf_debug!(self.id; "retrying last unsuccessful send_req");
        }

        // try until length + the request are all written
        while self.req_buf_cursor < self.req_buf.len() {
            match self
                .conn_write
                .try_write(&self.req_buf[self.req_buf_cursor..])
            {
                Ok(n) => {
                    self.req_buf_cursor += n;
                }
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    pf_debug!(self.id; "send_req would block; TCP buffer full?");
                    return Ok(false);
                }
                Err(err) => return Err(err.into()),
            }
        }

        // everything written, clear req_buf
        self.req_buf.clear();
        self.req_buf_cursor = 0;

        // pf_trace!(self.id; "send req {:?}", req);
        Ok(true)
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
