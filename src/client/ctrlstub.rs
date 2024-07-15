//! Summerset client -> manager oracle control API stub implementation.

use std::net::SocketAddr;

use crate::client::ClientId;
use crate::manager::{CtrlReply, CtrlRequest};
use crate::utils::{
    safe_tcp_read, safe_tcp_write, tcp_connect_with_retry, SummersetError, ME,
};

use bytes::BytesMut;

use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Client -> manager oracle control API stub.
pub struct ClientCtrlStub {
    /// My client ID.
    pub id: ClientId,

    /// Write-half split of the TCP connection stream.
    conn_write: OwnedWriteHalf,

    /// Request write buffer for deadlock avoidance.
    req_buf: BytesMut,

    /// Request write buffer cursor at first unwritten byte.
    req_buf_cursor: usize,

    /// Read-half split of the TCP connection stream.
    conn_read: OwnedReadHalf,

    /// Reply read buffer for cancellation safety.
    reply_buf: BytesMut,
}

impl ClientCtrlStub {
    /// Creates a new control API stub and connects to the manager.
    pub(crate) async fn new_by_connect(
        bind_addr: SocketAddr,
        manager: SocketAddr,
    ) -> Result<Self, SummersetError> {
        let mut stream = tcp_connect_with_retry(bind_addr, manager, 10).await?;
        let id = stream.read_u64().await?; // receive my client ID
        let (read_half, write_half) = stream.into_split();

        ME.get_or_init(|| id.to_string());

        Ok(ClientCtrlStub {
            id,
            conn_write: write_half,
            req_buf: BytesMut::with_capacity(8 + 1024),
            req_buf_cursor: 0,
            conn_read: read_half,
            reply_buf: BytesMut::with_capacity(8 + 1024),
        })
    }

    /// Sends a request to established manager connection. Returns:
    ///   - `Ok(true)` if successful
    ///   - `Ok(false)` if socket full and may block; in this case, the input
    ///                 request is saved and the next calls to `send_req()`
    ///                 must give arg `req == None` to retry until successful
    ///                 (typically after doing a few `recv_reply()`s to free
    ///                 up some buffer space)
    ///   - `Err(err)` if any unexpected error occurs
    pub fn send_req(
        &mut self,
        req: Option<&CtrlRequest>,
    ) -> Result<bool, SummersetError> {
        if req.is_none() {
            pf_debug!("retrying last unsuccessful send_req");
        }
        let no_retry = safe_tcp_write(
            &mut self.req_buf,
            &mut self.req_buf_cursor,
            &self.conn_write,
            req,
        )?;

        // pf_trace!("send req {:?}", req);
        if !no_retry {
            pf_debug!("send_req would block; TCP buffer / eth queue full?");
        }
        Ok(no_retry)
    }

    /// Sends a request to established manager connection, retrying immediately
    /// on `WouldBlock` failure. This shortcut should only be used in places
    /// where TCP write blocking is not expected.
    pub fn send_req_insist(
        &mut self,
        req: &CtrlRequest,
    ) -> Result<(), SummersetError> {
        let mut success = self.send_req(Some(req))?;
        while !success {
            success = self.send_req(None)?;
        }

        Ok(())
    }

    /// Receives a reply from established manager connection.
    pub async fn recv_reply(&mut self) -> Result<CtrlReply, SummersetError> {
        let reply =
            safe_tcp_read(&mut self.reply_buf, &mut self.conn_read).await?;

        // pf_trace!("recv reply {:?}", reply);
        Ok(reply)
    }
}

// Unit tests are done together with `manager::reactor`.
