//! Summerset client API communication stub implementation.

use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::client::ClientId;
use crate::server::{ApiReply, ApiRequest};
use crate::utils::{SummersetError, safe_tcp_read, safe_tcp_write, tcp_connect_with_retry};

/// Client API connection stub.
pub(crate) struct ClientApiStub {
    /// My client ID.
    _id: ClientId,

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

impl ClientApiStub {
    /// Creates a new API connection stub by connecting to the given server.
    pub(crate) async fn new_by_connect(
        id: ClientId,
        server: SocketAddr,
    ) -> Result<Self, SummersetError> {
        let mut stream = tcp_connect_with_retry(server, 150).await?;
        stream.write_u64(id).await?; // send my client ID
        let (read_half, write_half) = stream.into_split();

        Ok(ClientApiStub {
            _id: id,
            conn_write: write_half,
            req_buf: BytesMut::with_capacity(8 + 1024),
            req_buf_cursor: 0,
            conn_read: read_half,
            reply_buf: BytesMut::with_capacity(8 + 1024),
        })
    }

    /// Sends a request to established server connection. Returns:
    ///   - `Ok(true)` if successful
    ///   - `Ok(false)` if socket full and may block; in this case, the input
    ///     request is saved and the next calls to `send_req()`
    ///     must give arg `req == None` to retry until successful
    ///     (typically after doing a few `recv_reply()`s to free
    ///     up some buffer space)
    ///   - `Err(err)` if any unexpected error occurs
    pub(crate) fn send_req(&mut self, req: Option<&ApiRequest>) -> Result<bool, SummersetError> {
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

    /// Sends a request to established server connection, retrying whenever the
    /// connection becomes writable upon `WouldBlock` failures.
    pub(crate) async fn send_req_insist(&mut self, req: &ApiRequest) -> Result<(), SummersetError> {
        let mut sent = self.send_req(Some(req))?;
        while !sent {
            self.conn_write.writable().await?;
            sent = self.send_req(None)?;
        }
        Ok(())
    }

    /// Receives a reply from established server connection.
    pub(crate) async fn recv_reply(&mut self) -> Result<ApiReply, SummersetError> {
        let reply = safe_tcp_read(&mut self.reply_buf, &mut self.conn_read).await?;

        // pf_trace!("recv reply {:?}", reply);
        Ok(reply)
    }
}

// More unit tests are done together with `server::external`.

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::time::{self, Duration};

    use super::*;
    use crate::server::Command;
    use crate::utils::test_addr_at_offset;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_insist_waits_for_reader() -> Result<(), SummersetError> {
        let listener = TcpListener::bind(test_addr_at_offset(183)).await?;
        let req = ApiRequest::Req {
            id: 0,
            cmd: Command::Put {
                key: "k".into(),
                value: "v".repeat(1 << 16),
            },
        };
        // the server starts reading only after a while
        let reader = tokio::spawn({
            let req = req.clone();
            async move {
                let (mut conn, _) = listener.accept().await?;
                conn.read_u64().await?; // client ID
                time::sleep(Duration::from_millis(100)).await;
                let mut read_buf = BytesMut::new();
                for _ in 0..64 {
                    assert_eq!(
                        safe_tcp_read::<ApiRequest, _>(&mut read_buf, &mut conn).await?,
                        req
                    );
                }
                Ok::<_, SummersetError>(())
            }
        });
        let mut stub = ClientApiStub::new_by_connect(7, test_addr_at_offset(183)).await?;
        for _ in 0..64 {
            stub.send_req_insist(&req).await?;
        }
        time::timeout(Duration::from_secs(5), reader)
            .await
            .map_err(SummersetError::msg)???;
        Ok(())
    }
}
