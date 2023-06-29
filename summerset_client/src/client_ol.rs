//! Open-loop client implementation.

use std::sync::atomic::{AtomicU64, Ordering};

use summerset::{
    ClientId, ClientSendStub, ClientRecvStub, Command, CommandResult,
    ApiRequest, ApiReply, SummersetError,
};

/// Open-loop client struct.
pub struct ClientOpenLoop {
    /// Client ID.
    id: ClientId,

    /// ApiRequest send stub.
    send_stub: ClientSendStub,

    /// ApiReply receive stub.
    recv_stub: ClientRecvStub,

    /// Next request ID, monotonically increasing.
    next_req: AtomicU64,
}

impl ClientOpenLoop {
    /// Creates a new open-loop client.
    pub fn new(
        id: ClientId,
        send_stub: ClientSendStub,
        recv_stub: ClientRecvStub,
    ) -> Self {
        ClientOpenLoop {
            id,
            send_stub,
            recv_stub,
            next_req: AtomicU64::new(0),
        }
    }

    /// Make a Get request.
    pub async fn issue_get(&self, key: &str) -> Result<(), SummersetError> {
        let req_id = self.next_req.fetch_add(1, Ordering::AcqRel);

        self.send_stub
            .send_req(ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { key: key.into() },
            })
            .await?;
        Ok(())
    }

    /// Make a Put request.
    pub async fn issue_put(
        &self,
        key: &str,
        value: &str,
    ) -> Result<(), SummersetError> {
        let req_id = self.next_req.fetch_add(1, Ordering::AcqRel);

        self.send_stub
            .send_req(ApiRequest::Req {
                id: req_id,
                cmd: Command::Put {
                    key: key.into(),
                    value: value.into(),
                },
            })
            .await?;
        Ok(())
    }

    /// Wait for the next reply.
    pub async fn wait_reply(&self) -> Result<(), SummersetError> {}
}
