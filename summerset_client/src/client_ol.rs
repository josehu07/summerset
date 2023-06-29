//! Open-loop client implementation.

use std::sync::atomic::{AtomicU64, Ordering};

use summerset::{
    GenericClient, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    SummersetError,
};

/// Open-loop client struct.
pub struct ClientOpenLoop {
    /// Client ID.
    id: ClientId,

    /// Protocol-specific client stub.
    stub: Box<dyn GenericClient>,

    /// Next request ID, monotonically increasing.
    next_req: AtomicU64,
}

impl ClientOpenLoop {
    /// Creates a new open-loop client.
    pub fn new(id: ClientId, stub: Box<dyn GenericClient>) -> Self {
        ClientOpenLoop {
            id,
            stub,
            next_req: AtomicU64::new(0),
        }
    }

    /// Make a Get request.
    pub async fn issue_get(&self, key: &str) -> Result<(), SummersetError> {
        let req_id = self.next_req.fetch_add(1, Ordering::AcqRel);

        self.stub
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

        self.stub
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
    pub async fn wait_reply(&self) -> Result<(), SummersetError> {
        // TODO: finish me
        unimplemented!()
    }
}
