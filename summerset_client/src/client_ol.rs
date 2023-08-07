//! Open-loop client implementation.

use std::collections::HashSet;

use summerset::{
    GenericClient, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, SummersetError, pf_info, pf_error, logged_err,
};

/// Open-loop client struct.
pub struct ClientOpenLoop {
    /// Client ID.
    id: ClientId,

    /// Protocol-specific client stub.
    stub: Box<dyn GenericClient>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,

    /// Set of pending requests whose reply has not been received.
    pending_reqs: HashSet<RequestId>,
}

impl ClientOpenLoop {
    /// Creates a new open-loop client.
    pub fn new(id: ClientId, stub: Box<dyn GenericClient>) -> Self {
        ClientOpenLoop {
            id,
            stub,
            next_req: 0,
            pending_reqs: HashSet::new(),
        }
    }

    /// Make a Get request. Returns request ID for later reference.
    pub async fn issue_get(
        &mut self,
        key: &str,
    ) -> Result<RequestId, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.stub
            .send_req(ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { key: key.into() },
            })
            .await?;

        self.pending_reqs.insert(req_id);
        Ok(req_id)
    }

    /// Make a Put request. Returns request ID for later reference.
    pub async fn issue_put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<RequestId, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.stub
            .send_req(ApiRequest::Req {
                id: req_id,
                cmd: Command::Put {
                    key: key.into(),
                    value: value.into(),
                },
            })
            .await?;

        self.pending_reqs.insert(req_id);
        Ok(req_id)
    }

    /// Wait for the next reply. Returns the request ID and:
    ///   - `Ok(Some(cmd_result))` if request successful
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn wait_reply(
        &mut self,
    ) -> Result<(RequestId, Option<CommandResult>), SummersetError> {
        let reply = self.stub.recv_reply().await?;
        match reply {
            ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            } => {
                if !self.pending_reqs.contains(&reply_id) {
                    logged_err!(self.id; "request ID {} not in pending set", reply_id)
                } else {
                    self.pending_reqs.remove(&reply_id);
                    Ok((reply_id, cmd_result))
                }
            }
            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }

    /// Send leave notification. The leave action is left synchronous.
    pub async fn leave(&mut self) -> Result<(), SummersetError> {
        self.stub.send_req(ApiRequest::Leave).await?;

        let reply = self.stub.recv_reply().await?;
        match reply {
            ApiReply::Leave => {
                pf_info!(self.id; "left current server connection");
                Ok(())
            }
            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }
}
