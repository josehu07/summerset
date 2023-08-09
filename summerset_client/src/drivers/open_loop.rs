//! Open-loop client-side driver implementation.

use std::collections::HashSet;

use tokio::time::Duration;

use summerset::{
    GenericClient, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, Timer, SummersetError, pf_debug, pf_info, pf_error, logged_err,
};

/// Open-loop driver struct.
pub struct DriverOpenLoop {
    /// Client ID.
    id: ClientId,

    /// Protocol-specific client stub.
    stub: Box<dyn GenericClient>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,

    /// Set of pending requests whose reply has not been received.
    pending_reqs: HashSet<RequestId>,

    /// Reply timeout timer.
    timer: Timer,

    /// Reply timeout duration.
    timeout: Duration,
}

impl DriverOpenLoop {
    /// Creates a new open-loop client.
    pub fn new(
        id: ClientId,
        stub: Box<dyn GenericClient>,
        timeout: Duration,
    ) -> Self {
        DriverOpenLoop {
            id,
            stub,
            next_req: 0,
            pending_reqs: HashSet::new(),
            timer: Timer::new(),
            timeout,
        }
    }

    /// Wait on a reply from the service with timeout. Returns `Ok(None)` if
    /// timed-out.
    async fn recv_reply_with_timeout(
        &mut self,
    ) -> Result<Option<ApiReply>, SummersetError> {
        self.timer.kickoff(self.timeout)?;

        tokio::select! {
            () = self.timer.timeout() => {
                pf_debug!(self.id; "timed-out waiting for reply");
                Ok(None)
            }

            reply = self.stub.recv_reply() => {
                self.timer.cancel()?; // cancel current deadline
                Ok(Some(reply?))
            }
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
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader or timeout
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn wait_reply(
        &mut self,
    ) -> Result<Option<(RequestId, CommandResult)>, SummersetError> {
        let reply = self.recv_reply_with_timeout().await?;
        match reply {
            Some(ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            }) => {
                if !self.pending_reqs.contains(&reply_id) {
                    logged_err!(self.id; "request ID {} not in pending set", reply_id)
                } else {
                    self.pending_reqs.remove(&reply_id);
                    if let Some(res) = cmd_result {
                        Ok(Some((reply_id, res)))
                    } else {
                        Ok(None)
                    }
                }
            }

            None => Ok(None), // timed-out

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
