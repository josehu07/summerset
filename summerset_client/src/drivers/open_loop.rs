//! Open-loop client-side driver implementation.
//!
//! It is recommanded to avoid the following coding style of using a open-loop
//! client: issuing a large batch of requests, waiting for all of the replies,
//! and repeat. This could easily hit the TCP socket buffer size limit and lead
//! to excessive `WouldBlock` failures.

use std::collections::HashSet;

use tokio::time::Duration;

use summerset::{
    GenericEndpoint, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, Timer, SummersetError, pf_debug, pf_info, pf_error, logged_err,
};

/// Open-loop driver struct.
pub struct DriverOpenLoop {
    /// Client ID.
    id: ClientId,

    /// Protocol-specific client stub.
    stub: Box<dyn GenericEndpoint>,

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
        stub: Box<dyn GenericEndpoint>,
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

    /// Waits on a reply from the service with timeout. Returns `Ok(None)` if
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

    /// Makes a Get request. Returns request ID for later reference if send
    /// successful, or `Ok(None)` if got a `WouldBlock` failure. In the latter
    /// case, caller must do `retry()`s before issuing any new requests,
    /// typically after doing a few `wait_reply()`s to free up some TCP socket
    /// buffer space.
    pub async fn issue_get(
        &mut self,
        key: &str,
    ) -> Result<Option<RequestId>, SummersetError> {
        let req_id = self.next_req;
        let req = ApiRequest::Req {
            id: req_id,
            cmd: Command::Get { key: key.into() },
        };

        if self.stub.send_req(Some(&req))? {
            // successful
            self.pending_reqs.insert(req_id);
            self.next_req += 1;
            Ok(Some(req_id))
        } else {
            // got `WouldBlock` failure
            Ok(None)
        }
    }

    /// Makes a Put request. Returns request ID for later reference if send
    /// successful, or `Ok(None)` if got a `WouldBlock` failure. In the latter
    /// case, caller must do `retry()`s before issuing any new requests,
    /// typically after doing a few `wait_reply()`s to free up some TCP socket
    /// buffer space.
    pub async fn issue_put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<Option<RequestId>, SummersetError> {
        let req_id = self.next_req;
        let req = ApiRequest::Req {
            id: req_id,
            cmd: Command::Put {
                key: key.into(),
                value: value.into(),
            },
        };

        if self.stub.send_req(Some(&req))? {
            // successful
            self.pending_reqs.insert(req_id);
            self.next_req += 1;
            Ok(Some(req_id))
        } else {
            // got `WouldBlock` failure
            Ok(None)
        }
    }

    /// Retries the last request that got a `WouldBlock` failure. Returns
    /// request ID if this retry is successful.
    pub async fn issue_retry(
        &mut self,
    ) -> Result<Option<RequestId>, SummersetError> {
        let req_id = self.next_req;

        if self.stub.send_req(None)? {
            // successful
            self.pending_reqs.insert(req_id);
            self.next_req += 1;
            Ok(Some(req_id))
        } else {
            // got `WouldBlock` failure
            Ok(None)
        }
    }

    /// Waits for the next reply. Returns the request ID and:
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

    /// Sends leave notification. The leave action is left synchronous.
    pub async fn leave(&mut self) -> Result<(), SummersetError> {
        let mut sent = self.stub.send_req(Some(&ApiRequest::Leave))?;
        while !sent {
            sent = self.stub.send_req(None)?;
        }

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
