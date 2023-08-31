//! Open-loop client-side driver implementation.
//!
//! It is recommanded to avoid the following coding style of using a open-loop
//! client: issuing a large batch of requests, waiting for all of the replies,
//! and repeat. This could easily hit the TCP socket buffer size limit and lead
//! to excessive `WouldBlock` failures.

use std::collections::HashMap;

use tokio::time::{Duration, Instant};

use summerset::{
    GenericEndpoint, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, ClientCtrlStub, Timer, SummersetError, pf_debug, pf_error,
    logged_err,
};

/// Open-loop driver struct.
pub struct DriverOpenLoop {
    /// Client ID.
    pub id: ClientId,

    /// Protocol-specific client endpoint.
    endpoint: Box<dyn GenericEndpoint>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,

    /// Set of pending requests whose reply has not been received, along with
    /// their issuing timestamp.
    pending_reqs: HashMap<RequestId, Instant>,

    /// Last request reported `WouldBlock` failure.
    should_retry: bool,

    /// Reply timeout timer.
    timer: Timer,

    /// Reply timeout duration.
    timeout: Duration,
}

impl DriverOpenLoop {
    /// Creates a new open-loop client.
    pub fn new(endpoint: Box<dyn GenericEndpoint>, timeout: Duration) -> Self {
        DriverOpenLoop {
            id: endpoint.id(),
            endpoint,
            next_req: 0,
            pending_reqs: HashMap::new(),
            should_retry: false,
            timer: Timer::new(),
            timeout,
        }
    }

    /// Establishes connection with the service.
    pub async fn connect(&mut self) -> Result<(), SummersetError> {
        self.endpoint.connect().await
    }

    /// Waits for all pending replies to be received, then sends leave
    /// notification and forgets about the current TCP connections. The leave
    /// action is left synchronous.
    pub async fn leave(
        &mut self,
        permanent: bool,
    ) -> Result<(), SummersetError> {
        // loop until all pending replies have been received
        while self.should_retry {
            self.issue_retry()?;
        }
        while !self.pending_reqs.is_empty() {
            self.wait_reply().await?;
        }

        self.endpoint.leave(permanent).await
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

            reply = self.endpoint.recv_reply() => {
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
    pub fn issue_get(
        &mut self,
        key: &str,
    ) -> Result<Option<RequestId>, SummersetError> {
        let req_id = self.next_req;
        let req = ApiRequest::Req {
            id: req_id,
            cmd: Command::Get { key: key.into() },
        };

        if self.endpoint.send_req(Some(&req))? {
            // successful
            self.pending_reqs.insert(req_id, Instant::now());
            self.next_req += 1;
            self.should_retry = false;
            Ok(Some(req_id))
        } else {
            // got `WouldBlock` failure
            self.should_retry = true;
            Ok(None)
        }
    }

    /// Makes a Put request. Returns request ID for later reference if send
    /// successful, or `Ok(None)` if got a `WouldBlock` failure. In the latter
    /// case, caller must do `retry()`s before issuing any new requests,
    /// typically after doing a few `wait_reply()`s to free up some TCP socket
    /// buffer space.
    pub fn issue_put(
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

        if self.endpoint.send_req(Some(&req))? {
            // successful
            self.pending_reqs.insert(req_id, Instant::now());
            self.next_req += 1;
            self.should_retry = false;
            Ok(Some(req_id))
        } else {
            // got `WouldBlock` failure
            self.should_retry = true;
            Ok(None)
        }
    }

    /// Retries the last request that got a `WouldBlock` failure. Returns
    /// request ID if this retry is successful.
    pub fn issue_retry(&mut self) -> Result<Option<RequestId>, SummersetError> {
        let req_id = self.next_req;

        if self.endpoint.send_req(None)? {
            // successful
            self.pending_reqs.insert(req_id, Instant::now());
            self.next_req += 1;
            self.should_retry = false;
            Ok(Some(req_id))
        } else {
            // got `WouldBlock` failure
            self.should_retry = true;
            Ok(None)
        }
    }

    /// Waits for the next reply. Returns the request ID and:
    ///   - `Ok(Some((id, cmd_result, latency)))` if request successful
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader or timeout
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn wait_reply(
        &mut self,
    ) -> Result<Option<(RequestId, CommandResult, Duration)>, SummersetError>
    {
        let reply = self.recv_reply_with_timeout().await?;
        match reply {
            Some(ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            }) => {
                if !self.pending_reqs.contains_key(&reply_id) {
                    logged_err!(self.id; "request ID {} not in pending set",
                                         reply_id)
                } else {
                    let issue_ts = self.pending_reqs.remove(&reply_id).unwrap();
                    let lat = Instant::now().duration_since(issue_ts);

                    if let Some(res) = cmd_result {
                        Ok(Some((reply_id, res, lat)))
                    } else {
                        Ok(None)
                    }
                }
            }

            None => Ok(None), // timed-out

            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }

    /// Gets a mutable reference to the endpoint's control stub.
    pub fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        self.endpoint.ctrl_stub()
    }
}
