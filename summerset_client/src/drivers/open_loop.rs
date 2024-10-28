//! Open-loop client-side driver implementation.
//!
//! It is recommended to avoid the following coding style of using a open-loop
//! client: issuing a large batch of requests, waiting for all of the replies,
//! and repeat. This could easily hit the TCP socket buffer size limit and lead
//! to excessive `WouldBlock` failures.

use std::collections::HashMap;

use crate::drivers::DriverReply;

use tokio::time::{Duration, Instant};

use summerset::{
    logged_err, pf_debug, pf_error, ApiReply, ApiRequest, ClientCtrlStub,
    Command, GenericEndpoint, RequestId, SummersetError, Timer,
};

/// Open-loop driver struct.
pub(crate) struct DriverOpenLoop {
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
    pub(crate) fn new(
        endpoint: Box<dyn GenericEndpoint>,
        timeout: Duration,
    ) -> Self {
        DriverOpenLoop {
            endpoint,
            next_req: 0,
            pending_reqs: HashMap::new(),
            should_retry: false,
            timer: Timer::default(),
            timeout,
        }
    }

    /// Establishes connection with the service.
    pub(crate) async fn connect(&mut self) -> Result<(), SummersetError> {
        self.endpoint.connect().await
    }

    /// Sends leave notification and forgets about the current TCP connections.
    pub(crate) async fn leave(
        &mut self,
        permanent: bool,
    ) -> Result<(), SummersetError> {
        self.endpoint.leave(permanent).await
    }

    /// Makes a Get request. Returns request ID for later reference if send
    /// successful, or `Ok(None)` if got a `WouldBlock` failure. In the latter
    /// case, caller must do `retry()`s before issuing any new requests,
    /// typically after doing a few `wait_reply()`s to free up some TCP socket
    /// buffer space.
    pub(crate) fn issue_get(
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
    pub(crate) fn issue_put(
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
    pub(crate) fn issue_retry(
        &mut self,
    ) -> Result<Option<RequestId>, SummersetError> {
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

    /// Waits on a reply from the service with timeout. Returns `Ok(None)` if
    /// timed-out.
    async fn recv_reply_timed(
        &mut self,
    ) -> Result<Option<ApiReply>, SummersetError> {
        self.timer.kickoff(self.timeout)?;

        tokio::select! {
            () = self.timer.timeout() => {
                pf_debug!("timed-out waiting for reply");
                Ok(None)
            }

            reply = self.endpoint.recv_reply() => {
                self.timer.cancel()?; // cancel current deadline
                Ok(Some(reply?))
            }
        }
    }

    /// Waits for the next reply.
    pub(crate) async fn wait_reply(
        &mut self,
    ) -> Result<DriverReply, SummersetError> {
        loop {
            let reply = self.recv_reply_timed().await?;
            match reply {
                Some(ApiReply::Reply {
                    id: reply_id,
                    result: cmd_result,
                    redirect,
                    ..
                }) => {
                    if !self.pending_reqs.contains_key(&reply_id) {
                        // logged_err!("request ID {} not in pending set",
                        //                      reply_id)
                        continue;
                    } else {
                        let issue_ts =
                            self.pending_reqs.remove(&reply_id).unwrap();
                        let latency = Instant::now().duration_since(issue_ts);

                        if let Some(res) = cmd_result {
                            return Ok(DriverReply::Success {
                                req_id: reply_id,
                                cmd_result: res,
                                latency,
                            });
                        } else if let Some(server) = redirect {
                            return Ok(DriverReply::Redirect { server });
                        } else {
                            return Ok(DriverReply::Failure);
                        }
                    }
                }

                None => {
                    return Ok(DriverReply::Timeout);
                }

                _ => {
                    return logged_err!("unexpected reply type received");
                }
            }
        }
    }

    /// Gets a mutable reference to the endpoint's control stub.
    #[allow(dead_code)]
    pub(crate) fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        self.endpoint.ctrl_stub()
    }
}
