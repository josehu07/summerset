//! Closed-loop client-side driver implementation.

use tokio::time::{Duration, Instant};

use summerset::{
    GenericEndpoint, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, ClientCtrlStub, Timer, SummersetError, pf_debug, pf_error,
    logged_err,
};

/// Closed-loop driver struct.
pub struct DriverClosedLoop {
    /// Client ID.
    pub id: ClientId,

    /// Protocol-specific client endpoint.
    endpoint: Box<dyn GenericEndpoint>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,

    /// Reply timeout timer.
    timer: Timer,

    /// Reply timeout duration.
    timeout: Duration,
}

impl DriverClosedLoop {
    /// Creates a new closed-loop client.
    pub fn new(endpoint: Box<dyn GenericEndpoint>, timeout: Duration) -> Self {
        DriverClosedLoop {
            id: endpoint.id(),
            endpoint,
            next_req: 0,
            timer: Timer::new(),
            timeout,
        }
    }

    /// Establishes connection with the service.
    pub async fn connect(&mut self) -> Result<(), SummersetError> {
        self.endpoint.connect().await
    }

    /// Sends leave notification and forgets about the current TCP connections.
    pub async fn leave(
        &mut self,
        permanent: bool,
    ) -> Result<(), SummersetError> {
        self.endpoint.leave(permanent).await
    }

    /// Attempt to send a request, retrying if received `WouldBlock` failure.
    fn send_req_retry_on_block(
        &mut self,
        req: &ApiRequest,
    ) -> Result<(), SummersetError> {
        let mut success = self.endpoint.send_req(Some(req))?;
        while !success {
            success = self.endpoint.send_req(None)?;
        }
        Ok(())
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

            reply = self.endpoint.recv_reply() => {
                self.timer.cancel()?; // cancel current deadline
                Ok(Some(reply?))
            }
        }
    }

    /// Send a Get request and wait for its reply. Returns:
    ///   - `Ok(Some((id, Some(value), latency)))` if successful and key exists
    ///   - `Ok(Some((id, None, latency)))` if successful and key does not exist
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader or timeout
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn get(
        &mut self,
        key: &str,
    ) -> Result<Option<(RequestId, Option<String>, Duration)>, SummersetError>
    {
        let req_id = self.next_req;
        self.next_req += 1;

        self.send_req_retry_on_block(&ApiRequest::Req {
            id: req_id,
            cmd: Command::Get { key: key.into() },
        })?;
        let issue_ts = Instant::now();

        let reply = self.recv_reply_with_timeout().await?;
        match reply {
            Some(ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            }) => {
                if reply_id != req_id {
                    logged_err!(self.id; "request ID mismatch: expected {}, replied {}",
                                         req_id, reply_id)
                } else {
                    match cmd_result {
                        None => Ok(None),
                        Some(CommandResult::Get { value }) => {
                            let lat = Instant::now().duration_since(issue_ts);
                            Ok(Some((req_id, value, lat)))
                        }
                        _ => {
                            logged_err!(self.id; "command type mismatch: expected Get")
                        }
                    }
                }
            }

            None => Ok(None), // timed-out

            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }

    /// Send a Put request and wait for its reply. Returns:
    ///   - `Ok(Some((id, Some(old_value), latency)))` if successful and key exists
    ///   - `Ok(Some((id, None, latency)))` if successful and key did not exist
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader or timeout
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<Option<(RequestId, Option<String>, Duration)>, SummersetError>
    {
        let req_id = self.next_req;
        self.next_req += 1;

        self.send_req_retry_on_block(&ApiRequest::Req {
            id: req_id,
            cmd: Command::Put {
                key: key.into(),
                value: value.into(),
            },
        })?;
        let issue_ts = Instant::now();

        let reply = self.recv_reply_with_timeout().await?;
        match reply {
            Some(ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            }) => {
                if reply_id != req_id {
                    logged_err!(self.id; "request ID mismatch: expected {}, replied {}",
                                         req_id, reply_id)
                } else {
                    match cmd_result {
                        None => Ok(None),
                        Some(CommandResult::Put { old_value }) => {
                            let lat = Instant::now().duration_since(issue_ts);
                            Ok(Some((req_id, old_value, lat)))
                        }
                        _ => {
                            logged_err!(self.id; "command type mismatch: expected Put")
                        }
                    }
                }
            }

            None => Ok(None), // timed-out

            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }

    /// Gets a mutable reference to the endpoint's control stub.
    #[allow(dead_code)]
    pub fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        self.endpoint.ctrl_stub()
    }
}
