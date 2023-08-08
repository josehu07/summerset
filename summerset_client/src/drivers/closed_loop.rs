//! Closed-loop client-side driver implementation.

use tokio::time::Duration;

use summerset::{
    GenericClient, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, Timer, SummersetError, pf_debug, pf_info, pf_error, logged_err,
};

/// Closed-loop driver struct.
pub struct DriverClosedLoop {
    /// Client ID.
    id: ClientId,

    /// Protocol-specific client stub.
    stub: Box<dyn GenericClient>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,

    /// Reply timeout timer.
    timer: Timer,

    /// Reply timeout duration.
    timeout: Duration,
}

impl DriverClosedLoop {
    /// Creates a new closed-loop client.
    pub fn new(
        id: ClientId,
        stub: Box<dyn GenericClient>,
        timeout: Duration,
    ) -> Self {
        DriverClosedLoop {
            id,
            stub,
            next_req: 0,
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

    /// Send a Get request and wait for its reply. Returns:
    ///   - `Ok(Some(Some(value)))` if successful and key exists
    ///   - `Ok(Some(None))` if successful and key does not exist
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader or timeout
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn get(
        &mut self,
        key: &str,
    ) -> Result<Option<(RequestId, Option<String>)>, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.stub
            .send_req(ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { key: key.into() },
            })
            .await?;

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
                            Ok(Some((req_id, value)))
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
    ///   - `Ok(Some(Some(old_value)))` if successful and key exists
    ///   - `Ok(Some(None))` if successful and key did not exist
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader or timeout
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<Option<(RequestId, Option<String>)>, SummersetError> {
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
                            Ok(Some((req_id, old_value)))
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

    /// Send leave notification.
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
