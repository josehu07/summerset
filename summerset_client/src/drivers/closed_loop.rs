//! Closed-loop client-side driver implementation.

use crate::drivers::DriverReply;

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

    /// Attempt to send a request, retrying immediately if receiving
    /// `WouldBlock` failure. This shortcut is used here because TCP write
    /// blocking is not expected with a closed-loop client.
    fn send_req_insist(
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
    async fn recv_reply_timed(
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

    /// Send a Get request and wait for its reply.
    pub async fn get(
        &mut self,
        key: &str,
    ) -> Result<DriverReply, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.send_req_insist(&ApiRequest::Req {
            id: req_id,
            cmd: Command::Get { key: key.into() },
        })?;
        let issue_ts = Instant::now();

        loop {
            let reply = self.recv_reply_timed().await?;
            match reply {
                Some(ApiReply::Reply {
                    id: reply_id,
                    result: cmd_result,
                    redirect,
                }) => {
                    if reply_id != req_id {
                        // logged_err!(self.id;
                        //             "request ID mismatch: expected {}, replied {}",
                        //             req_id, reply_id)
                        continue;
                    } else {
                        match cmd_result {
                            None => {
                                if let Some(server) = redirect {
                                    return Ok(DriverReply::Redirect {
                                        server,
                                    });
                                } else {
                                    return Ok(DriverReply::Failure);
                                }
                            }

                            Some(CommandResult::Get { value }) => {
                                let latency =
                                    Instant::now().duration_since(issue_ts);
                                return Ok(DriverReply::Success {
                                    req_id,
                                    cmd_result: CommandResult::Get { value },
                                    latency,
                                });
                            }

                            _ => {
                                return logged_err!(
                                    self.id;
                                    "command type mismatch: expected Get"
                                );
                            }
                        }
                    }
                }

                None => {
                    return Ok(DriverReply::Timeout);
                }

                _ => {
                    return logged_err!(self.id; "unexpected reply type received");
                }
            }
        }
    }

    /// Send a Put request and wait for its reply.
    pub async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<DriverReply, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.send_req_insist(&ApiRequest::Req {
            id: req_id,
            cmd: Command::Put {
                key: key.into(),
                value: value.into(),
            },
        })?;
        let issue_ts = Instant::now();

        loop {
            let reply = self.recv_reply_timed().await?;
            match reply {
                Some(ApiReply::Reply {
                    id: reply_id,
                    result: cmd_result,
                    redirect,
                }) => {
                    if reply_id != req_id {
                        // logged_err!(self.id;
                        //             "request ID mismatch: expected {}, replied {}",
                        //             req_id, reply_id)
                        continue;
                    } else {
                        match cmd_result {
                            None => {
                                if let Some(server) = redirect {
                                    return Ok(DriverReply::Redirect {
                                        server,
                                    });
                                } else {
                                    return Ok(DriverReply::Failure);
                                }
                            }

                            Some(CommandResult::Put { old_value }) => {
                                let latency =
                                    Instant::now().duration_since(issue_ts);
                                return Ok(DriverReply::Success {
                                    req_id,
                                    cmd_result: CommandResult::Put {
                                        old_value,
                                    },
                                    latency,
                                });
                            }

                            _ => {
                                return logged_err!(
                                    self.id;
                                    "command type mismatch: expected Put"
                                );
                            }
                        }
                    }
                }

                None => {
                    return Ok(DriverReply::Timeout);
                }

                _ => {
                    return logged_err!(self.id; "unexpected reply type received");
                }
            }
        }
    }

    /// Gets a mutable reference to the endpoint's control stub.
    #[allow(dead_code)]
    pub fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        self.endpoint.ctrl_stub()
    }
}
