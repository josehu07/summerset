//! Closed-loop client-side driver implementation.

use summerset::{
    ApiReply, ApiRequest, ClientCtrlStub, ClientId, Command, CommandResult, ConfChange,
    GenericEndpoint, RequestId, SummersetError, Timer, logged_err, pf_debug, pf_error,
};
use tokio::time::{self, Duration, Instant};

use crate::drivers::DriverReply;

/// Closed-loop driver struct.
pub(crate) struct DriverClosedLoop {
    /// Protocol-specific client endpoint.
    endpoint: Box<dyn GenericEndpoint>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,

    /// Last request sent without a definitive reply (e.g., timed out or
    /// redirected), whose ID a retry of the same command keeps.
    unanswered: Option<ApiRequest>,

    /// Reply timeout timer.
    timer: Timer,

    /// Reply timeout duration.
    timeout: Duration,
}

impl DriverClosedLoop {
    /// Creates a new closed-loop client.
    pub(crate) fn new(endpoint: Box<dyn GenericEndpoint>, timeout: Duration) -> Self {
        DriverClosedLoop {
            endpoint,
            next_req: 0,
            unanswered: None,
            timer: Timer::default(),
            timeout,
        }
    }

    /// Establishes connection with the service.
    pub(crate) async fn connect(&mut self) -> Result<(), SummersetError> {
        self.endpoint.connect().await
    }

    /// Sends leave notification and forgets about the current TCP connections.
    pub(crate) async fn leave(&mut self, permanent: bool) -> Result<(), SummersetError> {
        self.endpoint.leave(permanent).await
    }

    /// Attempts to send a request, retrying shortly after upon `WouldBlock`
    /// failures (rarely happening with a closed-loop client).
    async fn send_req_insist(&mut self, req: &ApiRequest) -> Result<(), SummersetError> {
        let mut sent = self.endpoint.send_req(Some(req))?;
        while !sent {
            time::sleep(Duration::from_millis(1)).await;
            sent = self.endpoint.send_req(None)?;
        }
        Ok(())
    }

    /// Waits on a reply from the service with timeout. Returns `Ok(None)` if
    /// timed-out.
    async fn recv_reply_timed(&mut self) -> Result<Option<ApiReply>, SummersetError> {
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

    /// Makes the request of a command, keeping the ID of the last request
    /// left without a definitive reply if retrying it, so that the service
    /// applies it at most once.
    fn make_req(&mut self, cmd: Command) -> (RequestId, ApiRequest) {
        if let Some(ApiRequest::Req { id, cmd: last_cmd }) = &self.unanswered
            && *last_cmd == cmd
        {
            return (*id, ApiRequest::Req { id: *id, cmd });
        }
        let req_id = self.next_req;
        self.next_req += 1;
        let req = ApiRequest::Req { id: req_id, cmd };
        self.unanswered = Some(req.clone());
        (req_id, req)
    }

    /// Sends a Get request and waits for its reply.
    pub(crate) async fn get(&mut self, key: &str) -> Result<DriverReply, SummersetError> {
        let (req_id, req) = self.make_req(Command::Get { key: key.into() });
        self.send_req_insist(&req).await?;
        let issue_ts = Instant::now();

        loop {
            let reply = self.recv_reply_timed().await?;
            match reply {
                Some(ApiReply::Reply {
                    id: reply_id,
                    result: cmd_result,
                    redirect,
                    ..
                }) => {
                    if reply_id != req_id {
                        // logged_err!(self.id;
                        //             "request ID mismatch: expected {}, replied {}",
                        //             req_id, reply_id)
                        continue;
                    }

                    match cmd_result {
                        None => {
                            if let Some(server) = redirect {
                                return Ok(DriverReply::Redirect { server });
                            }
                            self.unanswered = None;
                            return Ok(DriverReply::Failure);
                        }

                        Some(CommandResult::Get { value }) => {
                            self.unanswered = None;
                            let latency = Instant::now().duration_since(issue_ts);
                            return Ok(DriverReply::Success {
                                req_id,
                                cmd_result: CommandResult::Get { value },
                                latency,
                            });
                        }

                        _ => {
                            return logged_err!("command type mismatch: expected Get");
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

    /// Sends a Put request and waits for its reply.
    pub(crate) async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<DriverReply, SummersetError> {
        let (req_id, req) = self.make_req(Command::Put {
            key: key.into(),
            value: value.into(),
        });
        self.send_req_insist(&req).await?;
        let issue_ts = Instant::now();

        loop {
            let reply = self.recv_reply_timed().await?;
            match reply {
                Some(ApiReply::Reply {
                    id: reply_id,
                    result: cmd_result,
                    redirect,
                    ..
                }) => {
                    if reply_id != req_id {
                        // logged_err!(self.id;
                        //             "request ID mismatch: expected {}, replied {}",
                        //             req_id, reply_id)
                        continue;
                    }

                    match cmd_result {
                        None => {
                            if let Some(server) = redirect {
                                return Ok(DriverReply::Redirect { server });
                            }
                            self.unanswered = None;
                            return Ok(DriverReply::Failure);
                        }

                        Some(CommandResult::Put { old_value }) => {
                            self.unanswered = None;
                            let latency = Instant::now().duration_since(issue_ts);
                            return Ok(DriverReply::Success {
                                req_id,
                                cmd_result: CommandResult::Put { old_value },
                                latency,
                            });
                        }

                        _ => {
                            return logged_err!("command type mismatch: expected Put");
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

    /// Sends a responders config change request and waits for its reply.
    pub(crate) async fn conf(&mut self, delta: ConfChange) -> Result<DriverReply, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;
        self.unanswered = None;

        self.send_req_insist(&ApiRequest::Conf { id: req_id, delta })
            .await?;

        loop {
            let reply = self.recv_reply_timed().await?;
            match reply {
                Some(ApiReply::Reply {
                    id: reply_id,
                    redirect,
                    ..
                }) => {
                    if reply_id != req_id {
                        // logged_err!(self.id;
                        //             "request ID mismatch: expected {}, replied {}",
                        //             req_id, reply_id)
                        continue;
                    }

                    if let Some(redirect) = redirect
                        && reply_id == req_id
                    {
                        return Ok(DriverReply::Redirect { server: redirect });
                    }
                    return Ok(DriverReply::Failure);
                }

                Some(ApiReply::Conf {
                    id: reply_id,
                    success,
                }) => {
                    if reply_id != req_id {
                        // logged_err!(self.id;
                        //             "request ID mismatch: expected {}, replied {}",
                        //             req_id, reply_id)
                        continue;
                    }

                    return Ok(DriverReply::Conf {
                        req_id,
                        changed: success,
                    });
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

    /// Gets my Client ID.
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> ClientId {
        self.endpoint.id()
    }

    /// Gets current cluster size.
    #[allow(dead_code)]
    pub(crate) fn population(&self) -> u8 {
        self.endpoint.population()
    }

    /// Gets a mutable reference to the endpoint's control stub.
    #[allow(dead_code)]
    pub(crate) fn ctrl_stub(&mut self) -> &mut ClientCtrlStub {
        self.endpoint.ctrl_stub()
    }
}
