//! Closed-loop client implementation.

use summerset::{
    GenericClient, ClientId, Command, CommandResult, ApiRequest, ApiReply,
    RequestId, SummersetError, pf_info, pf_error, logged_err,
};

/// Closed-loop client struct.
pub struct ClientClosedLoop {
    /// Client ID.
    id: ClientId,

    /// Protocol-specific client stub.
    stub: Box<dyn GenericClient>,

    /// Next request ID, monotonically increasing.
    next_req: RequestId,
}

impl ClientClosedLoop {
    /// Creates a new closed-loop client.
    pub fn new(id: ClientId, stub: Box<dyn GenericClient>) -> Self {
        ClientClosedLoop {
            id,
            stub,
            next_req: 0,
        }
    }

    /// Send a Get request and wait for its reply. Returns:
    ///   - `Ok(Some(Some(value)))` if successful and key exists
    ///   - `Ok(Some(None))` if successful and key does not exist
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn get(
        &mut self,
        key: &str,
    ) -> Result<Option<Option<String>>, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.stub
            .send_req(ApiRequest::Req {
                id: req_id,
                cmd: Command::Get { key: key.into() },
            })
            .await?;

        let reply = self.stub.recv_reply().await?;
        match reply {
            ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            } => {
                if reply_id != req_id {
                    logged_err!(self.id; "request ID mismatch: expected {}, replied {}", req_id, reply_id)
                } else {
                    match cmd_result {
                        None => Ok(None),
                        Some(CommandResult::Get { value }) => Ok(Some(value)),
                        _ => {
                            logged_err!(self.id; "command type mismatch: expected Get")
                        }
                    }
                }
            }
            _ => logged_err!(self.id; "unexpected reply type received"),
        }
    }

    /// Send a Put request and wait for its reply. Returns:
    ///   - `Ok(Some(Some(old_value)))` if successful and key exists
    ///   - `Ok(Some(None))` if successful and key did not exist
    ///   - `Ok(None)` if request unsuccessful, e.g., wrong leader
    ///   - `Err(err)` if any unexpected error occurs
    pub async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<Option<Option<String>>, SummersetError> {
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

        let reply = self.stub.recv_reply().await?;
        match reply {
            ApiReply::Reply {
                id: reply_id,
                result: cmd_result,
                ..
            } => {
                if reply_id != req_id {
                    logged_err!(self.id; "request ID mismatch: expected {}, replied {}", req_id, reply_id)
                } else {
                    match cmd_result {
                        None => Ok(None),
                        Some(CommandResult::Put { old_value }) => {
                            Ok(Some(old_value))
                        }
                        _ => {
                            logged_err!(self.id; "command type mismatch: expected Put")
                        }
                    }
                }
            }
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
