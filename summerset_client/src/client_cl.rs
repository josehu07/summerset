//! Closed-loop client implementation.

use summerset::{
    ClientId, ClientSendStub, ClientRecvStub, Command, CommandResult,
    ApiRequest, SummersetError, pf_error, logged_err,
};

/// Closed-loop client struct.
pub struct ClientClosedLoop {
    /// Client ID.
    id: ClientId,

    /// ApiRequest send stub.
    send_stub: ClientSendStub,

    /// ApiReply receive stub.
    recv_stub: ClientRecvStub,

    /// Next request ID, monitonically increasing.
    next_req: u64,
}

impl ClientClosedLoop {
    /// Creates a new closed-loop client.
    pub fn new(
        id: ClientId,
        send_stub: ClientSendStub,
        recv_stub: ClientRecvStub,
    ) -> Self {
        ClientClosedLoop {
            id,
            send_stub,
            recv_stub,
            next_req: 0,
        }
    }

    /// Send a Get request and wait for its reply.
    pub async fn get(
        &mut self,
        key: &str,
    ) -> Result<Option<String>, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.send_stub
            .send_req(ApiRequest {
                id: req_id,
                cmd: Command::Get { key: key.into() },
            })
            .await?;

        let reply = self.recv_stub.recv_reply().await?;
        if reply.id != req_id {
            logged_err!(self.id; "request ID mismatch: expected {}, replied {}", req_id, reply.id)
        } else {
            match reply.result {
                CommandResult::Get { value } => Ok(value),
                _ => {
                    logged_err!(self.id; "command type mismatch: expected Get")
                }
            }
        }
    }

    /// Send a Put request and wait for its reply.
    pub async fn put(
        &mut self,
        key: &str,
        value: &str,
    ) -> Result<Option<String>, SummersetError> {
        let req_id = self.next_req;
        self.next_req += 1;

        self.send_stub
            .send_req(ApiRequest {
                id: req_id,
                cmd: Command::Put {
                    key: key.into(),
                    value: value.into(),
                },
            })
            .await?;

        let reply = self.recv_stub.recv_reply().await?;
        if reply.id != req_id {
            logged_err!(self.id; "request ID mismatch: expected {}, replied {}", req_id, reply.id)
        } else {
            match reply.result {
                CommandResult::Put { old_value } => Ok(old_value),
                _ => {
                    logged_err!(self.id; "command type mismatch: expected Put")
                }
            }
        }
    }
}
