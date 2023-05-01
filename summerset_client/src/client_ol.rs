//! Open-loop client implementation.

// TODO: finish me!

use summerset::{ClientId, ClientSendStub, ClientRecvStub};

/// Open-loop client struct.
pub struct ClientOpenLoop {
    /// Client ID.
    id: ClientId,

    /// ApiRequest send stub.
    send_stub: ClientSendStub,

    /// ApiReply receive stub.
    recv_stub: ClientRecvStub,
}
