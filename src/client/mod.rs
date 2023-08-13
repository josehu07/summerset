//! Summerset's client functionality modules and trait.

mod endpoint;
mod apistub;

pub use endpoint::{GenericEndpoint, ClientId};
pub use apistub::{ClientApiStub, ClientSendStub, ClientRecvStub};
