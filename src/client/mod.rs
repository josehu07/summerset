//! Summerset's client functionality modules and trait.

mod handle;
mod apistub;

pub use handle::{GenericClient, ClientId};
pub use apistub::{ClientApiStub, ClientSendStub, ClientRecvStub};
