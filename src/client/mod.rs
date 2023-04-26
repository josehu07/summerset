//! Summerset's client functionality modules and trait.

mod client;
mod apistub;

pub use client::{GenericClient, ClientId};
pub use apistub::{ClientApiStub, ClientSendStub, ClientRecvStub};
