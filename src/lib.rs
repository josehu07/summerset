//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#[macro_use]
mod utils;

mod server;
mod client;

mod protocols;

// Things (other than exported macros) exposed to users of this crate:

#[doc(inline)]
pub use crate::utils::{SummersetError, ReplicaMap};

#[doc(inline)]
pub use crate::server::ReplicaId;

#[doc(inline)]
pub use crate::client::ClientId;

#[doc(inline)]
pub use crate::protocols::SMRProtocol;
