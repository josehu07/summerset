//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#[macro_use]
mod core;

mod protocols;

// Things (other than exported macros) exposed to users of this crate:

#[doc(inline)]
pub use crate::core::utils::{SummersetError, ReplicaMap};

#[doc(inline)]
pub use crate::core::replica::{GenericReplica, ReplicaId};

#[doc(inline)]
pub use crate::core::client::{GenericClient, ClientId};

#[doc(inline)]
pub use crate::protocols::SMRProtocol;
