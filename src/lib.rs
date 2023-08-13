//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#[macro_use]
mod utils;

mod server;
mod client;

mod protocols;

// Things (other than exported macros) exposed to users of this crate:

#[doc(inline)]
pub use crate::utils::{SummersetError, ReplicaMap, Timer};

#[doc(inline)]
pub use crate::server::{
    ReplicaId, RequestId, ApiRequest, ApiReply, Command, CommandResult,
    GenericReplica,
};

#[doc(inline)]
pub use crate::client::{ClientId, GenericEndpoint};

#[doc(inline)]
pub use crate::protocols::SMRProtocol;

// below are config structs exposed for users to know how to write TOML-format
// config strings
pub use crate::protocols::{ReplicaConfigRepNothing, ClientConfigRepNothing};
pub use crate::protocols::{ReplicaConfigSimplePush, ClientConfigSimplePush};
pub use crate::protocols::{ReplicaConfigMultiPaxos, ClientConfigMultiPaxos};
