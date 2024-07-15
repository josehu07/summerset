//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#[macro_use]
mod utils;

mod manager;

mod client;
mod server;

mod protocols;

// Things (other than exported macros) exposed to users of this crate:

#[doc(inline)]
pub use crate::utils::{
    logger_init, Bitmap, RSCodeword, Stopwatch, SummersetError, Timer, ME,
};

#[doc(inline)]
pub use crate::manager::{ClusterManager, CtrlReply, CtrlRequest, ServerInfo};

#[doc(inline)]
pub use crate::server::{
    ApiReply, ApiRequest, Command, CommandResult, GenericReplica, ReplicaId,
    RequestId,
};

#[doc(inline)]
pub use crate::client::{ClientCtrlStub, ClientId, GenericEndpoint};

#[doc(inline)]
pub use crate::protocols::SmrProtocol;

// below are config structs exposed for users to know how to write TOML-format
// config strings
pub use crate::protocols::{ClientConfigCRaft, ReplicaConfigCRaft};
pub use crate::protocols::{ClientConfigChainRep, ReplicaConfigChainRep};
pub use crate::protocols::{ClientConfigCrossword, ReplicaConfigCrossword};
pub use crate::protocols::{ClientConfigMultiPaxos, ReplicaConfigMultiPaxos};
pub use crate::protocols::{ClientConfigRSPaxos, ReplicaConfigRSPaxos};
pub use crate::protocols::{ClientConfigRaft, ReplicaConfigRaft};
pub use crate::protocols::{ClientConfigRepNothing, ReplicaConfigRepNothing};
pub use crate::protocols::{ClientConfigSimplePush, ReplicaConfigSimplePush};
