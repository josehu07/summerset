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
    ApiReply, ApiRequest, Command, CommandResult, GenericReplica, LeaserRoles,
    ReplicaId, RequestId,
};

#[doc(inline)]
pub use crate::client::{ClientCtrlStub, ClientId, GenericEndpoint};

#[doc(inline)]
pub use crate::protocols::SmrProtocol;

// below are config structs exposed for users to know how to write TOML-format
// config strings
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigRepNothing, ReplicaConfigRepNothing};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigSimplePush, ReplicaConfigSimplePush};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigChainRep, ReplicaConfigChainRep};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigMultiPaxos, ReplicaConfigMultiPaxos};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigEPaxos, ReplicaConfigEPaxos};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigRSPaxos, ReplicaConfigRSPaxos};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigRaft, ReplicaConfigRaft};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigCRaft, ReplicaConfigCRaft};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigQuorumLeases, ReplicaConfigQuorumLeases};
