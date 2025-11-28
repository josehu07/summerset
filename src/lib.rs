//! Public interface to the Summerset core library, linked by both server
//! executable and client library.
#![allow(
    clippy::similar_names,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::uninlined_format_args
)]

#[macro_use]
mod utils;

mod manager;

mod client;
mod server;

mod protocols;

// Things (other than exported macros) exposed to users of this crate:

#[doc(inline)]
pub use crate::client::{ClientCtrlStub, ClientId, GenericEndpoint};
#[doc(inline)]
pub use crate::manager::{ClusterManager, CtrlReply, CtrlRequest, ServerInfo};
#[doc(inline)]
pub use crate::protocols::SmrProtocol;
#[doc(inline)]
pub use crate::server::{
    ApiReply, ApiRequest, Command, CommandResult, ConfChange, GenericReplica,
    ReplicaId, RequestId,
};
#[doc(inline)]
pub use crate::utils::{
    Bitmap, ME, RSCodeword, Stopwatch, SummersetError, Timer, logger_init,
};

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
pub use crate::protocols::{ClientConfigCrossword, ReplicaConfigCrossword};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigQuorumLeases, ReplicaConfigQuorumLeases};
#[rustfmt::skip]
pub use crate::protocols::{ClientConfigBodega, ReplicaConfigBodega};
