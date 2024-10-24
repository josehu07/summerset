//! Summerset's server functionality modules and trait.

mod replica;

mod control;
mod external;
mod heartbeat;
mod leaseman;
mod statemach;
mod storage;
mod transport;

pub use external::{ApiReply, ApiRequest, RequestId};
pub use replica::{GenericReplica, ReplicaId};
pub use statemach::{Command, CommandId, CommandResult};

pub(crate) use control::ControlHub;
pub(crate) use external::ExternalApi;
pub(crate) use heartbeat::{HeartbeatEvent, Heartbeater};
pub(crate) use leaseman::{
    LeaseAction, LeaseManager, LeaseMsg, LeaseNotice, LeaseNum,
};
pub(crate) use statemach::StateMachine;
pub(crate) use storage::{LogAction, LogActionId, LogResult, StorageHub};
pub(crate) use transport::TransportHub;

// TODO: turns Heartbeater into a more organized, channel-oriented module
//       make Snapshotter a separate module and add full-fledged features
