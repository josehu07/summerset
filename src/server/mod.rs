//! Summerset's server functionality modules and trait.

mod replica;
mod statemach;
mod transport;
mod storage;
mod external;
mod control;

pub use replica::{GenericReplica, ReplicaId};
pub use external::{RequestId, ApiRequest, ApiReply};
pub use statemach::{CommandId, Command, CommandResult};

pub(crate) use external::ExternalApi;
pub(crate) use statemach::StateMachine;
pub(crate) use storage::{StorageHub, LogActionId, LogAction, LogResult};
pub(crate) use transport::TransportHub;
pub(crate) use control::ControlHub;
