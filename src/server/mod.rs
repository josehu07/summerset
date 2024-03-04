//! Summerset's server functionality modules and trait.

mod replica;
mod statemach;
mod transport;
mod storage;
mod external;
mod control;

pub use replica::{GenericReplica, ReplicaId};
pub use external::{ExternalApi, RequestId, ApiRequest, ApiReply};
pub use statemach::{StateMachine, CommandId, Command, CommandResult};
pub use storage::{StorageHub, LogActionId, LogAction, LogResult};
pub use transport::TransportHub;
pub use control::ControlHub;
