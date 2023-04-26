//! Summerset's server functionality modules and trait.

pub mod replica;
pub mod statemach;
pub mod transport;
pub mod storage;
pub mod external;

pub use replica::{GenericReplica, ReplicaId};
pub use statemach::{StateMachine, CommandId, Command, CommandResult};
pub use transport::TransportHub;
pub use storage::{StorageHub, LogActionId, LogAction, LogResult};
pub use external::{ExternalApi, ApiRequest, ApiReply};
