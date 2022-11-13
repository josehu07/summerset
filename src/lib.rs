//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

mod smr_client;
mod smr_server;
mod statemach;
mod replicator;
mod protocols;
mod utils;

pub use smr_client::SummersetClientStub;
pub use smr_server::SummersetServerNode;
pub use statemach::{Command, CommandResult};
pub use protocols::SMRProtocol;
pub use utils::{SummersetError, InitError};

// Below are tonic RPC protobufs.
pub mod external_api_proto {
    tonic::include_proto!("external_api");
}
