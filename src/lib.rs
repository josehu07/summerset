//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#![allow(clippy::uninlined_format_args)]

mod smr_client;
mod smr_server;
mod statemach;
mod replicator;
mod transport;
mod protocols;
mod utils;

pub use smr_client::SummersetClientStub;
pub use smr_server::{
    SummersetServerNode, SummersetApiService, InternalCommService,
};
pub use statemach::{Command, CommandResult};
pub use protocols::SMRProtocol;
pub use utils::{SummersetError, InitError};

pub mod external_api_proto {
    tonic::include_proto!("external_api");
}
