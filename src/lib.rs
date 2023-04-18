//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#[macro_use]
mod utils;

mod statemach;
mod replica;
mod transport;
mod storage;
mod protocols;

pub use utils::SummersetError;
