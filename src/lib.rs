//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

mod core;
mod protocols;

pub use crate::core::utils::SummersetError;
