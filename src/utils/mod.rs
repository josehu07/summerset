//! Helper utilities, functions, and macros.

#[macro_use]
mod print;

#[macro_use]
mod config;

mod bitmap;
mod error;
mod rscoding;
mod safetcp;
mod stopwatch;
mod timer;

pub use bitmap::Bitmap;
pub use error::SummersetError;
pub use print::{logger_init, ME};
pub use rscoding::RSCodeword;
pub use stopwatch::Stopwatch;
pub use timer::Timer;

pub(crate) use safetcp::{
    safe_tcp_read, safe_tcp_write, tcp_bind_with_retry, tcp_connect_with_retry,
};
