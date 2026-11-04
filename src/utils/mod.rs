//! Helper utilities, functions, and macros.

#[macro_use]
mod print;

#[macro_use]
mod config;

mod bitmap;
mod codec;
mod error;
pub(crate) mod faults;
mod keyrange;
mod linreg;
mod process;
mod qdisc;
mod rscoding;
mod safetcp;
mod stopwatch;
mod timer;

pub use bitmap::Bitmap;
pub use codec::{CODEC_CONFIG, Codec, CodecConfig};
pub use error::SummersetError;
pub use faults::Faults;
pub use keyrange::{ConfNum, RespondersConf};
pub(crate) use linreg::{LinearRegressor, PerfModel};
pub use print::{ME, logger_init};
pub use process::exit_with_parent;
pub(crate) use process::raise_fd_limit;
pub(crate) use qdisc::QdiscInfo;
pub use rscoding::RSCodeword;
#[cfg(test)]
pub(crate) use safetcp::test_addr_at_offset;
pub(crate) use safetcp::{
    safe_tcp_read, safe_tcp_write, tcp_bind_with_retry, tcp_connect_with_retry,
};
pub use stopwatch::Stopwatch;
pub use timer::Timer;
