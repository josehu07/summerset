//! Helper utilities, functions, and macros.

#[macro_use]
mod print;

#[macro_use]
mod config;

mod bitmap;
mod error;
mod keyrange;
mod linreg;
mod qdisc;
mod rscoding;
mod safetcp;
mod stopwatch;
mod timer;

pub use bitmap::Bitmap;
pub use error::SummersetError;
pub use keyrange::{ConfNum, RespondersConf};
pub(crate) use linreg::{LinearRegressor, PerfModel};
pub use print::{ME, logger_init};
pub(crate) use qdisc::QdiscInfo;
pub use rscoding::RSCodeword;
pub(crate) use safetcp::{
    safe_tcp_read, safe_tcp_write, tcp_bind_with_retry, tcp_connect_with_retry,
};
pub use stopwatch::Stopwatch;
pub use timer::Timer;
