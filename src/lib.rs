//! Public interface to the Summerset core library, linked by both server
//! executable and client library.

#[macro_use]
mod core;

mod protocols;

// Things exposed to users of this crate:
#[doc(inline)]
pub use crate::core::utils::SummersetError;
#[doc(inline)]
pub use crate::protocols::SMRProtocol;
