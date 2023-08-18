//! Customized unified error type.

use std::fmt;
use std::io;
use std::net;

use crate::server::ReplicaId;

/// Customized error type for Summerset.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SummersetError(pub String);

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

// Helper macro for saving boiler-plate `impl From<T>`s for transparent
// conversion from various common error types to `SummersetError`.
macro_rules! impl_from_error {
    ($error:ty) => {
        impl From<$error> for SummersetError {
            fn from(e: $error) -> Self {
                // just store the source error's string representation
                SummersetError(e.to_string())
            }
        }
    };
}

impl_from_error!(io::Error);
impl_from_error!(net::AddrParseError);
impl_from_error!(rmp_serde::encode::Error);
impl_from_error!(rmp_serde::decode::Error);
impl_from_error!(toml::ser::Error);
impl_from_error!(toml::de::Error);
impl_from_error!(tokio::sync::SetError<tokio::net::TcpListener>);
impl_from_error!(tokio::sync::SetError<tokio::fs::File>);
impl_from_error!(tokio::sync::mpsc::error::TryRecvError);
impl_from_error!(
    tokio::sync::watch::error::SendError<Option<tokio::time::Instant>>
);
impl_from_error!(
    tokio::sync::mpsc::error::SendError<(ReplicaId, net::SocketAddr)>
);

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = SummersetError("what the heck?".into());
        assert_eq!(format!("{}", e), String::from("what the heck?"));
    }

    #[test]
    fn from_io_error() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "oh no!");
        let e = SummersetError::from(io_error);
        assert!(e.0.contains("oh no!"));
    }
}
