//! Customized unified error type.

use std::error;
use std::fmt;
use std::io;
use std::net;
use std::num;
use std::string;

/// Customized error type for Summerset.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SummersetError(String);

impl SummersetError {
    pub fn msg(msg: impl ToString) -> Self {
        SummersetError(msg.to_string())
    }
}

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

impl error::Error for SummersetError {}

// Helper macro for saving boiler-plate `impl From<X>`s for transparent
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

// Helper macro for saving boiler-plate `impl From<X<T>>`s for transparent
// conversion from various common generic error types to `SummersetError`.
macro_rules! impl_from_error_generic {
    ($error:ty) => {
        impl<T> From<$error> for SummersetError {
            fn from(e: $error) -> SummersetError {
                SummersetError::msg(e.to_string())
            }
        }
    };
}

impl_from_error!(io::Error);
impl_from_error!(string::FromUtf8Error);
impl_from_error!(num::ParseIntError);
impl_from_error!(num::ParseFloatError);
impl_from_error!(net::AddrParseError);
impl_from_error!(bincode::Error);
impl_from_error!(toml::ser::Error);
impl_from_error!(toml::de::Error);
impl_from_error!(reed_solomon_erasure::Error);
impl_from_error!(ctrlc::Error);
impl_from_error!(linreg::Error);
impl_from_error!(tokio::sync::mpsc::error::TryRecvError);
impl_from_error!(zookeeper_client::Error);

impl_from_error_generic!(tokio::sync::SetError<T>);
impl_from_error_generic!(tokio::sync::watch::error::SendError<T>);
impl_from_error_generic!(tokio::sync::mpsc::error::SendError<T>);

#[cfg(test)]
mod tests {
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
