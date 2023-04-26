//! Customized unified error type.

use std::fmt;
use std::io;

/// Customized error type for Summerset.
#[derive(Debug, PartialEq, Eq)]
pub struct SummersetError(pub String);

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

// allow transparent convertion from common error types:

impl From<io::Error> for SummersetError {
    fn from(e: io::Error) -> Self {
        SummersetError(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for SummersetError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        SummersetError(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for SummersetError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        SummersetError(e.to_string())
    }
}

impl From<toml::ser::Error> for SummersetError {
    fn from(e: toml::ser::Error) -> Self {
        SummersetError(e.to_string())
    }
}

impl From<toml::de::Error> for SummersetError {
    fn from(e: toml::de::Error) -> Self {
        SummersetError(e.to_string())
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = SummersetError("what the heck?".into());
        assert_eq!(format!("{}", e), String::from("what the heck?"));
    }

    #[test]
    fn convert_from() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "oh no!");
        let e = SummersetError::from(io_error);
        assert_eq!(e.0, String::from("oh no!"));
    }
}
