//! Common helpers used by all targets.

use std::fmt;

/// Error type to be returned back to command line or initializer.
#[derive(PartialEq, Eq)]
pub struct InitError(pub String);

impl fmt::Debug for InitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

impl From<SummersetError> for InitError {
    fn from(e: SummersetError) -> Self {
        InitError(format!("{}", e))
    }
}

/// Error type for various run-time internal errors.
#[derive(Debug)]
pub enum SummersetError {
    CommandEmptyKey,
    WrongCommandType,
    ClientConnError(String),
    ClientSerdeError(String),
    ServerConnError(String),
    ProtocolError(String),
}

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO: better display of error message
        write!(f, "{:?}", self)
    }
}
