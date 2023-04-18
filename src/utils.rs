//! Common helpers used by all targets.

use std::fmt;

/// Customized error type.
#[derive(PartialEq, Eq)]
pub struct SummersetError(String);

impl fmt::Debug for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}
