//! Common helpers used by all targets.

use std::fmt;

/// Error type to be returned back to command line.
#[derive(PartialEq, Eq)]
pub struct CLIError(pub String);

impl fmt::Debug for CLIError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

impl fmt::Display for CLIError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}
