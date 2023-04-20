//! Common helpers used by all targets.

use std::fmt;

/// Customized error type for Summerset.
#[derive(PartialEq, Eq)]
pub struct SummersetError(pub String);

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

/// Log debug message with parenthesized prefix.
#[macro_export]
macro_rules! pf_debug {
    ($prefix:expr, $fmt_str:literal) => {
        debug!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        debug!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log info message with parenthesized prefix.
#[macro_export]
macro_rules! pf_info {
    ($prefix:expr, $fmt_str:literal) => {
        info!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        info!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log warn message with parenthesized prefix.
#[macro_export]
macro_rules! pf_warn {
    ($prefix:expr, $fmt_str:literal) => {
        warn!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        warn!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log error message with parenthesized prefix.
#[macro_export]
macro_rules! pf_error {
    ($prefix:expr, $fmt_str:literal) => {
        error!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        error!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log an error string to logger and then return a `SummersetError`
/// containing the string.
#[macro_export]
macro_rules! logged_err {
    ($prefix:expr, $fmt_str:literal) => {
        {
            pf_error!($prefix, $fmt_str);
            SummersetError($fmt_str.into())
        }
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        {
            pf_error!($prefix, $fmt_str, $($fmt_arg)*);
            SummersetError(format!($fmt_str, $($fmt_arg)*))
        }
    };
}
