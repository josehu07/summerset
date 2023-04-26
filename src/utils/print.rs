//! Helper macros for logging (console printing).

/// Log TRACE message with parenthesized prefix.
///
/// Example:
/// ```no_run
/// pf_trace!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_trace {
    ($prefix:expr; $fmt_str:literal) => {
        log::trace!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr; $fmt_str:literal, $($fmt_arg:tt)*) => {
        log::trace!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log DEBUG message with parenthesized prefix.
///
/// Example:
/// ```no_run
/// pf_debug!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_debug {
    ($prefix:expr; $fmt_str:literal) => {
        log::debug!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr; $fmt_str:literal, $($fmt_arg:tt)*) => {
        log::debug!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log INFO message with parenthesized prefix.
///
/// Example:
/// ```no_run
/// pf_info!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_info {
    ($prefix:expr; $fmt_str:literal) => {
        log::info!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr; $fmt_str:literal, $($fmt_arg:tt)*) => {
        log::info!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log WARN message with parenthesized prefix.
///
/// Example:
/// ```no_run
/// pf_warn!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_warn {
    ($prefix:expr; $fmt_str:literal) => {
        log::warn!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr; $fmt_str:literal, $($fmt_arg:tt)*) => {
        log::warn!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log ERROR message with parenthesized prefix.
///
/// Example:
/// ```no_run
/// pf_error!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_error {
    ($prefix:expr; $fmt_str:literal) => {
        log::error!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr; $fmt_str:literal, $($fmt_arg:tt)*) => {
        log::error!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
}

/// Log an error string to logger and then return a `SummersetError`
/// containing the string.
///
/// Example:
/// ```no_run
/// let e = logged_err!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! logged_err {
    ($prefix:expr; $fmt_str:literal) => {
        {
            pf_error!($prefix, $fmt_str);
            Err(SummersetError($fmt_str.into()))
        }
    };

    ($prefix:expr; $fmt_str:literal, $($fmt_arg:tt)*) => {
        {
            pf_error!($prefix, $fmt_str, $($fmt_arg)*);
            Err(SummersetError(format!($fmt_str, $($fmt_arg)*)))
        }
    };
}

#[cfg(test)]
mod print_tests {
    use crate::utils::SummersetError;

    #[test]
    fn error_no_args() {
        assert_eq!(
            logged_err!(0; "interesting message"),
            Err(SummersetError("(0) interesting message".into()))
        );
        assert_eq!(
            logged_err!("jose"; "interesting message"),
            Err(SummersetError("(jose) interesting message".into()))
        );
    }

    #[test]
    fn error_with_args() {
        assert_eq!(
            logged_err!(0; "got {} to print", 777),
            Err(SummersetError("(0) got 777 to print".into()))
        );
    }
}
