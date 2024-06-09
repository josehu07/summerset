//! Helper macros for logging (console printing).

/// Log TRACE message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_trace!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_trace {
    ($prefix:expr; $($fmt_args:tt)*) => {
        log::trace!("({}) {}", $prefix, format!($($fmt_args)*))
    };
}

/// Log DEBUG message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_debug!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_debug {
    ($prefix:expr; $($fmt_args:tt)*) => {
        log::debug!("({}) {}", $prefix, format!($($fmt_args)*))
    };
}

/// Log INFO message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_info!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_info {
    ($prefix:expr; $($fmt_args:tt)*) => {
        log::info!("({}) {}", $prefix, format!($($fmt_args)*))
    };
}

/// Log WARN message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_warn!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_warn {
    ($prefix:expr; $($fmt_args:tt)*) => {
        log::warn!("({}) {}", $prefix, format!($($fmt_args)*))
    };
}

/// Log ERROR message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_error!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_error {
    ($prefix:expr; $($fmt_args:tt)*) => {
        log::error!("({}) {}", $prefix, format!($($fmt_args)*))
    };
}

/// Log an error string to logger and then return a `SummersetError`
/// containing the string.
///
/// Example:
/// ```no_compile
/// let e = logged_err!(id; "got {} to print", msg);
/// ```
#[macro_export]
macro_rules! logged_err {
    ($prefix:expr; $($fmt_args:tt)*) => {
        {
            pf_error!($prefix; $($fmt_args)*);
            Err(SummersetError::msg(format!($($fmt_args)*)))
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
            Err::<(), SummersetError>(SummersetError::msg(
                "interesting message"
            ))
        );
        assert_eq!(
            logged_err!("jose"; "interesting message"),
            Err::<(), SummersetError>(SummersetError::msg(
                "interesting message"
            ))
        );
    }

    #[test]
    fn error_with_args() {
        assert_eq!(
            logged_err!(0; "got {} to print", 777),
            Err::<(), SummersetError>(SummersetError::msg("got 777 to print"))
        );
    }
}
