//! Helper macros for logging (console printing).

use std::sync::OnceLock;

use env_logger::Env;

/// Global variable holding the node identity string to used as logging prefix.
pub static ME: OnceLock<String> = OnceLock::new();

/// Log TRACE message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_trace!("got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_trace {
    ($($fmt_args:tt)*) => {
        log::trace!(
            "({}) {}",
            $crate::ME.get().map_or("-", |me| me.as_str()),
            format!($($fmt_args)*)
        )
    };
}

/// Log DEBUG message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_debug!("got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_debug {
    ($($fmt_args:tt)*) => {
        log::debug!(
            "({}) {}",
            $crate::ME.get().map_or("-", |me| me.as_str()),
            format!($($fmt_args)*)
        )
    };
}

/// Log INFO message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_info!("got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_info {
    ($($fmt_args:tt)*) => {
        log::info!(
            "({}) {}",
            $crate::ME.get().map_or("-", |me| me.as_str()),
            format!($($fmt_args)*)
        )
    };
}

/// Log WARN message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_warn!("got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_warn {
    ($($fmt_args:tt)*) => {
        log::warn!(
            "({}) {}",
            $crate::ME.get().map_or("-", |me| me.as_str()),
            format!($($fmt_args)*)
        )
    };
}

/// Log ERROR message with parenthesized prefix.
///
/// Example:
/// ```no_compile
/// pf_error!("got {} to print", msg);
/// ```
#[macro_export]
macro_rules! pf_error {
    ($($fmt_args:tt)*) => {
        log::error!(
            "({}) {}",
            $crate::ME.get().map_or("-", |me| me.as_str()),
            format!($($fmt_args)*)
        )
    };
}

/// Initialize `env_logger` to desired configuration if haven't.
pub fn logger_init() {
    let _ =
        env_logger::Builder::from_env(Env::default().default_filter_or("info"))
            .format_timestamp(None)
            .format_module_path(false)
            .format_target(false)
            .try_init();
}

/// Log an error string to logger and then return a `SummersetError`
/// containing the string.
///
/// Example:
/// ```no_compile
/// let e = logged_err!("got {} to print", msg);
/// ```
#[macro_export]
macro_rules! logged_err {
    ($($fmt_args:tt)*) => {
        {
            pf_error!($($fmt_args)*);
            Err($crate::SummersetError::msg(format!($($fmt_args)*)))
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::utils::SummersetError;

    #[test]
    fn error_no_args() {
        assert_eq!(
            logged_err!("interesting message"),
            Err::<(), SummersetError>(SummersetError::msg(
                "interesting message"
            ))
        );
    }

    #[test]
    fn error_with_args() {
        assert_eq!(
            logged_err!("got {} to print", 777),
            Err::<(), SummersetError>(SummersetError::msg("got 777 to print"))
        );
    }
}
