//! Common helpers used by all targets.

use std::fmt;

use crate::core::replica::ReplicaId;

use bitvec::prelude as bitv;

use serde::{Serialize, Deserialize};

/// Customized error type for Summerset.
#[derive(Debug, PartialEq, Eq)]
pub struct SummersetError(pub String);

impl fmt::Display for SummersetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0) // do not display literal quotes
    }
}

/// Log trace message with parenthesized prefix.
#[macro_export]
macro_rules! pf_trace {
    ($prefix:expr, $fmt_str:literal) => {
        trace!(concat!("({}) ", $fmt_str), $prefix)
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        trace!(concat!("({}) ", $fmt_str), $prefix, $($fmt_arg)*)
    };
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
            Err(SummersetError($fmt_str.into()))
        }
    };

    ($prefix:expr, $fmt_str:literal, $($fmt_arg:tt)*) => {
        {
            pf_error!($prefix, $fmt_str, $($fmt_arg)*);
            Err(SummersetError(format!($fmt_str, $($fmt_arg)*)))
        }
    };
}

/// Compact bitmap for replica ID -> bool mapping, suited for transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaMap(pub bitv::BitVec<ReplicaId>);

impl ReplicaMap {
    /// Creates a new bitmap of given size. If `ones` is true, all slots are
    /// marked true initially; otherwise, all slots are initially false.
    pub fn new(size: u8, ones: bool) -> Result<Self, SummersetError> {
        if size == 0 {
            return Err(SummersetError(format!(
                "invalid bitmap size {}",
                size
            )));
        }
        let flag = if ones { 1 } else { 0 };
        let bv = bitv::bitvec![flag; size];
        Ok(ReplicaMap(bv))
    }

    /// Sets bit at index to given flag.
    pub fn set(
        &mut self,
        idx: ReplicaId,
        flag: bool,
    ) -> Result<(), SummersetError> {
        if idx >= self.0.len() {
            return Err(SummersetError(format!("index {} out of bound", idx)));
        }
        self.0[idx] = flag;
        Ok(())
    }

    /// Gets the bit flag at index.
    pub fn get(&self, idx: ReplicaId) -> Result<bool, SummersetError> {
        if idx >= self.0.len() {
            return Err(SummersetError(format!("index {} out of bound", idx)));
        }
        Ok(self.0[idx])
    }

    /// Allows `for _ in map.iter()`.
    pub fn iter(&self) -> impl Iterator<Item = bool> {
        self.0.iter().by_vals()
    }
}

#[cfg(test)]
mod utils_tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = SummersetError("what the heck?".into());
        assert_eq!(format!("{}", e), String::from("what the heck?"));
    }

    #[test]
    fn bitmap_new() {
        assert!(ReplicaMap::new(0, true).is_err());
        assert!(ReplicaMap::new(3, true).is_ok());
        assert!(ReplicaMap::new(5, false).is_ok());
    }

    #[test]
    fn bitmap_set_get() {
        let mut map = ReplicaMap::new(7, false).unwrap();
        assert!(map.set(0, true).is_ok());
        assert!(map.set(1, false).is_ok());
        assert!(map.set(2, true).is_ok());
        assert!(map.set(7, true).is_err());
        assert_eq!(map.get(0), Ok(true));
        assert_eq!(map.get(1), Ok(false));
        assert_eq!(map.get(2), Ok(true));
        assert_eq!(map.get(3), Ok(false));
        assert!(map.get(7).is_err());
    }

    #[test]
    fn bitmap_iter() {
        let ref_map = vec![true, true, false, true, true];
        let mut map = ReplicaMap::new(5, true).unwrap();
        assert!(map.set(2, false).is_ok());
        for (id, flag) in map.iter().enumerate() {
            assert_eq!(ref_map[id], flag);
        }
    }
}
