//! Client-side utilities for benchmarking, testing, etc.

mod repl;
pub use repl::ClientRepl;

// mod tester;

// mod bench;

/// Enum of supported client utility modes.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ClientMode {
    Repl,
}

impl ClientMode {
    /// Parse command line string into ClientMode enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match &name.to_lowercase()[..] {
            "repl" => Some(Self::Repl),
            _ => None,
        }
    }
}

#[cfg(test)]
mod modes_name_tests {
    use super::*;

    macro_rules! valid_name_test {
        ($mode:ident) => {
            assert_eq!(
                ClientMode::parse_name(stringify!($mode)),
                Some(ClientMode::$mode)
            );
        };
    }

    #[test]
    fn parse_valid_names() {
        valid_name_test!(Repl);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(ClientMode::parse_name("InvalidMode"), None);
    }
}
