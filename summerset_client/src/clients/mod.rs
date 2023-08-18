//! Client-side utilities for benchmarking, testing, etc.

mod repl;
pub use repl::ClientRepl;

mod bench;
pub use bench::ClientBench;

mod tester;
pub use tester::ClientTester;

/// Enum of supported client utility modes.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ClientMode {
    Repl,
    Bench,
    Tester,
}

impl ClientMode {
    /// Parse command line string into ClientMode enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match &name.to_lowercase()[..] {
            "repl" => Some(Self::Repl),
            "bench" => Some(Self::Bench),
            "tester" => Some(Self::Tester),
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
        valid_name_test!(Bench);
        valid_name_test!(Tester);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(ClientMode::parse_name("InvalidMode"), None);
    }
}
