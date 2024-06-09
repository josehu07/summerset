//! Client-side utilities for benchmarking, testing, etc.

mod repl;
pub(crate) use repl::ClientRepl;

mod bench;
pub(crate) use bench::ClientBench;

mod tester;
pub(crate) use tester::ClientTester;

mod mess;
pub(crate) use mess::ClientMess;

/// Enum of supported client utility modes.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum ClientMode {
    Repl,
    Bench,
    Tester,
    Mess,
}

impl ClientMode {
    /// Parse command line string into ClientMode enum.
    pub(crate) fn parse_name(name: &str) -> Option<Self> {
        match &name.to_lowercase()[..] {
            "repl" => Some(Self::Repl),
            "bench" => Some(Self::Bench),
            "tester" => Some(Self::Tester),
            "mess" => Some(Self::Mess),
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
        valid_name_test!(Mess);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(ClientMode::parse_name("InvalidMode"), None);
    }
}
