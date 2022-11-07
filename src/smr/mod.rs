mod do_nothing;

use crate::replicator::Replicator;

use do_nothing::DoNothing;

/// Enum of supported replication protocol types.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SMRProtocol {
    DoNothing,
}

impl SMRProtocol {
    /// Parse command line string into SMRProtocol enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match name {
            "DoNothing" => Some(Self::DoNothing),
            _ => None,
        }
    }

    /// Create a Replicator instance of this protocol type on heap.
    pub fn new_replicator(self) -> Box<dyn Replicator> {
        match self {
            Self::DoNothing => Box::new(DoNothing::new()),
        }
    }
}

#[cfg(test)]
mod smr_tests {
    use super::SMRProtocol;

    #[test]
    fn parse_valid_names() {
        assert_eq!(
            SMRProtocol::parse_name("DoNothing"),
            Some(SMRProtocol::DoNothing)
        );
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SMRProtocol::parse_name("InvalidProtocol"), None);
    }
}
