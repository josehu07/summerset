//! Summerset's collection of replication protocols.

mod do_nothing;

use crate::replicator::{ReplicatorServerNode, ReplicatorClientStub};
use crate::utils::InitError;

use do_nothing::{DoNothingServerNode, DoNothingClientStub};

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

    /// Create a server replicator module instance of this protocol on heap.
    pub fn new_server_node(
        &self,
        peers: &Vec<String>,
    ) -> Result<Box<dyn ReplicatorServerNode>, InitError> {
        match self {
            Self::DoNothing => DoNothingServerNode::new(peers),
        }
        .map(|s| Box::new(s) as _) // explicitly coerce to unsized Box<dyn ...>
    }

    /// Create a client replicator stub instance of this protocol on heap.
    pub fn new_client_stub(
        &self,
        servers: &Vec<String>,
    ) -> Result<Box<dyn ReplicatorClientStub>, InitError> {
        match self {
            Self::DoNothing => DoNothingClientStub::new(servers),
        }
        .map(|c| Box::new(c) as _) // explicitly coerce to unsized Box<dyn ...>
    }
}

#[cfg(test)]
mod protocols_tests {
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
