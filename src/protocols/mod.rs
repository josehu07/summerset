//! Summerset's collection of replication protocols.

use std::fmt;

mod rep_nothing;
use rep_nothing::{RepNothingServerNode, RepNothingClientStub};

mod simple_push;
use simple_push::{SimplePushServerNode, SimplePushClientStub};

/// Helper macro for saving boilder-plate `Box<dyn ..>` mapping in
/// protocol-specific struct creations.
#[allow(unused_macros)]
macro_rules! box_if_ok {
    ($r:expr) => {
        $r.map(|o| Box::new(o) as _) // explicitly coerce to unsized Box<dyn ..>
    };
}

/// Enum of supported replication protocol types.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SMRProtocol {
    RepNothing,
    SimplePush,
}

impl SMRProtocol {
    /// Parse command line string into SMRProtocol enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match name {
            "RepNothing" => Some(Self::RepNothing),
            "SimplePush" => Some(Self::SimplePush),
            _ => None,
        }
    }

    /// Create a server replicator module instance of this protocol on heap.
    pub fn new_server_node(
        &self,
        peers: Vec<String>,
    ) -> Result<Box<dyn ReplicatorServerNode>, InitError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(RepNothingServerNode::new(peers))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushServerNode::new(peers))
            }
        }
    }

    /// Create a client replicator stub instance of this protocol on heap.
    pub fn new_client_stub(
        &self,
        servers: Vec<String>,
    ) -> Result<Box<dyn ReplicatorClientStub>, InitError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(RepNothingClientStub::new(servers))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushClientStub::new(servers))
            }
        }
    }
}

impl fmt::Display for SMRProtocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod protocols_name_tests {
    use super::SMRProtocol;

    macro_rules! valid_name_test {
        ($p:ident) => {
            assert_eq!(
                SMRProtocol::parse_name(stringify!($p)),
                Some(SMRProtocol::$p)
            );
        };
    }

    #[test]
    fn parse_valid_names() {
        valid_name_test!(RepNothing);
        valid_name_test!(SimplePush);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SMRProtocol::parse_name("InvalidProtocol"), None);
    }
}
