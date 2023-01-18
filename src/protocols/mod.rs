//! Summerset's collection of replication protocols.

mod do_nothing;
mod simple_push;

use crate::replicator::{ReplicatorServerNode, ReplicatorClientStub};
use crate::utils::InitError;

use do_nothing::{DoNothingServerNode, DoNothingClientStub};
use simple_push::{SimplePushServerNode, SimplePushClientStub};

use std::net::SocketAddr;
use tokio::runtime::Runtime;

/// Helper macro for saving boilder-plate `Box<dyn ..>` mapping.
macro_rules! box_if_ok {
    ($r:expr) => {
        $r.map(|o| Box::new(o) as _) // explicitly coerce to unsized Box<dyn ..>
    };
}

/// Enum of supported replication protocol types.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SMRProtocol {
    DoNothing,
    SimplePush,
}

impl SMRProtocol {
    /// Parse command line string into SMRProtocol enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match name {
            "DoNothing" => Some(Self::DoNothing),
            "SimplePush" => Some(Self::SimplePush),
            _ => None,
        }
    }

    /// Create a server replicator module instance of this protocol on heap.
    pub fn new_server_node(
        &self,
        peers: Vec<String>,
        smr_addr: SocketAddr,
        main_runtime: &Runtime,
    ) -> Result<Box<dyn ReplicatorServerNode>, InitError> {
        match self {
            Self::DoNothing => {
                box_if_ok!(DoNothingServerNode::new(
                    peers,
                    smr_addr,
                    main_runtime
                ))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushServerNode::new(
                    peers,
                    smr_addr,
                    main_runtime
                ))
            }
        }
    }

    /// Create a client replicator stub instance of this protocol on heap.
    pub fn new_client_stub(
        &self,
        servers: Vec<String>,
    ) -> Result<Box<dyn ReplicatorClientStub>, InitError> {
        match self {
            Self::DoNothing => {
                box_if_ok!(DoNothingClientStub::new(servers))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushClientStub::new(servers))
            }
        }
    }
}

#[cfg(test)]
mod protocols_tests {
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
        valid_name_test!(DoNothing);
        valid_name_test!(SimplePush);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SMRProtocol::parse_name("InvalidProtocol"), None);
    }
}
