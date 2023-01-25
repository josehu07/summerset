//! Summerset's collection of replication protocols.

use std::fmt;
use std::sync::Arc;

use crate::smr_server::SummersetServerNode;
use crate::replicator::{
    ReplicatorServerNode, ReplicatorCommService, ReplicatorClientStub,
};
use crate::utils::InitError;

#[cfg(feature = "protocol_do_nothing")]
mod do_nothing;
#[cfg(feature = "protocol_do_nothing")]
use do_nothing::{DoNothingServerNode, DoNothingCommService, DoNothingClientStub};

#[cfg(feature = "protocol_simple_push")]
mod simple_push;
#[cfg(feature = "protocol_simple_push")]
use simple_push::{
    SimplePushServerNode, SimplePushCommService, SimplePushClientStub,
};

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
    #[allow(unused_variables, unreachable_patterns)]
    pub fn new_server_node(
        &self,
        peers: Vec<String>,
    ) -> Result<Box<dyn ReplicatorServerNode>, InitError> {
        match self {
            #[cfg(feature = "protocol_do_nothing")]
            Self::DoNothing => {
                box_if_ok!(DoNothingServerNode::new(peers))
            }
            #[cfg(feature = "protocol_simple_push")]
            Self::SimplePush => {
                box_if_ok!(SimplePushServerNode::new(peers))
            }
            _ => Err(InitError(format!(
                "protocol {} is not enabled in cargo features",
                self
            ))),
        }
    }

    /// Create a server internal communication tonic service holder struct.
    #[allow(unused_variables, unreachable_patterns)]
    pub fn new_comm_service(
        &self,
        node: Arc<SummersetServerNode>,
    ) -> Result<Box<dyn ReplicatorCommService>, InitError> {
        match self {
            #[cfg(feature = "protocol_do_nothing")]
            Self::DoNothing => {
                box_if_ok!(DoNothingCommService::new(node))
            }
            #[cfg(feature = "protocol_simple_push")]
            Self::SimplePush => {
                box_if_ok!(SimplePushCommService::new(node))
            }
            _ => Err(InitError(format!(
                "protocol {} is not enabled in cargo features",
                self
            ))),
        }
    }

    /// Create a client replicator stub instance of this protocol on heap.
    #[allow(unused_variables, unreachable_patterns)]
    pub fn new_client_stub(
        &self,
        servers: Vec<String>,
    ) -> Result<Box<dyn ReplicatorClientStub>, InitError> {
        match self {
            #[cfg(feature = "protocol_do_nothing")]
            Self::DoNothing => {
                box_if_ok!(DoNothingClientStub::new(servers))
            }
            #[cfg(feature = "protocol_simple_push")]
            Self::SimplePush => {
                box_if_ok!(SimplePushClientStub::new(servers))
            }
            _ => Err(InitError(format!(
                "protocol {} is not enabled in cargo features",
                self
            ))),
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
        valid_name_test!(DoNothing);
        valid_name_test!(SimplePush);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SMRProtocol::parse_name("InvalidProtocol"), None);
    }
}
