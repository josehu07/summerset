//! Summerset's collection of replication protocols.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::server::{GenericReplica, ReplicaId};
use crate::client::{GenericClient, ClientId};

mod rep_nothing;
use rep_nothing::{RepNothingReplica, RepNothingClient};
pub use rep_nothing::{ReplicaConfigRepNothing, ClientConfigRepNothing};

mod simple_push;
use simple_push::{SimplePushReplica, SimplePushClient};
pub use simple_push::{ReplicaConfigSimplePush, ClientConfigSimplePush};

mod multipaxos;
use multipaxos::{MultiPaxosReplica, MultiPaxosClient};
pub use multipaxos::{ReplicaConfigMultiPaxos, ClientConfigMultiPaxos};

/// Enum of supported replication protocol types.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SMRProtocol {
    RepNothing,
    SimplePush,
    MultiPaxos,
}

/// Helper macro for saving boilder-plate `Box<dyn ..>` mapping in
/// protocol-specific struct creations.
macro_rules! box_if_ok {
    ($thing:expr) => {
        // explicitly coerce to unsized `Box<dyn ..>`
        $thing.map(|o| Box::new(o) as _)
    };
}

impl SMRProtocol {
    /// Parse command line string into SMRProtocol enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match name {
            "RepNothing" => Some(Self::RepNothing),
            "SimplePush" => Some(Self::SimplePush),
            "MultiPaxos" => Some(Self::MultiPaxos),
            _ => None,
        }
    }

    /// Create a server replicator module instance of this protocol on heap.
    pub fn new_server_node(
        &self,
        id: ReplicaId,
        population: u8,
        api_addr: SocketAddr,
        conn_addrs: HashMap<ReplicaId, SocketAddr>,
        peer_addrs: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Box<dyn GenericReplica>, SummersetError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(RepNothingReplica::new(
                    id, population, api_addr, conn_addrs, peer_addrs,
                    config_str
                ))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushReplica::new(
                    id, population, api_addr, conn_addrs, peer_addrs,
                    config_str
                ))
            }
            Self::MultiPaxos => {
                box_if_ok!(MultiPaxosReplica::new(
                    id, population, api_addr, conn_addrs, peer_addrs,
                    config_str
                ))
            }
        }
    }

    /// Create a client replicator stub instance of this protocol on heap.
    pub fn new_client_stub(
        &self,
        id: ClientId,
        servers: HashMap<ReplicaId, SocketAddr>,
        config_str: Option<&str>,
    ) -> Result<Box<dyn GenericClient>, SummersetError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(RepNothingClient::new(id, servers, config_str))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushClient::new(id, servers, config_str))
            }
            Self::MultiPaxos => {
                box_if_ok!(MultiPaxosClient::new(id, servers, config_str))
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
    use super::*;

    macro_rules! valid_name_test {
        ($protocol:ident) => {
            assert_eq!(
                SMRProtocol::parse_name(stringify!($protocol)),
                Some(SMRProtocol::$protocol)
            );
        };
    }

    #[test]
    fn parse_valid_names() {
        valid_name_test!(RepNothing);
        valid_name_test!(SimplePush);
        valid_name_test!(MultiPaxos);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SMRProtocol::parse_name("InvalidProtocol"), None);
    }
}
