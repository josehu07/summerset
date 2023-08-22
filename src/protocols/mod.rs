//! Summerset's collection of replication protocols.

use std::fmt;
use std::net::SocketAddr;

use crate::utils::SummersetError;
use crate::manager::ClusterManager;
use crate::server::GenericReplica;
use crate::client::GenericEndpoint;

use serde::{Serialize, Deserialize};

mod rep_nothing;
use rep_nothing::{RepNothingReplica, RepNothingClient};
pub use rep_nothing::{ReplicaConfigRepNothing, ClientConfigRepNothing};

mod simple_push;
use simple_push::{SimplePushReplica, SimplePushClient};
pub use simple_push::{ReplicaConfigSimplePush, ClientConfigSimplePush};

mod multipaxos;
use multipaxos::{MultiPaxosReplica, MultiPaxosClient};
pub use multipaxos::{ReplicaConfigMultiPaxos, ClientConfigMultiPaxos};

mod rs_paxos;
use rs_paxos::{RSPaxosReplica, RSPaxosClient};
pub use rs_paxos::{ReplicaConfigRSPaxos, ClientConfigRSPaxos};

/// Enum of supported replication protocol types.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum SmrProtocol {
    RepNothing,
    SimplePush,
    MultiPaxos,
    RSPaxos,
}

/// Helper macro for saving boilder-plate `Box<dyn ..>` mapping in
/// protocol-specific struct creations.
macro_rules! box_if_ok {
    ($thing:expr) => {
        // explicitly coerce to unsized `Box<dyn ..>`
        $thing.map(|o| Box::new(o) as _)
    };
}

impl SmrProtocol {
    /// Parse command line string into SmrProtocol enum.
    pub fn parse_name(name: &str) -> Option<Self> {
        match name {
            "RepNothing" => Some(Self::RepNothing),
            "SimplePush" => Some(Self::SimplePush),
            "MultiPaxos" => Some(Self::MultiPaxos),
            "RSPaxos" => Some(Self::RSPaxos),
            _ => None,
        }
    }

    /// Create the cluster manager for this protocol.
    pub async fn new_cluster_manager_setup(
        &self,
        srv_addr: SocketAddr,
        cli_addr: SocketAddr,
        population: u8,
    ) -> Result<ClusterManager, SummersetError> {
        ClusterManager::new_and_setup(*self, srv_addr, cli_addr, population)
            .await
    }

    /// Create a server replica instance of this protocol on heap.
    pub async fn new_server_replica_setup(
        &self,
        api_addr: SocketAddr,
        p2p_addr: SocketAddr,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Box<dyn GenericReplica>, SummersetError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(
                    RepNothingReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::SimplePush => {
                box_if_ok!(
                    SimplePushReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::MultiPaxos => {
                box_if_ok!(
                    MultiPaxosReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::RSPaxos => {
                box_if_ok!(
                    RSPaxosReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
        }
    }

    /// Create a client endpoint instance of this protocol on heap.
    pub fn new_client_endpoint(
        &self,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Box<dyn GenericEndpoint>, SummersetError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(RepNothingClient::new(manager, config_str))
            }
            Self::SimplePush => {
                box_if_ok!(SimplePushClient::new(manager, config_str))
            }
            Self::MultiPaxos => {
                box_if_ok!(MultiPaxosClient::new(manager, config_str))
            }
            Self::RSPaxos => {
                box_if_ok!(RSPaxosClient::new(manager, config_str))
            }
        }
    }
}

impl fmt::Display for SmrProtocol {
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
                SmrProtocol::parse_name(stringify!($protocol)),
                Some(SmrProtocol::$protocol)
            );
        };
    }

    #[test]
    fn parse_valid_names() {
        valid_name_test!(RepNothing);
        valid_name_test!(SimplePush);
        valid_name_test!(MultiPaxos);
        valid_name_test!(RSPaxos);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SmrProtocol::parse_name("InvalidProtocol"), None);
    }
}
