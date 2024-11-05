//! Summerset's collection of replication protocols.

use std::fmt;
use std::net::SocketAddr;

use crate::client::GenericEndpoint;
use crate::manager::ClusterManager;
use crate::server::GenericReplica;
use crate::utils::SummersetError;

use serde::{Deserialize, Serialize};

mod rep_nothing;
pub use rep_nothing::{ClientConfigRepNothing, ReplicaConfigRepNothing};
use rep_nothing::{RepNothingClient, RepNothingReplica};

mod simple_push;
pub use simple_push::{ClientConfigSimplePush, ReplicaConfigSimplePush};
use simple_push::{SimplePushClient, SimplePushReplica};

mod chain_rep;
use chain_rep::{ChainRepClient, ChainRepReplica};
pub use chain_rep::{ClientConfigChainRep, ReplicaConfigChainRep};

mod multipaxos;
pub use multipaxos::{ClientConfigMultiPaxos, ReplicaConfigMultiPaxos};
use multipaxos::{MultiPaxosClient, MultiPaxosReplica};

mod epaxos;
pub use epaxos::{ClientConfigEPaxos, ReplicaConfigEPaxos};
use epaxos::{EPaxosClient, EPaxosReplica};

mod rspaxos;
pub use rspaxos::{ClientConfigRSPaxos, ReplicaConfigRSPaxos};
use rspaxos::{RSPaxosClient, RSPaxosReplica};

mod raft;
pub use raft::{ClientConfigRaft, ReplicaConfigRaft};
use raft::{RaftClient, RaftReplica};

mod craft;
use craft::{CRaftClient, CRaftReplica};
pub use craft::{ClientConfigCRaft, ReplicaConfigCRaft};

mod crossword;
pub use crossword::{ClientConfigCrossword, ReplicaConfigCrossword};
use crossword::{CrosswordClient, CrosswordReplica};

mod quorum_leases;
pub use quorum_leases::{ClientConfigQuorumLeases, ReplicaConfigQuorumLeases};
use quorum_leases::{QuorumLeasesClient, QuorumLeasesReplica};

mod bodega;
use bodega::{BodegaClient, BodegaReplica};
pub use bodega::{ClientConfigBodega, ReplicaConfigBodega};

/// Enum of supported replication protocol types.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum SmrProtocol {
    RepNothing,
    SimplePush,
    ChainRep,
    MultiPaxos,
    EPaxos,
    RSPaxos,
    Raft,
    CRaft,
    Crossword,
    QuorumLeases,
    Bodega,
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
            "ChainRep" => Some(Self::ChainRep),
            "MultiPaxos" => Some(Self::MultiPaxos),
            "EPaxos" => Some(Self::EPaxos),
            "RSPaxos" => Some(Self::RSPaxos),
            "Raft" => Some(Self::Raft),
            "CRaft" => Some(Self::CRaft),
            "Crossword" => Some(Self::Crossword),
            "QuorumLeases" => Some(Self::QuorumLeases),
            "Bodega" => Some(Self::Bodega),
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
            Self::ChainRep => {
                box_if_ok!(
                    ChainRepReplica::new_and_setup(
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
            Self::EPaxos => {
                box_if_ok!(
                    EPaxosReplica::new_and_setup(
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
            Self::Raft => {
                box_if_ok!(
                    RaftReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::CRaft => {
                box_if_ok!(
                    CRaftReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::Crossword => {
                box_if_ok!(
                    CrosswordReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::QuorumLeases => {
                box_if_ok!(
                    QuorumLeasesReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
            Self::Bodega => {
                box_if_ok!(
                    BodegaReplica::new_and_setup(
                        api_addr, p2p_addr, manager, config_str
                    )
                    .await
                )
            }
        }
    }

    /// Create a client endpoint instance of this protocol on heap.
    pub async fn new_client_endpoint(
        &self,
        manager: SocketAddr,
        config_str: Option<&str>,
    ) -> Result<Box<dyn GenericEndpoint>, SummersetError> {
        match self {
            Self::RepNothing => {
                box_if_ok!(
                    RepNothingClient::new_and_setup(manager, config_str).await
                )
            }
            Self::SimplePush => {
                box_if_ok!(
                    SimplePushClient::new_and_setup(manager, config_str).await
                )
            }
            Self::ChainRep => {
                box_if_ok!(
                    ChainRepClient::new_and_setup(manager, config_str).await
                )
            }
            Self::MultiPaxos => {
                box_if_ok!(
                    MultiPaxosClient::new_and_setup(manager, config_str).await
                )
            }
            Self::EPaxos => {
                box_if_ok!(
                    EPaxosClient::new_and_setup(manager, config_str).await
                )
            }
            Self::RSPaxos => {
                box_if_ok!(
                    RSPaxosClient::new_and_setup(manager, config_str).await
                )
            }
            Self::Raft => {
                box_if_ok!(RaftClient::new_and_setup(manager, config_str).await)
            }
            Self::CRaft => {
                box_if_ok!(
                    CRaftClient::new_and_setup(manager, config_str).await
                )
            }
            Self::Crossword => {
                box_if_ok!(
                    CrosswordClient::new_and_setup(manager, config_str).await
                )
            }
            Self::QuorumLeases => {
                box_if_ok!(
                    QuorumLeasesClient::new_and_setup(manager, config_str)
                        .await
                )
            }
            Self::Bodega => {
                box_if_ok!(
                    BodegaClient::new_and_setup(manager, config_str).await
                )
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
mod name_tests {
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
        valid_name_test!(ChainRep);
        valid_name_test!(MultiPaxos);
        valid_name_test!(EPaxos);
        valid_name_test!(RSPaxos);
        valid_name_test!(Raft);
        valid_name_test!(CRaft);
        valid_name_test!(Crossword);
        valid_name_test!(QuorumLeases);
        valid_name_test!(Bodega);
    }

    #[test]
    fn parse_invalid_name() {
        assert_eq!(SmrProtocol::parse_name("InvalidProtocol"), None);
    }
}
