//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::net::SocketAddr;

use crate::core::utils::SummersetError;

use async_trait::async_trait;

use log::error;

/// Server replica ID type.
pub type ReplicaId = u8;

/// Replica trait to be implement by all protocol-specific server structs.
#[async_trait]
pub trait GenericReplica {
    /// Get the ID of this replica.
    fn id(&self) -> ReplicaId;

    /// Get the cluster size (number of replicas).
    fn population(&self) -> u8;

    /// Get the address string of this replica on peer-to-peer connections.
    fn smr_addr(&self) -> &SocketAddr;

    /// Get the address string of this replica on client requests.
    fn api_addr(&self) -> &SocketAddr;

    /// Set up required functionality modules.
    async fn setup(&mut self) -> Result<(), SummersetError>;

    /// Main event loop logic of running this replica.
    async fn run(&mut self) -> Result<(), SummersetError>;
}

/// Dummy replica type, mainly for testing purposes.
#[derive(Debug)]
pub struct DummyReplica {
    id: ReplicaId,
    population: u8,
    smr_addr: SocketAddr,
    api_addr: SocketAddr,
}

impl DummyReplica {
    pub fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: SocketAddr,
        api_addr: SocketAddr,
    ) -> Result<Self, SummersetError> {
        if population == 0 {
            return Err(SummersetError(format!(
                "invalid population {}",
                population
            )));
        }
        if id >= population {
            return Err(SummersetError(format!(
                "invalid replica ID {} / {}",
                id, population
            )));
        }
        if smr_addr == api_addr {
            return logged_err!(id, "smr_addr == api_addr '{}'", smr_addr);
        }

        Ok(DummyReplica {
            id,
            population,
            smr_addr,
            api_addr,
        })
    }
}

impl Default for DummyReplica {
    fn default() -> Self {
        DummyReplica {
            id: 0,
            population: 3,
            smr_addr: "127.0.0.1:52800".into(),
            api_addr: "127.0.0.1:52700".into(),
        }
    }
}

#[async_trait]
impl GenericReplica for DummyReplica {
    fn id(&self) -> ReplicaId {
        self.id
    }

    fn population(&self) -> u8 {
        self.population
    }

    fn smr_addr(&self) -> &SocketAddr {
        &self.smr_addr
    }

    fn api_addr(&self) -> &SocketAddr {
        &self.api_addr
    }

    async fn setup(&mut self) -> Result<(), SummersetError> {
        Err(SummersetError("setting up DummyReplica".into()))
    }

    async fn run(&mut self) -> Result<(), SummersetError> {
        Err(SummersetError("running DummyReplica".into()))
    }
}

#[cfg(test)]
mod dummy_replica_test {
    use super::*;

    #[test]
    fn dummy_replica_new() {
        assert!(DummyReplica::new(
            0,
            0,
            "127.0.0.1:52800".into(),
            "127.0.0.1:52700".into()
        )
        .is_err());
        assert!(DummyReplica::new(
            3,
            3,
            "127.0.0.1:52800".into(),
            "127.0.0.1:52700".into()
        )
        .is_err());
        assert!(DummyReplica::new(
            0,
            5,
            "987abc123".into(),
            "127.0.0.1:52700".into()
        )
        .is_err());
        assert!(DummyReplica::new(
            0,
            5,
            "127.0.0.1:52800".into(),
            "987abc123".into()
        )
        .is_err());
        assert!(DummyReplica::new(
            0,
            5,
            "127.0.0.1:52800".into(),
            "127.0.0.1:52700".into()
        )
        .is_ok());
    }

    #[test]
    fn dummy_replica_id() {
        let dr = DummyReplica::new(
            0,
            7,
            "127.0.0.1:52800".into(),
            "127.0.0.1:52700".into(),
        )
        .unwrap();
        assert_eq!(dr.id(), 0);
    }

    #[test]
    fn dummy_replica_population() {
        let dr = DummyReplica::new(
            0,
            7,
            "127.0.0.1:52800".into(),
            "127.0.0.1:52700".into(),
        )
        .unwrap();
        assert_eq!(dr.population(), 7);
    }

    #[test]
    fn dummy_replica_addr() {
        let dr = DummyReplica::new(
            0,
            7,
            "127.0.0.1:52800".into(),
            "127.0.0.1:52700".into(),
        )
        .unwrap();
        assert_eq!(dr.smr_addr(), "127.0.0.1:52800");
        assert_eq!(dr.api_addr(), "127.0.0.1:52700");
    }
}
