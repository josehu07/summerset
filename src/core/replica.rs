//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::net::SocketAddr;

use crate::core::utils::{SummersetError, ReplicaId};

/// Replica trait to be implement by all protocol-specific server structs.
pub trait GenericReplica {
    /// Get the ID of this replica.
    fn id(&self) -> ReplicaId;

    /// Get the cluster size (number of replicas).
    fn population(&self) -> u8;

    /// Get the address string of this replica.
    fn addr(&self) -> &str;
}

/// Dummy replica type, mainly for testing purposes.
pub struct DummyReplica {
    id: ReplicaId,
    population: u8,
    addr: String,
}

impl DummyReplica {
    pub fn new(
        id: ReplicaId,
        population: u8,
        addr: String,
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
        if let Err(_) = addr.parse::<SocketAddr>() {
            return Err(SummersetError(format!(
                "invalid addr string '{}'",
                addr
            )));
        }

        Ok(DummyReplica {
            id,
            population,
            addr,
        })
    }
}

impl GenericReplica for DummyReplica {
    fn id(&self) -> ReplicaId {
        self.id
    }

    fn population(&self) -> u8 {
        self.population
    }

    fn addr(&self) -> &str {
        &self.addr
    }
}

#[cfg(test)]
mod replica_test {
    use super::*;

    #[test]
    fn dummy_replica_new() {
        assert!(DummyReplica::new(0, 0, "127.0.0.1:52800".into()).is_err());
        assert!(DummyReplica::new(3, 3, "127.0.0.1:52800".into()).is_err());
        assert!(DummyReplica::new(0, 5, "987abc123".into()).is_err());
        assert!(DummyReplica::new(0, 5, "127.0.0.1:52800".into()).is_ok());
    }

    #[test]
    fn dummy_replica_id() {
        let dr = DummyReplica::new(0, 7, "127.0.0.1:52800".into()).unwrap();
        assert_eq!(dr.id(), 0);
    }

    #[test]
    fn dummy_replica_population() {
        let dr = DummyReplica::new(0, 7, "127.0.0.1:52800".into()).unwrap();
        assert_eq!(dr.population(), 7);
    }

    #[test]
    fn dummy_replica_addr() {
        let dr = DummyReplica::new(0, 7, "127.0.0.1:52800".into()).unwrap();
        assert_eq!(dr.addr(), "127.0.0.1:52800");
    }
}
