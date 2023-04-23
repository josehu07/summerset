//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use std::net::SocketAddr;

use crate::core::utils::SummersetError;

/// Server replica ID type.
pub type ReplicaId = u8;

/// Replica trait to be implement by all protocol-specific server structs.
pub trait GeneralReplica {
    /// Get the ID of this replica.
    fn id(&self) -> ReplicaId;

    /// Get the cluster size (number of replicas).
    fn population(&self) -> u8;

    /// Get the address string of this replica on peer-to-peer connections.
    fn smr_addr(&self) -> &str;

    /// Get the address string of this replica on client requests.
    fn api_addr(&self) -> &str;
}

/// Dummy replica type, mainly for testing purposes.
#[derive(Debug)]
pub struct DummyReplica {
    id: ReplicaId,
    population: u8,
    smr_addr: String,
    api_addr: String,
}

impl DummyReplica {
    pub fn new(
        id: ReplicaId,
        population: u8,
        smr_addr: String,
        api_addr: String,
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
        if let Err(_) = smr_addr.parse::<SocketAddr>() {
            return Err(SummersetError(format!(
                "invalid smr_addr string '{}'",
                smr_addr
            )));
        }
        if let Err(_) = api_addr.parse::<SocketAddr>() {
            return Err(SummersetError(format!(
                "invalid api_addr string '{}'",
                api_addr
            )));
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

impl GeneralReplica for DummyReplica {
    fn id(&self) -> ReplicaId {
        self.id
    }

    fn population(&self) -> u8 {
        self.population
    }

    fn smr_addr(&self) -> &str {
        &self.smr_addr
    }

    fn api_addr(&self) -> &str {
        &self.api_addr
    }
}

#[cfg(test)]
mod replica_test {
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
