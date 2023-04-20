//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

/// Replica trait to be implement by all protocol-specific server structs.
pub trait GenericReplica {
    /// Get the ID of this replica.
    fn id(&self) -> u8;

    /// Get the cluster size (number of replicas).
    fn population(&self) -> u8;

    /// Get the address string of this replica.
    fn addr(&self) -> &str;
}

/// Dummy replica type, mainly for testing purposes.
pub struct DummyReplica {
    id: u8,
    population: u8,
    addr: String,
}

impl DummyReplica {
    pub fn new() -> Self {
        DummyReplica {
            id: 0,
            population: 5,
            addr: "127.0.0.1:52800".into(),
        }
    }
}

impl GenericReplica for DummyReplica {
    fn id(&self) -> u8 {
        self.id
    }

    fn population(&self) -> u8 {
        self.population
    }

    fn addr(&self) -> &str {
        &self.addr
    }
}
