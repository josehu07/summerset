//! Summerset generic replica trait to be implemented by all protocol-specific
//! server replica structs.

use crate::utils::SummersetError;

/// Replica trait to be implement by all protocol-specific server structs.
pub trait GenericReplica {
    /// Create a new replica struct.
    fn new(id: u8) -> Result<Self, SummersetError>;

    /// Get the ID of this replica.
    fn id(&self) -> u8;
}

/// Dummy replica type, mainly for testing purposes.
pub struct DummyReplica {
    id: u8,
}

impl GenericReplica for DummyReplica {
    fn new(id: u8) -> Result<Self, SummersetError> {
        Ok(DummyReplica { id })
    }

    fn id(&self) -> u8 {
        self.id
    }
}
