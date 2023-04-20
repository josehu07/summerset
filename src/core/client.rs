//! Summerset generic client trait to be implemented by all protocol-specific
//! client stub structs.

use crate::core::utils::SummersetError;

/// Client trait to be implement by all protocol-specific client structs.
pub trait GenericClient {
    /// Create a new client struct.
    fn new(name: String) -> Result<Self, SummersetError>;
}

/// Dummy client type, mainly for testing purposes.
pub struct DummyClient {
    name: String,
}

impl GenericClient for DummyClient {
    fn new(name: String) -> Result<Self, SummersetError> {
        Ok(DummyClient { name })
    }
}
