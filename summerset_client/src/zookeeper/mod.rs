//! ZooKeeper client implementation that translates Summerset Puts/Gets.

use serde::Deserialize;

mod bench;
mod session;

pub(crate) use bench::ZooKeeperBench;
pub(crate) use session::ZooKeeperSession;

/// ZooKeeper-specific configuration parameters.
#[derive(Debug, Deserialize)]
pub(crate) struct ClientConfigZooKeeper {
    /// Session expiry timeout in millisecs (0 means use server default).
    pub(crate) expiry_ms: u64,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigZooKeeper {
    fn default() -> Self {
        ClientConfigZooKeeper { expiry_ms: 0 }
    }
}
