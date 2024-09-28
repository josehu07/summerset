//! ZooKeeper client implementation that translates Summerset Puts/Gets.

use serde::Deserialize;

mod bench;
mod session;

pub(crate) use bench::ZooKeeperBench;
pub(crate) use session::ZooKeeperSession;

/// ZooKeeper-specific configuration parameters.
#[derive(Debug, Deserialize)]
pub(crate) struct ClientConfigZooKeeper {
    /// Session expiry timeout in millisecs (0 means use preset default).
    pub(crate) expiry_ms: u64,

    /// Do a `sync` before every `get` for stricter consistency (though still
    /// not linearizability).
    pub(crate) sync_on_get: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigZooKeeper {
    fn default() -> Self {
        ClientConfigZooKeeper {
            expiry_ms: 0,
            sync_on_get: false,
        }
    }
}
