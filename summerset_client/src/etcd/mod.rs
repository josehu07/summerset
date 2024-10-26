//! etcd client implementation that translates Summerset Puts/Gets.

use serde::Deserialize;

mod kvclient;
pub(crate) use kvclient::EtcdKvClient;

mod bench;
pub(crate) use bench::EtcdBench;

/// etcd-specific configuration parameters.
#[derive(Debug, Deserialize)]
pub(crate) struct ClientConfigEtcd {
    /// Connection & per-req timeout in millisecs (0 means not setting them).
    pub(crate) timeout_ms: u64,

    /// Use sequential consistency on reads (i.e., stale reads), instead of
    /// linearizable reads.
    pub(crate) stale_reads: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ClientConfigEtcd {
    fn default() -> Self {
        ClientConfigEtcd {
            timeout_ms: 0,
            stale_reads: false,
        }
    }
}
