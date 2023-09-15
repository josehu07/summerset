//! Closed-loop & Open-loop client-side driver implementations.

use tokio::time::Duration;

use summerset::{ReplicaId, RequestId, CommandResult};

mod closed_loop;
mod open_loop;

pub use closed_loop::DriverClosedLoop;
pub use open_loop::DriverOpenLoop;

/// Reply result type, common across the two driver styles.
pub enum DriverReply {
    /// Successful reply.
    Success {
        /// Request ID.
        req_id: RequestId,
        /// Command result.
        cmd_result: CommandResult,
        /// Latency duration.
        latency: Duration,
    },

    /// Service indicated redirection.
    Redirect { server: ReplicaId },

    /// Unknown failure.
    Failure,

    /// Client-side timer timeout.
    Timeout,
}
