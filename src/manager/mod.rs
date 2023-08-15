//! Summerset

/// Control events for testing purposes.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum CtrlEvent {
    /// Reset replica to initial state.
    Reset,

    /// Pause the replica's functionality.
    Pause,

    /// Resume the replica's functionality.
    Resume,

    /// Simulate crash: lose everything in memory and pause.
    Crash,

    /// Simulate reboot: resume and load persistent information.
    Reload,
}
