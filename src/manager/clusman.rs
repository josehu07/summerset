//! Summerset cluster manager oracle implementation.

use crate::utils::SummersetError;
use crate::server::ReplicaId;

use serde::{Serialize, Deserialize};

/// Control events for testing purposes.
// TODO: add pause, resume, disconnect, leader change, view change, etc.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum CtrlEvent {
    /// Boot .
    Boot,
}

pub struct ClusterManager {}
