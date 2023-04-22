//! Summerset server external API module implementation.

use crate::core::utils::{SummersetError, ReplicaId};
use crate::core::replica::GeneralReplica;

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use rmp_serde::encode::to_vec as encode_to_vec;
use rmp_serde::decode::from_slice as decode_from_slice;

use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use log::{trace, debug, error};

pub struct ExternalApi<'r, Rpl>
where
    Rpl: 'r + GeneralReplica,
{
    /// Reference to protocol-specific replica struct.
    replica: &'r Rpl,

    /// Receiver side of the reqst channel.
    rx_reqst: Option<mpsc::Receiver<()>>,

    /// Sender side of the reply channel.
    tx_reply: Option<mpsc::Sender<()>>,
}
