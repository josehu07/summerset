//! ChainRep -- peer-peer messaging.

use std::cmp;

use super::*;

use crate::utils::SummersetError;
use crate::server::{ReplicaId, LogAction};

// ChainRepReplica peer-peer messages handling
impl ChainRepReplica {
    /// Handler of Propagate message from predecessor.
    fn handle_msg_propagate(
        &mut self,
        peer: ReplicaId,
        slot: usize,
        reqs: ReqBatch,
    ) -> Result<(), SummersetError> {
        unimplemented!()
    }

    /// Handler of Propagate reply from successor.
    fn handle_msg_propagate_reply(
        &mut self,
        peer: ReplicaId,
        slot: usize,
    ) -> Result<(), SummersetError> {
        unimplemented!()
    }

    /// Synthesized handler of receiving message from peer.
    pub fn handle_msg_recv(
        &mut self,
        peer: ReplicaId,
        msg: PeerMsg,
    ) -> Result<(), SummersetError> {
        match msg {
            PeerMsg::Propagate { slot, reqs } => {
                self.handle_msg_propagate(peer, slot, reqs)
            }
            PeerMsg::PropagateReply { slot } => {
                self.handle_msg_propagate_reply(peer, slot)
            }
        }
    }
}
