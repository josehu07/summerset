//! SimplePush -- peer-peer messaging.

use super::*;

use crate::utils::{SummersetError, Bitmap};
use crate::server::{ReplicaId, ApiRequest, LogAction, LogActionId};
use crate::client::ClientId;

// SimplePushReplica peer-peer messages handling
impl SimplePushReplica {
    /// Handler of push message from peer.
    pub fn handle_push_msg(
        &mut self,
        peer: ReplicaId,
        src_inst_idx: usize,
        req_batch: Vec<(ClientId, ApiRequest)>,
    ) -> Result<(), SummersetError> {
        let inst = Instance {
            reqs: req_batch.clone(),
            durable: false,
            pending_peers: Bitmap::new(self.population, false),
            execed: vec![false; req_batch.len()],
            from_peer: Some((peer, src_inst_idx)),
        };
        let inst_idx = self.insts.len();
        self.insts.push(inst);

        // submit log action to make this instance durable
        let wal_entry = WalEntry::PeerPushed {
            peer,
            src_inst_idx,
            reqs: req_batch.clone(),
        };
        self.storage_hub.submit_action(
            inst_idx as LogActionId,
            LogAction::Append {
                entry: wal_entry,
                sync: true,
            },
        )?;

        Ok(())
    }

    /// Handler of push reply from peer.
    pub fn handle_push_reply(
        &mut self,
        peer: ReplicaId,
        inst_idx: usize,
        num_reqs: usize,
    ) -> Result<(), SummersetError> {
        if inst_idx >= self.insts.len() {
            return logged_err!(self.id; "invalid src_inst_idx {} seen", inst_idx);
        }

        let inst = &mut self.insts[inst_idx];
        if inst.from_peer.is_some() {
            return logged_err!(self.id; "from_peer should not be set for {}", inst_idx);
        }
        if inst.pending_peers.count() == 0 {
            return logged_err!(self.id; "pending_peers already 0 for {}", inst_idx);
        }
        if !inst.pending_peers.get(peer)? {
            return logged_err!(self.id; "unexpected push reply from peer {} for {}",
                                        peer, inst_idx);
        }
        if num_reqs != inst.reqs.len() {
            return logged_err!(self.id; "num_reqs mismatch: expected {}, got {}",
                                        inst.reqs.len(), num_reqs);
        }
        inst.pending_peers.set(peer, false)?;

        // if pushed peers have all replied and the logging on myself has
        // completed as well, submit execution commands
        if inst.pending_peers.count() == 0 && inst.durable {
            for (cmd_idx, (_, req)) in inst.reqs.iter().enumerate() {
                match req {
                    ApiRequest::Req { cmd, .. } => {
                        self.state_machine.submit_cmd(
                            Self::make_command_id(inst_idx, cmd_idx),
                            cmd.clone(),
                        )?
                    }
                    _ => continue, // ignore other types of requests
                }
            }
        }

        Ok(())
    }
}
