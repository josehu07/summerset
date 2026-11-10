//! `Raft` -- durable logging.

use super::*;
use crate::server::LogResult;
use crate::utils::SummersetError;

// RaftReplica durable logging
impl RaftReplica {
    /// Handler of leader append logging result chan recv.
    async fn handle_logged_leader_append(
        &mut self,
        slot: usize,
        term: Term,
    ) -> Result<(), SummersetError> {
        if slot < self.start_slot || self.role != Role::Leader || term != self.curr_term {
            return Ok(()); // ignore if outdated
        }
        pf_trace!(
            "finished leader append logging for slot {} term {}",
            slot,
            term
        );

        // broadcast AppendEntries messages to followers
        for peer in 0..self.population {
            if peer == self.id || self.try_next_slot[&peer] < 1 {
                continue;
            }

            let prev_slot = self.try_next_slot[&peer] - 1;
            if prev_slot < self.start_slot {
                return logged_err!("snapshotted slot {} queried", prev_slot);
            }
            if prev_slot >= self.start_slot + self.log.len() {
                continue;
            }

            if slot >= self.try_next_slot[&peer] {
                self.send_append_entries(peer, self.try_next_slot[&peer], slot)?;

                // update try_next_slot to avoid blindly sending the same
                // entries again on future triggers
                *self.try_next_slot.get_mut(&peer).unwrap() = slot + 1;
            }
        }

        // I also heard my own heartbeat
        self.heard_heartbeat(self.id, self.curr_term).await?;

        Ok(())
    }

    /// Handler of follower append logging result chan recv.
    fn handle_logged_follower_append(
        &mut self,
        slot: usize,
        slot_e: usize,
        term: Term,
    ) -> Result<(), SummersetError> {
        // a completion from an older term must not acknowledge entries to a
        // newer leader
        if slot < self.start_slot || self.role != Role::Follower || term != self.curr_term {
            return Ok(()); // ignore if outdated
        }
        pf_trace!(
            "finished follower append logging for slot {} <= {}",
            slot,
            slot_e
        );
        debug_assert!(slot <= slot_e);

        // if all consecutive entries are made durable, reply AppendEntries
        // success back to leader
        if slot == slot_e
            && let Some(leader) = self.leader
        {
            self.transport_hub.send_msg(
                PeerMsg::AppendEntriesReply {
                    term: self.curr_term,
                    end_slot: slot_e,
                    conflict: None,
                },
                leader,
            )?;
            pf_trace!(
                "sent AppendEntriesReply -> {} up to slot {}",
                leader,
                slot_e
            );
        }

        Ok(())
    }

    /// Synthesized handler of durable logging result chan recv.
    pub(super) async fn handle_log_result(
        &mut self,
        log_ctx: LogCtx,
        log_result: LogResult<DurEntry>,
    ) -> Result<(), SummersetError> {
        let LogResult::Append { offset, now_size } = log_result else {
            return logged_err!("unexpected log result type: {:?}", log_result);
        };
        debug_assert!(now_size > self.log_offset);
        self.log_offset = now_size;

        let slot = log_ctx.slot();
        if slot < self.start_slot {
            return Ok(()); // ignore if slot index outdated
        }
        debug_assert!(slot < self.start_slot + self.log.len());
        self.log[slot - self.start_slot].log_offset = Some(offset);

        match log_ctx {
            LogCtx::FollowerAppend {
                slot,
                end_slot,
                term,
            } => self.handle_logged_follower_append(slot, end_slot, term),
            LogCtx::LeaderAppend { slot, term } => {
                self.handle_logged_leader_append(slot, term).await
            }
        }
    }
}
