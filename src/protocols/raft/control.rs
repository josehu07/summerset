//! Raft -- manager control actions.

use super::*;

use crate::manager::CtrlMsg;
use crate::server::{LogAction, LogResult};
use crate::utils::SummersetError;

// RaftReplica control messages handling
impl RaftReplica {
    /// Handler of ResetState control message.
    async fn handle_ctrl_reset_state(
        &mut self,
        durable: bool,
    ) -> Result<(), SummersetError> {
        pf_warn!("server got restart req");

        // send leave notification to peers and wait for their replies
        self.transport_hub.leave().await?;

        // send leave notification to manager and wait for its reply
        self.control_hub
            .do_sync_ctrl(CtrlMsg::Leave, |m| m == &CtrlMsg::LeaveReply)
            .await?;

        // if `durable` is false, truncate backer file
        if !durable
            && self
                .storage_hub
                .do_sync_action(
                    0, // using 0 as dummy log action ID
                    LogAction::Truncate { offset: 0 },
                )
                .await?
                .1
                != (LogResult::Truncate {
                    offset_ok: true,
                    now_size: 0,
                })
        {
            return logged_err!("failed to truncate log to 0");
        }

        Ok(())
    }

    /// Handler of Pause control message.
    fn handle_ctrl_pause(
        &mut self,
        paused: &mut bool,
    ) -> Result<(), SummersetError> {
        pf_warn!("server got pause req");
        *paused = true;
        self.control_hub.send_ctrl(CtrlMsg::PauseReply)?;
        Ok(())
    }

    /// Handler of Resume control message.
    fn handle_ctrl_resume(
        &mut self,
        paused: &mut bool,
    ) -> Result<(), SummersetError> {
        pf_warn!("server got resume req");

        // reset leader heartbeat timer
        if !self.config.disable_hb_timer {
            self.heartbeater.kickoff_hear_timer(None)?;
        }

        *paused = false;
        self.control_hub.send_ctrl(CtrlMsg::ResumeReply)?;
        Ok(())
    }

    /// Handler of TakeSnapshot control message.
    async fn handle_ctrl_take_snapshot(
        &mut self,
    ) -> Result<(), SummersetError> {
        pf_warn!("server told to take snapshot");
        self.take_new_snapshot().await?;

        self.control_hub.send_ctrl(CtrlMsg::SnapshotUpTo {
            new_start: self.start_slot,
        })?;
        Ok(())
    }

    /// Synthesized handler of manager control messages. If ok, returns
    /// `Some(true)` if decides to terminate and reboot, `Some(false)` if
    /// decides to shutdown completely, and `None` if not terminating.
    pub(super) async fn handle_ctrl_msg(
        &mut self,
        msg: CtrlMsg,
        paused: &mut bool,
    ) -> Result<Option<bool>, SummersetError> {
        match msg {
            CtrlMsg::ResetState { durable } => {
                self.handle_ctrl_reset_state(durable).await?;
                Ok(Some(true))
            }

            CtrlMsg::Pause => {
                self.handle_ctrl_pause(paused)?;
                Ok(None)
            }

            CtrlMsg::Resume => {
                self.handle_ctrl_resume(paused)?;
                Ok(None)
            }

            CtrlMsg::TakeSnapshot => {
                self.handle_ctrl_take_snapshot().await?;
                Ok(None)
            }

            _ => Ok(None), // ignore all other types
        }
    }
}
