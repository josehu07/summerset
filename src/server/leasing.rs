//! Summerset server lease manager module implementation.

use std::collections::HashMap;

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError, Timer};

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;

/// Monotonically non-decreasing lease number type.
pub(crate) type LeaseNumber = u64;

/// Lease-related peer-to-peer messages, used as a sub-category of the message
/// type of TransportHub.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum LeaseMsg {
    /// Push-based Guard phase.
    Guard,
    /// Reply to Guard.
    GuardReply,

    /// Promise or refresh the lease for fixed amount of time.
    Promise,
    /// Reply to Promise.
    PromiseReply { guarded: bool },

    /// Revoke lease if held.
    Revoke,
    /// Reply to Revoke.
    RevokeReply { held: bool },
}

/// Wrapper type for active lease operations the module wants to conduct, e.g.,
/// sending some lease-related messages.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum LeaseAction {
    /// To send a lease-related message to a peer.
    SendLeaseMsg { peer: ReplicaId, msg: LeaseMsg },

    /// To broadcast lease-related messages to peers.
    BcastLeaseMsgs { peers: Bitmap, msg: LeaseMsg },

    /// Timed-out as a grantor (either in guard phase or repeated promises).
    GrantTimeout { peer: ReplicaId },

    /// Timed-out as a lease holder (i.e., not refreshed in time).
    LeaseTimeout { peer: ReplicaId },
}

/// Wrapper type for lease-related notifications that aim to trigger something
/// to happen in this module.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum LeaseNotice {
    /// Want to grant a new set of leases with given lease number to replicas,
    /// possibly invalidating everything if the current lease number is older.
    NewGrants { peers: Bitmap },

    /// Actively revoke leases made to peers.
    DoRevoke { peers: Bitmap },

    /// Received a lease-related message from a peer.
    RecvLeaseMsg { peer: ReplicaId, msg: LeaseMsg },
}

/// The time-based lease manager module.
///
/// Current implementation uses the push-based guard + promise approach with no
/// clock synchronization but with the assumption of bounded-error elapsed time
/// across nodes. Reference in:
///   https://www.cs.cmu.edu/~imoraru/papers/qrl.pdf
///
/// It delegates lease refreshing to the existing heartbeats mechanism of the
/// protocol. It takes care of other things, e.g., expiration, internally.
pub(crate) struct LeaseManager {
    /// My replica ID.
    _me: ReplicaId,

    /// Receiver side of the action channel.
    rx_action: mpsc::UnboundedReceiver<(LeaseNumber, LeaseAction)>,

    /// Sender side of the notice channel.
    tx_notice: mpsc::UnboundedSender<(LeaseNumber, LeaseNotice)>,

    /// Map from replica ID I've grant lease to on current lease number -> T_guard
    /// timer; shared with the lease manager thread.
    grants: flashmap::ReadHandle<ReplicaId, Timer>,

    /// Map from replica ID I've been granted lease from (i.e., held) on current
    /// lease number -> T_lease timer; shared with the lease manager thread.
    givens: flashmap::ReadHandle<ReplicaId, Timer>,

    /// Join handle of the lease manager thread.
    _lease_manager_handle: JoinHandle<()>,
}

// LeaseManager public API implementation
impl LeaseManager {
    /// Creates a new lease manager.
    pub(crate) fn new_and_setup(
        me: ReplicaId,
        guard_timeout: Duration,
        lease_timeout: Duration,
        hb_send_interval: Duration,
    ) -> Result<Self, SummersetError> {
        if guard_timeout < Duration::from_millis(10)
            || guard_timeout > Duration::from_secs(10)
        {
            return logged_err!("invalid guard timeout {:?}", guard_timeout);
        }
        if lease_timeout < Duration::from_millis(10)
            || lease_timeout > Duration::from_secs(10)
        {
            return logged_err!("invalid lease timeout {:?}", lease_timeout);
        }
        if 2 * hb_send_interval > lease_timeout {
            return logged_err!(
                "heartbeat interval {:?} too short given lease timeout {:?}",
                hb_send_interval,
                lease_timeout
            );
        }

        let (tx_notice, rx_notice) = mpsc::unbounded_channel();
        let (tx_action, rx_action) = mpsc::unbounded_channel();

        let (grants_write, grants_read) = flashmap::new::<ReplicaId, Timer>();
        let (givens_write, givens_read) = flashmap::new::<ReplicaId, Timer>();

        let lease_manager_handle = tokio::spawn(Self::lease_manager_thread(
            me,
            guard_timeout,
            lease_timeout,
            tx_action,
            tx_notice.clone(),
            rx_notice,
            grants_write,
            givens_write,
        ));

        Ok(LeaseManager {
            _me: me,
            rx_action,
            tx_notice,
            grants: grants_read,
            givens: givens_read,
            _lease_manager_handle: lease_manager_handle,
        })
    }

    /// Gets the set of replicas I've currently granted lease to.
    #[allow(dead_code)]
    pub(crate) fn grants_set(&self, population: u8) -> Bitmap {
        let mut map = Bitmap::new(population, false);
        for &r in self.grants.guard().keys() {
            map.set(r, true).unwrap();
        }
        map
    }

    /// Gets the number of leases I'm currently granted.
    pub(crate) fn givens_cnt(&self) -> u8 {
        self.givens.guard().len() as u8
    }

    /// Waits for a lease action to be treated by the protocol implementation,
    /// probably leading to sending some messages.
    pub(crate) async fn get_action(
        &mut self,
    ) -> Result<(LeaseNumber, LeaseAction), SummersetError> {
        match self.rx_action.recv().await {
            Some((lease_num, action)) => Ok((lease_num, action)),
            None => logged_err!("action channel has been closed"),
        }
    }

    /// Adds a lease notice to be taken care of by the lease manager by sending
    /// to the notice channel.
    pub(crate) fn add_notice(
        &mut self,
        lease_num: LeaseNumber,
        notice: LeaseNotice,
    ) -> Result<(), SummersetError> {
        self.tx_notice
            .send((lease_num, notice))
            .map_err(SummersetError::msg)
    }
}

// LeaseManager lease_manager thread implementation
impl LeaseManager {
    /// Attempt new grants to peers.
    fn handle_new_grants(
        lease_num: LeaseNumber,
        peers: Bitmap,
        guard_timeout: Duration,
        tx_action: &mpsc::UnboundedSender<(LeaseNumber, LeaseAction)>,
        tx_notice: &mpsc::UnboundedSender<(LeaseNumber, LeaseNotice)>,
        guards: &mut HashMap<ReplicaId, Timer>,
        grants: &mut flashmap::WriteHandle<ReplicaId, Timer>,
        givens: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        for (peer, flag) in peers.iter() {
            if !flag {
                continue;
            }

            // remove existing states about this peer
            guards.remove(&peer);
            grants.guard().remove(peer);
            givens.guard().remove(peer);

            // start timer of length T_guard
            guards.insert(peer, Timer::new());
        }

        // broadcast Guard messages to these peers
        tx_action.send((
            lease_num,
            LeaseAction::BcastLeaseMsgs {
                peers,
                msg: LeaseMsg::Guard,
            },
        ))?;
        Ok(())
    }

    /// Lease manager thread function.
    async fn lease_manager_thread(
        id: ReplicaId,
        guard_timeout: Duration,
        lease_timeout: Duration,
        tx_action: mpsc::UnboundedSender<(LeaseNumber, LeaseAction)>,
        // will be cloned into every timer thread for its timeout trigger
        tx_notice: mpsc::UnboundedSender<(LeaseNumber, LeaseNotice)>,
        mut rx_notice: mpsc::UnboundedReceiver<(LeaseNumber, LeaseNotice)>,
        grants: flashmap::WriteHandle<ReplicaId, Timer>,
        givens: flashmap::WriteHandle<ReplicaId, Timer>,
    ) {
        pf_debug!("lease_manager thread spawned");

        // the active lease number must be monotonically non-decreasing; old
        // lease numbers mean actions/notices for old leasing periods and are
        // simply ignored
        let mut active_num: LeaseNumber = 0;

        // need to internally maintain a set of in-progress guarded targets
        let mut guards: HashMap<ReplicaId, Timer> = HashMap::new();

        while let Some((lease_num, notice)) = rx_notice.recv().await {
            if lease_num < active_num {
                pf_warn!(
                    "ignoring outdated lease number: {} < {}",
                    lease_num,
                    active_num
                );
                continue;
            } else if lease_num > active_num
                && !matches!(notice, LeaseNotice::NewGrants { .. })
            {
                pf_warn!(
                    "ignoring ahead-of-time lease number: {} > {}",
                    lease_num,
                    active_num
                );
                continue;
            } else if lease_num > active_num {
                // starting the grants of a new lease number, invalidate everything
                // with old number
                guards.clear();
                {
                    let grants_guard = grants.guard();
                    for (&peer, _) in grants_guard.iter() {
                        grants_guard.remove(peer);
                    }
                }
                {
                    let givens_guard = givens.guard();
                    for (&peer, _) in givens_guard.iter() {
                        givens_guard.remove(peer);
                    }
                }
                active_num = lease_num;
            }

            match notice {
                LeaseNotice::NewGrants { peers } => {
                    if let Err(e) = Self::handle_new_grants(
                        lease_num,
                        peers,
                        guard_timeout,
                        &tx_action,
                        &tx_notice,
                        &mut guards,
                        &mut grants,
                        &mut givens,
                    ) {
                        pf_error!("error handling lease NewGrants: {}", e);
                    }
                }
                LeaseNotice::DoRevoke { peers } => {
                    if let Err(e) = Self::handle_do_revoke(peers) {
                        pf_error!("error handling lease DoRevoke: {}", e);
                    }
                }
                LeaseNotice::RecvLeaseMsg { peer, msg } => {
                    if let Err(e) = match msg {
                        LeaseMsg::Guard => Self::handle_guard(peer),
                        LeaseMsg::GuardReply => Self::handle_guard_reply(peer),
                        LeaseMsg::Promise => Self::handle_promise(peer),
                        LeaseMsg::PromiseReply { guarded } => {
                            Self::handle_promise_reply(peer, guarded)
                        }
                        LeaseMsg::Revoke => Self::handle_revoke(peer),
                        LeaseMsg::RevokeReply { held } => {
                            Self::handle_revoke_reply(peer, held)
                        }
                    } {
                        pf_error!("error handling lease message: {}", e);
                    }
                }
            }
        }

        pf_debug!("lease_manager thread exitted");
    }
}

// TODO: tests: grant_refresh, grant_expire, regrant_number, active_revoke
