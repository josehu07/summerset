//! Summerset server lease management module implementation.

use std::collections::{HashMap, HashSet};

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError, Timer};

use get_size::GetSize;

use serde::{Deserialize, Serialize};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;

/// Monotonically non-decreasing lease number type.
pub(crate) type LeaseNum = u64;

/// Lease-related peer-to-peer messages, used as a sub-category of the message
/// type of TransportHub.
// TODO: merge promise refreshes with heartbeats after turning heartbeats into
//       a generic server module
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, GetSize)]
pub(crate) enum LeaseMsg {
    /// Push-based Guard phase.
    Guard,
    /// Reply to Guard.
    GuardReply,

    /// Promise or refresh the lease for fixed amount of time.
    Promise,
    /// Reply to Promise. `held` is true if lease was guarded/held and so will
    /// be successfully held for the next T_lease window; is false otherwise.
    PromiseReply { held: bool },

    /// Revoke lease if held.
    Revoke,
    /// Reply to Revoke. `held` is true if lease promise was held; as long as
    /// RevokeReply is received, revocation is guaranteed to have taken effect
    /// whether or not this field is true.
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

    /// To mark next heartbeat to peer as a promise refresh, usually just used
    /// by `get_action()` internally.
    NextRefresh { peer: ReplicaId },

    /// To indicate a peer being removed from promises_sent by a successful
    /// active RevokeReply (or any other reason).
    GrantRemoved { peer: ReplicaId, held: bool },

    /// In case the protocol logic needs it:
    /// Timed-out as a grantor (either in guard phase or in repeated promise).
    GrantTimeout { peer: ReplicaId },

    /// In case the protocol logic needs it:
    /// Timed-out as a lease holder (promise not received/refreshed in time).
    LeaseTimeout { peer: ReplicaId },

    /// In case the protocol logic needs it:
    /// Higher lease number received from a peer.
    HigherNumber,
}

/// Wrapper type for lease-related notifications to trigger something to happen
/// in this module.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum LeaseNotice {
    /// Want to grant a new set of leases with given lease number to replicas,
    /// possibly invalidating everything if the current lease number is older.
    /// If `peers` is `None`, then granting to all peers.
    NewGrants { peers: Option<Bitmap> },

    /// Want to actively revoke leases made to peers. If `peers` is `None`,
    /// then granting to all peers.
    DoRevoke { peers: Option<Bitmap> },

    /// Received a lease-related message from a peer.
    RecvLeaseMsg { peer: ReplicaId, msg: LeaseMsg },

    /// By internal timers ONLY:
    /// Timed-out as a grantor (either in guard phase or in repeated promise).
    GrantTimeout { peer: ReplicaId },

    /// By internal timers ONLY:
    /// Timed-out as a lease holder (promise not received/refreshed in time).
    LeaseTimeout { peer: ReplicaId },
}

/// The time-based lease manager module.
///
/// Current implementation uses the push-based guard + promise approach with no
/// clock synchronization but with the assumption of bounded-error elapsed time
/// across nodes. Reference in:
///   https://www.cs.cmu.edu/~imoraru/papers/qrl.pdf
/// except minor simplifications (e.g., no seq_ACKs) thanks to TCP semantics.
///
/// It delegates lease refreshing to the existing heartbeats mechanism of the
/// protocol. It takes care of other things, e.g., expiration, internally.
pub(crate) struct LeaseManager {
    /// My replica ID.
    me: ReplicaId,

    /// Total number of replicas in the cluster.
    population: u8,

    /// Expiration timeout used as both T_guard and T_lease.
    expire_timeout: Duration,

    /// Receiver side of the action channel.
    rx_action: mpsc::UnboundedReceiver<(LeaseNum, LeaseAction)>,

    /// Sender side of the notice channel.
    tx_notice: mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>,

    /// Map from peer ID I've grant lease to on current lease number ->
    /// (T_guard timer, revocation intention flag); shared with the lease
    /// manager task.
    promises_sent: flashmap::ReadHandle<ReplicaId, (Timer, bool)>,

    /// Map from peer ID I've been granted lease from (i.e., held) on current
    /// lease number -> T_lease timer; shared with the lease manager task.
    promises_held: flashmap::ReadHandle<ReplicaId, Timer>,

    /// Set of peer IDs to which the next heartbeat should be a promise refresh.
    refresh_mark: HashSet<ReplicaId>,

    /// Join handle of the lease manager task.
    _lease_manager_handle: JoinHandle<()>,
}

// LeaseManager public API implementation
impl LeaseManager {
    /// Creates a new lease manager. Returns a tuple of the created LeaseManager
    /// and a clone of the notice channel sender to be used by the TransportHub
    /// to feed lease messages directly in.
    pub(crate) fn new_and_setup(
        me: ReplicaId,
        population: u8,
        expire_timeout: Duration, // serves both T_guard and T_lease
        hb_send_interval: Duration,
    ) -> Result<
        (Self, mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>),
        SummersetError,
    > {
        if expire_timeout < Duration::from_millis(100)
            || expire_timeout > Duration::from_secs(10)
        {
            return logged_err!(
                "invalid lease expire_timeout {:?}",
                expire_timeout
            );
        }
        if 2 * hb_send_interval >= expire_timeout {
            // T_lease must be > 2 * heartbeat interval
            return logged_err!(
                "heartbeat interval {:?} too short given lease timeout {:?}",
                hb_send_interval,
                expire_timeout
            );
        }

        let (tx_notice, rx_notice) = mpsc::unbounded_channel();
        let (tx_action, rx_action) = mpsc::unbounded_channel();

        let (promises_sent_write, promises_sent_read) =
            flashmap::new::<ReplicaId, (Timer, bool)>();
        let (promises_held_write, promises_held_read) =
            flashmap::new::<ReplicaId, Timer>();

        let mut manager = LeaseManagerLogicTask::new(
            me,
            population,
            expire_timeout,
            expire_timeout,
            tx_action,
            tx_notice.clone(),
            rx_notice,
            promises_sent_write,
            promises_held_write,
        );
        let lease_manager_handle =
            tokio::spawn(async move { manager.run().await });

        let tx_notice_clone = tx_notice.clone();
        Ok((
            LeaseManager {
                me,
                population,
                expire_timeout,
                rx_action,
                tx_notice,
                promises_sent: promises_sent_read,
                promises_held: promises_held_read,
                refresh_mark: HashSet::new(),
                _lease_manager_handle: lease_manager_handle,
            },
            tx_notice_clone,
        ))
    }

    /// Gets the set of replicas I've currently granted promise to.
    #[allow(dead_code)]
    pub(crate) fn grant_set(&self) -> Bitmap {
        let mut map = Bitmap::new(self.population, false);
        for &r in self.promises_sent.guard().keys() {
            map.set(r, true).unwrap();
        }
        map
    }

    /// Gets the number of promises currently held (self not included).
    pub(crate) fn lease_cnt(&self) -> u8 {
        self.promises_held.guard().len() as u8
    }

    /// Adds a lease notice to be taken care of by the lease manager by sending
    /// to the notice channel.
    pub(crate) fn add_notice(
        &mut self,
        lease_num: LeaseNum,
        notice: LeaseNotice,
    ) -> Result<(), SummersetError> {
        self.tx_notice
            .send((lease_num, notice))
            .map_err(SummersetError::msg)
    }

    /// Waits for a lease action to be treated by the protocol implementation,
    /// probably leading to sending some messages.
    pub(crate) async fn get_action(
        &mut self,
    ) -> Result<(LeaseNum, LeaseAction), SummersetError> {
        match self.rx_action.recv().await {
            Some((lease_num, action)) => {
                if let LeaseAction::NextRefresh { peer } = action {
                    // bookkeep refresh_mark set internally
                    self.refresh_mark.insert(peer);
                }
                Ok((lease_num, action))
            }
            None => logged_err!("action channel has been closed"),
        }
    }

    /// Checks if the next heartbeat to peers should be marked as a promise
    /// refresh. If `peers` is `None`, checks all peers. For a peer, if to
    /// refresh, extends the promise_sent timer for them by T_lease and unmarks
    /// refresh. Returns the set of peers that should send refresh to.
    ///
    /// This synchronous version method is called right before every heartbeat
    /// attempt by the protocol module.
    pub(crate) fn attempt_refresh(
        &mut self,
        peers: Option<&Bitmap>,
    ) -> Result<Bitmap, SummersetError> {
        let mut to_refresh = Bitmap::new(self.population, false);
        let promises_sent = self.promises_sent.guard();
        for &peer in promises_sent.keys() {
            if peer != self.me
                && peers
                    .map(|bm| bm.get(peer).unwrap_or(false))
                    .unwrap_or(false)
                // bookkeep refresh_mark set internally
                && self.refresh_mark.remove(&peer)
            {
                if let Some((timer, revoking)) = promises_sent.get(&peer) {
                    if !revoking {
                        timer.extend(self.expire_timeout)?;
                        to_refresh.set(peer, true)?;
                    }
                }
            }
        }
        Ok(to_refresh)
    }
}

/// LeaseManager manager logic task.
struct LeaseManagerLogicTask {
    me: ReplicaId,
    population: u8,

    active_num: LeaseNum,

    guard_timeout: Duration,
    lease_timeout: Duration,

    tx_action: mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,

    /// Will be cloned into every timer task for its timeout trigger.
    tx_notice: mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>,
    rx_notice: mpsc::UnboundedReceiver<(LeaseNum, LeaseNotice)>,

    guards_sent: HashMap<ReplicaId, Timer>,
    guards_held: HashMap<ReplicaId, Timer>,

    promises_sent: flashmap::WriteHandle<ReplicaId, (Timer, bool)>,
    promises_held: flashmap::WriteHandle<ReplicaId, Timer>,
}

impl LeaseManagerLogicTask {
    /// Creates the lease manager logic task.
    #[allow(clippy::too_many_arguments)]
    fn new(
        me: ReplicaId,
        population: u8,
        guard_timeout: Duration,
        lease_timeout: Duration,
        tx_action: mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        tx_notice: mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>,
        rx_notice: mpsc::UnboundedReceiver<(LeaseNum, LeaseNotice)>,
        promises_sent: flashmap::WriteHandle<ReplicaId, (Timer, bool)>,
        promises_held: flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Self {
        // the active lease number must be monotonically non-decreasing; old
        // lease numbers mean actions/notices for old leasing periods and are
        // simply ignored
        let active_num: LeaseNum = 0;

        // need to internally maintain a set of in-progress guards sent/held.
        // On either side, a peer must not appear in both guards and promises
        // at the same time
        let guards_sent: HashMap<ReplicaId, Timer> = HashMap::new();
        let guards_held: HashMap<ReplicaId, Timer> = HashMap::new();

        LeaseManagerLogicTask {
            me,
            population,
            active_num,
            guard_timeout,
            lease_timeout,
            tx_action,
            tx_notice,
            rx_notice,
            guards_sent,
            guards_held,
            promises_sent,
            promises_held,
        }
    }

    /// Attempts new grants to peers.
    fn handle_new_grants(
        &mut self,
        lease_num: LeaseNum,
        peers: Option<Bitmap>,
    ) -> Result<(), SummersetError> {
        let peers = peers.unwrap_or(Bitmap::new(self.population, true));
        let mut bcast_peers = peers.clone();

        let promises_sent = self.promises_sent.guard();
        for (peer, flag) in peers.iter() {
            if peer == self.me || !flag {
                bcast_peers.set(peer, false)?;
                continue;
            }

            if promises_sent.contains_key(&peer) {
                // already granting promise to this peer with this number, skip
                bcast_peers.set(peer, false)?;
                continue;
            }

            // enter guard phase and create grant-side timer
            let tx_notice_ref = self.tx_notice.clone();
            let timer = Timer::new(
                false,
                Some(move || {
                    tx_notice_ref
                        .send((lease_num, LeaseNotice::GrantTimeout { peer }))
                        .expect("sending to tx_notice_ref should succeed");
                }),
                true,
            );
            self.guards_sent.insert(peer, timer);
        }

        // broadcast Guard messages to these peers
        pf_info!("lease bcast Guard @ {} -> {:?}", lease_num, bcast_peers);
        self.tx_action.send((
            lease_num,
            LeaseAction::BcastLeaseMsgs {
                peers: bcast_peers,
                msg: LeaseMsg::Guard,
            },
        ))?;
        Ok(())
    }

    /// Revokes leases given to peers.
    fn handle_do_revoke(
        &mut self,
        lease_num: LeaseNum,
        peers: Option<Bitmap>,
    ) -> Result<(), SummersetError> {
        let peers = peers.unwrap_or(Bitmap::new(self.population, true));
        let mut bcast_peers = peers.clone();

        for (peer, flag) in peers.iter() {
            if peer == self.me || !flag {
                bcast_peers.set(peer, false)?;
                continue;
            }

            // remove existing guard about this peer if exists
            self.guards_sent.remove(&peer);
        }

        // broadcast Revoke messages to these peers
        pf_info!("leases bcast Revoke @ {} -> {:?}", lease_num, bcast_peers);
        self.tx_action.send((
            lease_num,
            LeaseAction::BcastLeaseMsgs {
                peers: bcast_peers,
                msg: LeaseMsg::Revoke,
            },
        ))?;
        Ok(())
    }

    /// Received a Guard message.
    fn handle_msg_guard(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_trace!("lease recv Guard @ {} <- {}", lease_num, peer);

        if self.promises_held.guard().contains_key(&peer) {
            // already held promise from this peer with this number, ignore
            return Ok(());
        }

        // create recv-side timer and kickoff for T_guard, and update guards_held
        let tx_notice_ref = self.tx_notice.clone();
        let timer = Timer::new(
            false,
            Some(move || {
                tx_notice_ref
                    .send((lease_num, LeaseNotice::LeaseTimeout { peer }))
                    .expect("sending to tx_notice_ref should succeed");
            }),
            true,
        );
        timer.kickoff(self.guard_timeout)?;
        self.guards_held.insert(peer, timer);

        // send GuardReply back
        pf_trace!("lease send GuardReply @ {} -> {}", lease_num, peer);
        self.tx_action.send((
            lease_num,
            LeaseAction::SendLeaseMsg {
                peer,
                msg: LeaseMsg::GuardReply,
            },
        ))?;
        Ok(())
    }

    /// Received a GuardReply message.
    fn handle_msg_guard_reply(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_trace!("lease recv GuardReply @ {} <- {}", lease_num, peer);

        if !self.guards_sent.contains_key(&peer) {
            // not a GuardReply I'm expecting, ignore
            return Ok(());
        }

        let timer = self.guards_sent.remove(&peer).unwrap();
        if timer.exploded() {
            // timer has already timed out, it's just that the timeout notice
            // has not been processed by the manager yet; abort
            return Ok(());
        }

        // move peer from guards_sent to promises_sent and kickoff for
        // T_guard + T_promise
        timer.kickoff(self.guard_timeout + self.lease_timeout)?;
        self.promises_sent.guard().insert(peer, (timer, false));

        // send Promise message to this peer
        pf_trace!("lease send Promise @ {} -> {}", lease_num, peer);
        self.tx_action.send((
            lease_num,
            LeaseAction::SendLeaseMsg {
                peer,
                msg: LeaseMsg::Promise,
            },
        ))?;
        Ok(())
    }

    /// Received a Promise message.
    fn handle_msg_promise(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_trace!("lease recv Promise @ {} <- {}", lease_num, peer);

        let mut promises_held = self.promises_held.guard();
        if self.guards_held.contains_key(&peer) {
            // was in guard phase, this is the first promise
            let timer = self.guards_held.remove(&peer).unwrap();
            timer.kickoff(self.lease_timeout)?;
            promises_held.insert(peer, timer);

            // send PromiseReply back
            pf_trace!("lease send PromiseReply(T) @ {} -> {}", lease_num, peer);
            self.tx_action.send((
                lease_num,
                LeaseAction::SendLeaseMsg {
                    peer,
                    msg: LeaseMsg::PromiseReply { held: true },
                },
            ))?;
        } else if promises_held.contains_key(&peer) {
            // was in promise phase, this is a refresh
            promises_held
                .get(&peer)
                .unwrap()
                .kickoff(self.lease_timeout)?;

            // send PromiseReply back
            pf_trace!("lease send PromiseReply(T) @ {} -> {}", lease_num, peer);
            self.tx_action.send((
                lease_num,
                LeaseAction::SendLeaseMsg {
                    peer,
                    msg: LeaseMsg::PromiseReply { held: true },
                },
            ))?;
        } else {
            // was in neither, so not a Promise I'm expecting; reply with
            // `held` false
            pf_trace!("lease send PromiseReply(F) @ {} -> {}", lease_num, peer);
            self.tx_action.send((
                lease_num,
                LeaseAction::SendLeaseMsg {
                    peer,
                    msg: LeaseMsg::PromiseReply { held: false },
                },
            ))?;
        }
        Ok(())
    }

    /// Received a PromiseReply message.
    fn handle_msg_promise_reply(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
        held: bool,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "lease recv PromiseReply({}) @ {} <- {}",
            if held { "T" } else { "F" },
            lease_num,
            peer
        );

        let mut promises_sent = self.promises_sent.guard();
        if !promises_sent.contains_key(&peer) {
            // not a PromiseReply I'm expecting, ignore
            return Ok(());
        }
        if !held {
            // failed promise, proactively remove peer from guards_sent or
            // promises_sent for efficiency
            self.guards_sent.remove(&peer);
            promises_sent.remove(peer);
            self.tx_action
                .send((lease_num, LeaseAction::GrantRemoved { peer, held }))?;
            return Ok(());
        }

        let (timer, revoking) = promises_sent.get(&peer).unwrap();
        if timer.exploded() {
            // timer has already timed out, it's just that the timeout notice
            // has not been processed by the manager yet; abort
            return Ok(());
        }

        if !revoking {
            // refresh timer for T_lease
            timer.kickoff(self.lease_timeout)?;

            // let protocol module mark next heartbeat as a promise refresh
            pf_trace!("lease mark NextRefresh @ {} -> {}", lease_num, peer);
            self.tx_action
                .send((lease_num, LeaseAction::NextRefresh { peer }))?
        }
        Ok(())
    }

    /// Received a Revoke message.
    fn handle_msg_revoke(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_trace!("lease recv Revoke @ {} <- {}", lease_num, peer);

        // remove existing guard or promise held from this peer
        self.guards_held.remove(&peer);
        let held = self.promises_held.guard().remove(peer).is_some();

        // send RevokeReply back
        pf_trace!(
            "lease send RevokeReply({}) @ {} -> {}",
            if held { "T" } else { "F" },
            lease_num,
            peer
        );
        self.tx_action.send((
            lease_num,
            LeaseAction::SendLeaseMsg {
                peer,
                msg: LeaseMsg::RevokeReply { held },
            },
        ))?;
        Ok(())
    }

    /// Received a RevokeReply message.
    fn handle_msg_revoke_reply(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
        held: bool,
    ) -> Result<(), SummersetError> {
        pf_trace!(
            "lease recv RevokeReply({}) @ {} <- {}",
            if held { "T" } else { "F" },
            lease_num,
            peer
        );

        // remove existing guard or promise sent to this peer
        self.guards_sent.remove(&peer);
        self.promises_sent.guard().remove(peer);

        // let protocol module know about successful revocation
        self.tx_action
            .send((lease_num, LeaseAction::GrantRemoved { peer, held }))?;
        Ok(())
    }

    /// Timeout reached granting lease to a peer.
    fn handle_grant_timeout(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_debug!("lease grant timeout @ {} -> {}", lease_num, peer);

        // remove existing guard or promise sent to this peer
        self.guards_sent.remove(&peer);
        self.promises_sent.guard().remove(peer);

        // tell the protocol module about this timeout
        self.tx_action
            .send((lease_num, LeaseAction::GrantTimeout { peer }))?;
        Ok(())
    }

    /// Timeout reached without promise refresh from a peer.
    fn handle_lease_timeout(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        pf_debug!("leaseholder timeout @ {} <- {}", lease_num, peer);

        // remove existing guard or promise held from this peer
        self.guards_held.remove(&peer);
        self.promises_held.guard().remove(peer);

        // tell the protocol module about this timeout
        self.tx_action
            .send((lease_num, LeaseAction::LeaseTimeout { peer }))?;
        Ok(())
    }

    /// Synthesized handler of leader notices.
    fn handle_notice(
        &mut self,
        lease_num: LeaseNum,
        notice: LeaseNotice,
    ) -> Result<(), SummersetError> {
        match notice {
            LeaseNotice::NewGrants { peers } => {
                self.handle_new_grants(lease_num, peers)
            }
            LeaseNotice::DoRevoke { peers } => {
                self.handle_do_revoke(lease_num, peers)
            }
            LeaseNotice::RecvLeaseMsg { peer, msg } => {
                debug_assert_ne!(peer, self.me);
                match msg {
                    LeaseMsg::Guard => self.handle_msg_guard(lease_num, peer),
                    LeaseMsg::GuardReply => {
                        self.handle_msg_guard_reply(lease_num, peer)
                    }
                    LeaseMsg::Promise => {
                        self.handle_msg_promise(lease_num, peer)
                    }
                    LeaseMsg::PromiseReply { held } => {
                        self.handle_msg_promise_reply(lease_num, peer, held)
                    }
                    LeaseMsg::Revoke => self.handle_msg_revoke(lease_num, peer),
                    LeaseMsg::RevokeReply { held } => {
                        self.handle_msg_revoke_reply(lease_num, peer, held)
                    }
                }
            }
            LeaseNotice::GrantTimeout { peer } => {
                debug_assert_ne!(peer, self.me);
                self.handle_grant_timeout(lease_num, peer)
            }
            LeaseNotice::LeaseTimeout { peer } => {
                debug_assert_ne!(peer, self.me);
                self.handle_lease_timeout(lease_num, peer)
            }
        }
    }

    /// Starts the lease manager logic task loop.
    async fn run(&mut self) {
        pf_debug!("lease_manager task spawned");

        while let Some((lease_num, notice)) = self.rx_notice.recv().await {
            #[allow(clippy::comparison_chain)]
            if lease_num < self.active_num {
                pf_debug!(
                    "ignoring outdated lease_num: {} < {}",
                    lease_num,
                    self.active_num
                );
                continue;
            } else if lease_num > self.active_num {
                // starting the grants of a new lease number, invalidate everything
                // with old number
                pf_debug!(
                    "observed higher lease_num: {} > {}",
                    lease_num,
                    self.active_num
                );
                self.guards_sent.clear();
                self.guards_held.clear();
                {
                    let mut promises_sent_guard = self.promises_sent.guard();
                    for peer in (0..self.population).filter(|&p| p != self.me) {
                        promises_sent_guard.remove(peer);
                    }
                }
                {
                    let mut promises_held_guard = self.promises_held.guard();
                    for peer in (0..self.population).filter(|&p| p != self.me) {
                        promises_held_guard.remove(peer);
                    }
                }
                self.active_num = lease_num;

                if let Err(e) =
                    self.tx_action.send((lease_num, LeaseAction::HigherNumber))
                {
                    pf_error!(
                        "error sending HigherNumber action @ {}: {}",
                        lease_num,
                        e
                    );
                }
            }

            // pf_trace!("lease notice {:?}", notice);
            let res = self.handle_notice(lease_num, notice);
            if let Err(e) = res {
                pf_error!("error handling lease notice @ {}: {}", lease_num, e);
            }
        }

        pf_debug!("lease_manager task exitted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::Barrier;
    use tokio::task::JoinSet;
    use tokio::time;

    /// Mock server node (lease module caller) used for testing.
    struct TestNode {
        leaseman: LeaseManager,
        transport: TestTransport,
        _messengers: HashMap<ReplicaId, JoinHandle<()>>,
    }

    impl TestNode {
        /// Creates a cluster of connected test node structs.
        async fn new_cluster(
            population: u8,
            expire_timeout: Duration,
        ) -> Result<Vec<TestNode>, SummersetError> {
            assert!(population >= 2);
            let mut nodes = Vec::with_capacity(population as usize);

            let mut conns = HashMap::with_capacity(
                (population * (population - 1)) as usize,
            );
            for i in 0..(population - 1) {
                for j in (i + 1)..population {
                    let (tx_i_j, rx_i_j) = mpsc::unbounded_channel();
                    let (tx_j_i, rx_j_i) = mpsc::unbounded_channel();
                    conns.insert((i, j), (tx_i_j, rx_j_i));
                    conns.insert((j, i), (tx_j_i, rx_i_j));
                }
            }

            for n in 0..population {
                let (tx_recv, rx_recv) = mpsc::unbounded_channel();
                let mut tx_sends = HashMap::new();
                let mut messengers = HashMap::new();
                for p in (0..population).filter(|&p| p != n) {
                    let (tx_conn, mut rx_conn) = conns.remove(&(n, p)).unwrap();
                    let (tx_send, mut rx_send) = mpsc::unbounded_channel();
                    tx_sends.insert(p, tx_send);
                    let tx_recv_clone = tx_recv.clone();
                    messengers.insert(
                        p,
                        tokio::spawn(async move {loop { tokio::select! {
                            msg = rx_send.recv() => {
                                if msg.is_none() {
                                    break;
                                }
                                let (lease_num, lease_msg) = msg.unwrap();
                                tx_conn.send((lease_num, lease_msg)).unwrap();
                            }
                            msg = rx_conn.recv() => {
                                if msg.is_none() {
                                    break;
                                }
                                let (lease_num, lease_msg) = msg.unwrap();
                                tx_recv_clone.send((p, lease_num, lease_msg)).unwrap();
                            }
                        }}}),
                    );
                }

                let (leaseman, _) = LeaseManager::new_and_setup(
                    n,
                    population,
                    expire_timeout,
                    Duration::ZERO, // dummy; refreshes are hardcoded in tests
                )?;

                nodes.push(TestNode {
                    leaseman,
                    transport: TestTransport {
                        id: n,
                        tx_sends,
                        rx_recv,
                    },
                    _messengers: messengers,
                });
            }

            Ok(nodes)
        }
    }

    /// Mock server transport struct.
    struct TestTransport {
        id: ReplicaId,
        tx_sends:
            HashMap<ReplicaId, mpsc::UnboundedSender<(LeaseNum, LeaseMsg)>>,
        rx_recv: mpsc::UnboundedReceiver<(ReplicaId, LeaseNum, LeaseMsg)>,
    }

    impl TestTransport {
        /// Sends a message to a peer node.
        fn send_msg(
            &mut self,
            peer: ReplicaId,
            msg: (LeaseNum, LeaseMsg),
        ) -> Result<(), SummersetError> {
            self.tx_sends
                .get(&peer)
                .unwrap()
                .send(msg)
                .map_err(SummersetError::msg)
        }

        /// Broadcasts a messages to peers.
        fn bcast_msg(
            &mut self,
            peers: Bitmap,
            msg: (LeaseNum, LeaseMsg),
        ) -> Result<(), SummersetError> {
            for (peer, flag) in peers.iter() {
                if peer != self.id && flag {
                    self.send_msg(peer, msg.clone())?;
                }
            }
            Ok(())
        }

        /// Receives a message from some peer node.
        async fn recv_msg(
            &mut self,
        ) -> Result<(ReplicaId, LeaseNum, LeaseMsg), SummersetError> {
            self.rx_recv
                .recv()
                .await
                .ok_or(SummersetError::msg("msg channel closed"))
        }

        /// Tries to receive a message from some peer node, returning `None`
        /// immediately if no ready message in channel.
        fn try_recv_msg(
            &mut self,
        ) -> Result<Option<(ReplicaId, LeaseNum, LeaseMsg)>, SummersetError>
        {
            match self.rx_recv.try_recv() {
                Ok(msg) => Ok(Some(msg)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(err) => Err(err.into()),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guard_expired() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(600)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.transport.recv_msg().await?, (0, 7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            // deliberately not sending GuardReply
            time::sleep(Duration::from_millis(60)).await;
            assert_eq!(n1.transport.try_recv_msg(), Ok(None));
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            // still so after T_guard expires
            time::sleep(Duration::from_millis(660)).await;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::LeaseTimeout { peer: 0 })
            );
            assert_eq!(n1.transport.try_recv_msg(), Ok(None));
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            barrier1.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            7,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Guard))?;
        // no GuardReply received so no promise to make
        time::sleep(Duration::from_millis(30)).await;
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_expired() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(600)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.transport.recv_msg().await?, (0, 7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            // send GuardReply and wait for Promise
            n1.transport.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // deliberately not sending PromiseReply
            time::sleep(Duration::from_millis(60)).await;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // since no refreshes coming, after T_lease expires the held
            // promise should've expired
            time::sleep(Duration::from_millis(660)).await;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::LeaseTimeout { peer: 0 })
            );
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            barrier1.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            7,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::GuardReply)
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::GuardReply,
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::SendLeaseMsg {
                    peer: 1,
                    msg: LeaseMsg::Promise
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        // no PromiseReply received so has to wait for T_guard + T_lease
        time::sleep(Duration::from_millis(30)).await;
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // still so after T_guard expires
        time::sleep(Duration::from_millis(660)).await;
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // no longer considered granted after T_guard + T_lease expires
        time::sleep(Duration::from_millis(660)).await;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::GrantTimeout { peer: 1 })
        );
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_refresh() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(600)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.transport.recv_msg().await?, (0, 7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            // send GuardReply and wait for Promise
            n1.transport.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // send PromiseReply and wait for the next refresh Promise
            n1.transport
                .send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // send PromiseReply and wait for the next refresh Promise again
            n1.transport
                .send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // deliberately not sending PromiseReply
            time::sleep(Duration::from_millis(60)).await;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // since no refreshes coming, after T_lease expires the held
            // promise should've expired
            time::sleep(Duration::from_millis(660)).await;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::LeaseTimeout { peer: 0 })
            );
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            barrier1.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            7,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Guard))?;
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        // wait for GuardReply and send Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::GuardReply)
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::GuardReply,
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::SendLeaseMsg {
                    peer: 1,
                    msg: LeaseMsg::Promise
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::PromiseReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::PromiseReply { held: true },
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::NextRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(120)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        assert_eq!(n0.leaseman.attempt_refresh(Some(&peers))?, peers);
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        // wait for PromiseReply and schedule next refresh Promise again
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::PromiseReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::PromiseReply { held: true },
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::NextRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(120)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        assert_eq!(n0.leaseman.attempt_refresh(Some(&peers))?, peers);
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        // no PromiseReply received so consider granted for 2 * T_lease since
        // last promise
        time::sleep(Duration::from_millis(30)).await;
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // still so after T_lease expires
        time::sleep(Duration::from_millis(660)).await;
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // no longer considered granted after 2 * T_lease has expired after
        // last promise
        time::sleep(Duration::from_millis(660)).await;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::GrantTimeout { peer: 1 })
        );
        assert_eq!(n0.transport.try_recv_msg(), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revoke_replied() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(600)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.transport.recv_msg().await?, (0, 7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            // send GuardReply and wait for Promise
            n1.transport.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // send PromiseReply
            n1.transport
                .send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // grantor decided to revoke the lease
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Revoke)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Revoke,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::RevokeReply { held: true }
                    }
                )
            );
            // send RevokeReply
            n1.transport
                .send_msg(0, (7, LeaseMsg::RevokeReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            barrier1.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            7,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::GuardReply)
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::GuardReply,
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::SendLeaseMsg {
                    peer: 1,
                    msg: LeaseMsg::Promise
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::PromiseReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::PromiseReply { held: true },
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::NextRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(120)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // I decide to revoke the granted lease
        n0.leaseman.add_notice(
            7,
            LeaseNotice::DoRevoke {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Revoke
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Revoke))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for RevokeReply
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::RevokeReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::RevokeReply { held: true },
            },
        )?;
        time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::GrantRemoved {
                    peer: 1,
                    held: true
                }
            )
        );
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revoke_expired() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(600)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.transport.recv_msg().await?, (0, 7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            // send GuardReply and wait for Promise
            n1.transport.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // send PromiseReply
            n1.transport
                .send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // grantor decided to revoke the lease
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Revoke)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Revoke,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::RevokeReply { held: true }
                    }
                )
            );
            // intentionally not sending RevokeReply
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            barrier1.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            7,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::GuardReply)
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::GuardReply,
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::SendLeaseMsg {
                    peer: 1,
                    msg: LeaseMsg::Promise
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::PromiseReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::PromiseReply { held: true },
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::NextRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(120)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // I decide to revoke the granted lease
        n0.leaseman.add_notice(
            7,
            LeaseNotice::DoRevoke {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Revoke
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Revoke))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // not receiving RevokeReply, so has to wait for grant timeout
        assert_eq!(n0.leaseman.grant_set(), peers);
        time::sleep(Duration::from_millis(660)).await;
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        // also check that no further refreshes scheduled
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::GrantTimeout { peer: 1 })
        );
        assert_eq!(
            n0.leaseman.attempt_refresh(Some(&peers))?,
            Bitmap::new(2, false)
        );
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn regrant_higher() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(600)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.transport.recv_msg().await?, (0, 7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            // send GuardReply and wait for Promise
            n1.transport.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 7, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    7,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // send PromiseReply
            n1.transport
                .send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // grantor decided to move to a new lease number
            assert_eq!(n1.transport.recv_msg().await?, (0, 8, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                8,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (8, LeaseAction::HigherNumber)
            );
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    8,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            // send GuardReply and wait for Promise
            n1.transport.send_msg(0, (8, LeaseMsg::GuardReply))?;
            assert_eq!(
                n1.transport.recv_msg().await?,
                (0, 8, LeaseMsg::Promise)
            );
            n1.leaseman.add_notice(
                8,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Promise,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    8,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::PromiseReply { held: true }
                    }
                )
            );
            // send PromiseReply
            n1.transport
                .send_msg(0, (8, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            barrier1.wait().await;
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            7,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::GuardReply)
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::GuardReply,
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::SendLeaseMsg {
                    peer: 1,
                    msg: LeaseMsg::Promise
                }
            )
        );
        n0.transport.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 7, LeaseMsg::PromiseReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::PromiseReply { held: true },
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::NextRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(120)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // I decide to move to a new lease number
        n0.leaseman.add_notice(
            8,
            LeaseNotice::NewGrants {
                peers: Some(peers.clone()),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (8, LeaseAction::HigherNumber)
        );
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                8,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        n0.transport.send_msg(1, (8, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(
            n0.transport.recv_msg().await?,
            (1, 8, LeaseMsg::GuardReply)
        );
        n0.leaseman.add_notice(
            8,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::GuardReply,
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                8,
                LeaseAction::SendLeaseMsg {
                    peer: 1,
                    msg: LeaseMsg::Promise
                }
            )
        );
        n0.transport.send_msg(1, (8, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn mutual_leases() -> Result<(), SummersetError> {
        let population = 5;
        let nodes =
            TestNode::new_cluster(population, Duration::from_millis(1200))
                .await?;
        let mut joins = JoinSet::new();
        let barrier = Arc::new(Barrier::new(population as usize));
        let abitmap = Arc::new(AtomicU8::new(0));
        for (i, mut n) in nodes.into_iter().enumerate() {
            let barrier_ref = barrier.clone();
            let abitmap_ref = abitmap.clone();
            let mask_i: u8 = 1 << i;
            // spawn node i's mimic event loop
            joins.spawn(async move {
                n.leaseman
                    .add_notice(7, LeaseNotice::NewGrants { peers: None })?;
                loop {
                    tokio::select! {
                        lease_action = n.leaseman.get_action() => {
                            let (lease_num, lease_action) = lease_action.unwrap();
                            match lease_action {
                                LeaseAction::SendLeaseMsg { peer, msg } => {
                                    n.transport.send_msg(
                                        peer,
                                        (lease_num, msg)
                                    ).unwrap();
                                },
                                LeaseAction::BcastLeaseMsgs { peers, msg } => {
                                    n.transport.bcast_msg(
                                        peers,
                                        (lease_num, msg)
                                    ).unwrap();
                                },
                                LeaseAction::HigherNumber => {
                                    if lease_num > 7 {
                                        return Err(SummersetError::msg(format!(
                                            "unexpected lease number {} seen",
                                            lease_num
                                        )));
                                    }
                                },
                                LeaseAction::GrantRemoved { .. }
                                | LeaseAction::GrantTimeout { .. }
                                | LeaseAction::LeaseTimeout { .. } => {
                                    return Err(SummersetError::msg(format!(
                                        "removal or timeout happened on {}",
                                        i
                                    )));
                                },
                                LeaseAction::NextRefresh { .. } => {},
                            }
                        }
                        msg = n.transport.recv_msg() => {
                            let (peer, lease_num, msg) = msg.unwrap();
                            if lease_num > 0 {
                                n.leaseman.add_notice(
                                    lease_num,
                                    LeaseNotice::RecvLeaseMsg { peer, msg }
                                ).unwrap();
                            }
                        }
                    }
                    // update global counter of how many nodes have all peers'
                    // leases held, and broadcast a dummy message to allow other
                    // peers a chance to check abitmap
                    if n.leaseman.lease_cnt() + 1 == population {
                        let old = abitmap_ref.fetch_or(mask_i, Ordering::AcqRel);
                        if old & mask_i == 0 {
                            n.transport.bcast_msg(
                                Bitmap::new(population, true),
                                (0, LeaseMsg::Guard)
                            ).unwrap();
                        }
                    }
                    // all node should have held leases from all peers at some
                    // point before any timeout happens
                    let abitmap = abitmap_ref.load(Ordering::Acquire);
                    if (abitmap.count_ones() as u8) == population {
                        break;
                    }
                };
                barrier_ref.wait().await;
                Ok::<(), SummersetError>(())
            });
        }
        while let Some(join_res) = joins.join_next().await {
            join_res??;
        }
        Ok(())
    }
}
