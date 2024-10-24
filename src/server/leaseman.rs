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
pub(crate) type LeaseNum = u64;

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

    /// To mark next heartbeat as a promise refresh.
    ScheduleRefresh { peer: ReplicaId },

    /// To indicate a successful active RevokeReply.
    RevokeReplied { peer: ReplicaId, held: bool },

    /// In case the protocol logic needs it:
    /// Timed-out as a grantor (either in guard phase or in repeated promise).
    GrantTimeout { peer: ReplicaId },

    /// In case the protocol logic needs it:
    /// Timed-out as a lease holder (promise not received/refreshed in time).
    LeaseTimeout { peer: ReplicaId },

    /// Special: used only by `do_sync_notice()`.
    SyncBarrier,
}

/// Wrapper type for lease-related notifications to trigger something to happen
/// in this module.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum LeaseNotice {
    /// Want to grant a new set of leases with given lease number to replicas,
    /// possibly invalidating everything if the current lease number is older.
    NewGrants { peers: Bitmap },

    /// Wnat to actively revoke leases made to peers.
    DoRevoke { peers: Bitmap },

    /// Received a lease-related message from a peer.
    RecvLeaseMsg { peer: ReplicaId, msg: LeaseMsg },

    /// By internal timers ONLY:
    /// Timed-out as a grantor (either in guard phase or in repeated promise).
    GrantTimeout { peer: ReplicaId },

    /// By internal timers ONLY:
    /// Timed-out as a lease holder (promise not received/refreshed in time).
    LeaseTimeout { peer: ReplicaId },

    /// Special: used only by `do_sync_notice()`.
    SyncBarrier,
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
    _me: ReplicaId,

    /// Total number of replicas in the cluster.
    population: u8,

    /// Expiration timeout used as both T_guard and T_lease.
    expire_timeout: Duration,

    /// Receiver side of the action channel.
    rx_action: mpsc::UnboundedReceiver<(LeaseNum, LeaseAction)>,

    /// Sender side of the notice channel.
    tx_notice: mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>,

    /// Map from replica ID I've grant lease to on current lease number ->
    /// (T_guard timer, revocation intention flag); shared with the lease
    /// manager task.
    promises_sent: flashmap::ReadHandle<ReplicaId, (Timer, bool)>,

    /// Map from replica ID I've been granted lease from (i.e., held) on current
    /// lease number -> T_lease timer; shared with the lease manager task.
    promises_held: flashmap::ReadHandle<ReplicaId, Timer>,

    /// Join handle of the lease manager task.
    _lease_manager_handle: JoinHandle<()>,
}

// LeaseManager public API implementation
impl LeaseManager {
    /// Creates a new lease manager.
    pub(crate) fn new_and_setup(
        me: ReplicaId,
        population: u8,
        expire_timeout: Duration, // serves both T_guard and T_lease
        hb_send_interval: Duration,
    ) -> Result<Self, SummersetError> {
        if expire_timeout < Duration::from_millis(10)
            || expire_timeout > Duration::from_secs(10)
        {
            return logged_err!(
                "invalid lease expire_timeout {:?}",
                expire_timeout
            );
        }
        if 2 * hb_send_interval >= expire_timeout {
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

        Ok(LeaseManager {
            _me: me,
            population,
            expire_timeout,
            rx_action,
            tx_notice,
            promises_sent: promises_sent_read,
            promises_held: promises_held_read,
            _lease_manager_handle: lease_manager_handle,
        })
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
            Some((lease_num, action)) => Ok((lease_num, action)),
            None => logged_err!("action channel has been closed"),
        }
    }

    /// Try to get the next lease action to be treated by the protocol
    /// implementation using `try_recv()`.
    #[allow(dead_code)]
    pub(crate) fn try_get_action(
        &mut self,
    ) -> Result<(LeaseNum, LeaseAction), SummersetError> {
        match self.rx_action.try_recv() {
            Ok((lease_num, action)) => Ok((lease_num, action)),
            Err(e) => Err(SummersetError::msg(e)),
        }
    }

    /// Submits a lease notice and waits for it to be processed blockingly.
    /// Returns a vec of actions triggered by any previously submitted notices
    /// followed by those triggered by this sync notice.
    #[allow(dead_code)]
    pub(crate) async fn do_sync_notice(
        &mut self,
        lease_num: LeaseNum,
        notice: LeaseNotice,
    ) -> Result<Vec<(LeaseNum, LeaseAction)>, SummersetError> {
        self.add_notice(lease_num, notice)?;
        self.add_notice(lease_num, LeaseNotice::SyncBarrier)?;
        let mut actions = vec![];
        loop {
            let (this_num, action) = self.get_action().await?;
            if matches!(action, LeaseAction::SyncBarrier) {
                debug_assert_eq!(this_num, lease_num);
                return Ok(actions);
            }
            actions.push((this_num, action));
        }
    }

    /// Extends the promise_sent timer for a peer by T_lease. This synchronous
    /// version method is useful for protocol modules to avoid the hassle of
    /// go through the channels API.
    pub(crate) fn extend_grant_timer(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        let promises_sent = self.promises_sent.guard();
        if let Some((timer, revoking)) = promises_sent.get(&peer) {
            if !revoking {
                timer.extend(self.expire_timeout)
            } else {
                logged_err!(
                    "refreshing a revoking lease @ {} -> {}",
                    lease_num,
                    peer
                )
            }
        } else {
            logged_err!(
                "refreshing not-found lease @ {} -> {}",
                lease_num,
                peer
            )
        }
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
        peers: Bitmap,
    ) -> Result<(), SummersetError> {
        let mut bcast_peers = peers.clone();
        for (peer, flag) in peers.iter() {
            if peer == self.me || !flag {
                continue;
            }

            if self.promises_sent.guard().contains_key(&peer) {
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
        peers: Bitmap,
    ) -> Result<(), SummersetError> {
        for (peer, flag) in peers.iter() {
            if peer == self.me || !flag {
                continue;
            }

            // remove existing guard about this peer if exists
            self.guards_sent.remove(&peer);
        }

        // broadcast Revoke messages to these peers
        self.tx_action.send((
            lease_num,
            LeaseAction::BcastLeaseMsgs {
                peers,
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
        if self.guards_held.contains_key(&peer) {
            // was in guard phase, this is the first promise
            let timer = self.guards_held.remove(&peer).unwrap();
            timer.kickoff(self.lease_timeout)?;
            self.promises_held.guard().insert(peer, timer);

            // send PromiseReply back
            self.tx_action.send((
                lease_num,
                LeaseAction::SendLeaseMsg {
                    peer,
                    msg: LeaseMsg::PromiseReply { held: true },
                },
            ))?;
        } else if self.promises_held.guard().contains_key(&peer) {
            // was in promise phase, this is a refresh
            self.promises_held
                .guard()
                .get(&peer)
                .unwrap()
                .kickoff(self.lease_timeout)?;

            // send PromiseReply back
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
        if !self.promises_sent.guard().contains_key(&peer) {
            // not a PromiseReply I'm expecting, ignore
            return Ok(());
        }
        if !held {
            // failed promise, proactively remove peer from guards_sent or
            // promises_sent for efficiency
            self.guards_sent.remove(&peer);
            self.promises_sent.guard().remove(peer);
            return Ok(());
        }

        let mut promises_sent = self.promises_sent.guard();
        let (timer, revoking) = promises_sent.get(&peer).unwrap();
        if timer.exploded() {
            // timer has already timed out, it's just that the timeout notice
            // has not been processed by the manager yet; abort
            promises_sent.remove(peer);
            return Ok(());
        }

        if !revoking {
            // refresh timer for T_lease
            timer.kickoff(self.lease_timeout)?;

            // let protocol module mark next heartbeat as a promise refresh
            self.tx_action
                .send((lease_num, LeaseAction::ScheduleRefresh { peer }))?
        }
        Ok(())
    }

    /// Received a Revoke message.
    fn handle_msg_revoke(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        // remove existing guard or promise held from this peer
        self.guards_held.remove(&peer);
        self.promises_held.guard().remove(peer);

        // send RevokeReply back
        self.tx_action.send((
            lease_num,
            LeaseAction::SendLeaseMsg {
                peer,
                msg: LeaseMsg::RevokeReply { held: true },
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
        // remove existing guard or promise sent to this peer
        self.guards_sent.remove(&peer);
        self.promises_sent.guard().remove(peer);

        // let protocol module know about successful revocation
        self.tx_action
            .send((lease_num, LeaseAction::RevokeReplied { peer, held }))?;
        Ok(())
    }

    /// Timeout reached granting lease to a peer.
    fn handle_grant_timeout(
        &mut self,
        lease_num: LeaseNum,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
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
            LeaseNotice::SyncBarrier => Ok(self
                .tx_action
                .send((lease_num, LeaseAction::SyncBarrier))?),
        }
    }

    /// Starts the lease manager logic task loop.
    async fn run(&mut self) {
        pf_debug!("lease_manager task spawned");

        while let Some((lease_num, notice)) = self.rx_notice.recv().await {
            #[allow(clippy::comparison_chain)]
            if lease_num < self.active_num {
                pf_warn!(
                    "ignoring outdated lease_num: {} < {}",
                    lease_num,
                    self.active_num
                );
                continue;
            } else if lease_num > self.active_num {
                // starting the grants of a new lease number, invalidate everything
                // with old number
                // pf_debug!(
                //     "observed higher lease_num: {} > {}",
                //     lease_num,
                //     active_num
                // );
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
    use std::sync::Arc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::Barrier;
    use tokio::time;

    /// Mock server node (lease module caller) used for testing.
    struct TestNode {
        #[allow(clippy::type_complexity)]
        transport: HashMap<
            ReplicaId,
            (
                mpsc::UnboundedSender<(LeaseNum, LeaseMsg)>,
                mpsc::UnboundedReceiver<(LeaseNum, LeaseMsg)>,
            ),
        >,
        leaseman: LeaseManager,
    }

    impl TestNode {
        /// Creates a cluster of connected test node structs.
        async fn new_cluster(
            population: u8,
            expire_timeout: Duration,
        ) -> Result<Vec<TestNode>, SummersetError> {
            assert!(population >= 2);
            let mut nodes = Vec::with_capacity(population as usize);

            let mut channels = HashMap::with_capacity(
                (population * (population - 1)) as usize,
            );
            for i in 0..(population - 1) {
                for j in (i + 1)..population {
                    let (tx_i_j, rx_i_j) = mpsc::unbounded_channel();
                    let (tx_j_i, rx_j_i) = mpsc::unbounded_channel();
                    channels.insert((i, j), (tx_i_j, rx_j_i));
                    channels.insert((j, i), (tx_j_i, rx_i_j));
                }
            }

            for n in 0..population {
                let transport = (0..population)
                    .filter_map(|p| {
                        if p == n {
                            None
                        } else {
                            Some((p, channels.remove(&(n, p)).unwrap()))
                        }
                    })
                    .collect();
                let leaseman = LeaseManager::new_and_setup(
                    n,
                    population,
                    expire_timeout,
                    Duration::ZERO, // dummy; refreshes are hardcoded in tests
                )?;
                nodes.push(TestNode {
                    transport,
                    leaseman,
                });
            }

            Ok(nodes)
        }

        /// Sends a message to a peer node.
        fn send_msg(
            &mut self,
            peer: ReplicaId,
            msg: (LeaseNum, LeaseMsg),
        ) -> Result<(), SummersetError> {
            let (tx_msg, _) = self.transport.get(&peer).unwrap();
            tx_msg.send(msg).map_err(SummersetError::msg)
        }

        /// Receives a message from a peer node.
        async fn recv_msg(
            &mut self,
            peer: ReplicaId,
        ) -> Result<(LeaseNum, LeaseMsg), SummersetError> {
            let (_, rx_msg) = self.transport.get_mut(&peer).unwrap();
            rx_msg
                .recv()
                .await
                .ok_or(SummersetError::msg("msg channel closed"))
        }

        /// Tries to receive a message from a peer node, returning `None`
        /// immediately if no ready message in channel.
        fn try_recv_msg(
            &mut self,
            peer: ReplicaId,
        ) -> Result<Option<(LeaseNum, LeaseMsg)>, SummersetError> {
            let (_, rx_msg) = self.transport.get_mut(&peer).unwrap();
            match rx_msg.try_recv() {
                Ok(msg) => Ok(Some(msg)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(err) => Err(err.into()),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn guard_expired() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(150)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            time::sleep(Duration::from_millis(20)).await;
            assert_eq!(n1.try_recv_msg(0), Ok(None));
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            // still so after T_guard expires
            time::sleep(Duration::from_millis(160)).await;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (7, LeaseAction::LeaseTimeout { peer: 0 })
            );
            assert_eq!(n1.try_recv_msg(0), Ok(None));
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
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (7, LeaseMsg::Guard))?;
        // no GuardReply received so no promise to make
        time::sleep(Duration::from_millis(10)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_expired() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(150)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            n1.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            time::sleep(Duration::from_millis(20)).await;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // since no refreshes coming, after T_lease expires the held
            // promise should've expired
            time::sleep(Duration::from_millis(160)).await;
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
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(n0.recv_msg(1).await?, (7, LeaseMsg::GuardReply));
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
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        // no PromiseReply received so has to wait for T_guard + T_lease
        time::sleep(Duration::from_millis(10)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // still so after T_guard expires
        time::sleep(Duration::from_millis(160)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // no longer considered granted after T_guard + T_lease expires
        time::sleep(Duration::from_millis(160)).await;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::GrantTimeout { peer: 1 })
        );
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_refresh() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(150)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            n1.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            n1.send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            n1.send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            time::sleep(Duration::from_millis(20)).await;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // since no refreshes coming, after T_lease expires the held
            // promise should've expired
            time::sleep(Duration::from_millis(160)).await;
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
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (7, LeaseMsg::Guard))?;
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        // wait for GuardReply and send Promise
        assert_eq!(n0.recv_msg(1).await?, (7, LeaseMsg::GuardReply));
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
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.recv_msg(1).await?,
            (7, LeaseMsg::PromiseReply { held: true })
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
            (7, LeaseAction::ScheduleRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        n0.leaseman.extend_grant_timer(7, 1)?;
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        // wait for PromiseReply and schedule next refresh Promise again
        assert_eq!(
            n0.recv_msg(1).await?,
            (7, LeaseMsg::PromiseReply { held: true })
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
            (7, LeaseAction::ScheduleRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        n0.leaseman.extend_grant_timer(7, 1)?;
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        // no PromiseReply received so consider granted for 2 * T_lease since
        // last promise
        time::sleep(Duration::from_millis(10)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // still so after T_lease expires
        time::sleep(Duration::from_millis(160)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), peers);
        // no longer considered granted after 2 * T_lease has expired after
        // last promise
        time::sleep(Duration::from_millis(160)).await;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::GrantTimeout { peer: 1 })
        );
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revoke_replied() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(150)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            n1.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            n1.send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // grantor decided to revoke the lease
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Revoke));
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
            n1.send_msg(0, (7, LeaseMsg::RevokeReply { held: true }))?;
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
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(n0.recv_msg(1).await?, (7, LeaseMsg::GuardReply));
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
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.recv_msg(1).await?,
            (7, LeaseMsg::PromiseReply { held: true })
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
            (7, LeaseAction::ScheduleRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // I decide to revoke the granted lease
        n0.leaseman.add_notice(
            7,
            LeaseNotice::DoRevoke {
                peers: peers.clone(),
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
        n0.send_msg(1, (7, LeaseMsg::Revoke))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for RevokeReply
        assert_eq!(
            n0.recv_msg(1).await?,
            (7, LeaseMsg::RevokeReply { held: true })
        );
        n0.leaseman.add_notice(
            7,
            LeaseNotice::RecvLeaseMsg {
                peer: 1,
                msg: LeaseMsg::RevokeReply { held: true },
            },
        )?;
        time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                7,
                LeaseAction::RevokeReplied {
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
            TestNode::new_cluster(2, Duration::from_millis(150)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            n1.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            n1.send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // grantor decided to revoke the lease
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Revoke));
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
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(n0.recv_msg(1).await?, (7, LeaseMsg::GuardReply));
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
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.recv_msg(1).await?,
            (7, LeaseMsg::PromiseReply { held: true })
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
            (7, LeaseAction::ScheduleRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // I decide to revoke the granted lease
        n0.leaseman.add_notice(
            7,
            LeaseNotice::DoRevoke {
                peers: peers.clone(),
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
        n0.send_msg(1, (7, LeaseMsg::Revoke))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // not receiving RevokeReply, so has to wait for grant timeout
        assert_eq!(n0.leaseman.grant_set(), peers);
        time::sleep(Duration::from_millis(160)).await;
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        // also check that no further refreshes scheduled
        assert_eq!(
            n0.leaseman.get_action().await?,
            (7, LeaseAction::GrantTimeout { peer: 1 })
        );
        assert!(n0.leaseman.try_get_action().is_err());
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn regrant_higher() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(2, Duration::from_millis(150)).await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                7,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            n1.send_msg(0, (7, LeaseMsg::GuardReply))?;
            assert_eq!(n1.recv_msg(0).await?, (7, LeaseMsg::Promise));
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
            n1.send_msg(0, (7, LeaseMsg::PromiseReply { held: true }))?;
            assert_eq!(n1.leaseman.lease_cnt(), 1);
            // grantor decided to move to a new lease number
            assert_eq!(n1.recv_msg(0).await?, (8, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                8,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
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
            n1.send_msg(0, (8, LeaseMsg::GuardReply))?;
            assert_eq!(n1.recv_msg(0).await?, (8, LeaseMsg::Promise));
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
            n1.send_msg(0, (8, LeaseMsg::PromiseReply { held: true }))?;
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
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (7, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(n0.recv_msg(1).await?, (7, LeaseMsg::GuardReply));
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
        n0.send_msg(1, (7, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // wait for PromiseReply and schedule next refresh Promise
        assert_eq!(
            n0.recv_msg(1).await?,
            (7, LeaseMsg::PromiseReply { held: true })
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
            (7, LeaseAction::ScheduleRefresh { peer: 1 }),
        );
        time::sleep(Duration::from_millis(50)).await;
        assert_eq!(n0.leaseman.grant_set(), peers);
        // I decide to move to a new lease number
        n0.leaseman.add_notice(
            8,
            LeaseNotice::NewGrants {
                peers: peers.clone(),
            },
        )?;
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
        n0.send_msg(1, (8, LeaseMsg::Guard))?;
        // wait for GuardReply and send Promise
        assert_eq!(n0.recv_msg(1).await?, (8, LeaseMsg::GuardReply));
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
        n0.send_msg(1, (8, LeaseMsg::Promise))?;
        assert_eq!(n0.leaseman.grant_set(), peers);
        barrier0.wait().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn mutual_leases() -> Result<(), SummersetError> {
        let nodes =
            TestNode::new_cluster(3, Duration::from_millis(150)).await?;
        let mut joins = Vec::with_capacity(3);
        let barrier = Arc::new(Barrier::new(3));
        for (i, mut n) in nodes.into_iter().enumerate() {
            let barrier_ref = barrier.clone();
            let peers = Bitmap::from((
                3,
                (0..3_u8).filter(|&p| p != i as u8).collect::<Vec<u8>>(),
            ));
            joins.push(tokio::spawn(async move {
                n.leaseman.add_notice(
                    7,
                    LeaseNotice::NewGrants {
                        peers: peers.clone(),
                    },
                )?;
                assert_eq!(
                    n.leaseman.get_action().await?,
                    (
                        7,
                        LeaseAction::BcastLeaseMsgs {
                            peers: peers.clone(),
                            msg: LeaseMsg::Guard
                        }
                    )
                );
                // TODO: this test is obvious to hold but require some extra
                // work on the testing side to implement; leaving blank for now
                barrier_ref.wait().await;
                Ok::<(), SummersetError>(())
            }));
        }
        for j in joins {
            j.await??;
        }
        Ok(())
    }
}