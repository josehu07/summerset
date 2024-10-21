//! Summerset server lease manager module implementation.

use std::collections::HashMap;
use std::sync::Arc;

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
    NextRefresh { peer: ReplicaId },
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

    /// By internal timers only:
    /// Timed-out as a grantor (either in guard phase or in repeated promise).
    GrantTimeout { peer: ReplicaId },

    /// By internal timers only:
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
    _me: ReplicaId,

    /// Total number of replicas in the cluster.
    population: u8,

    /// Receiver side of the action channel.
    rx_action: mpsc::UnboundedReceiver<(LeaseNum, LeaseAction)>,

    /// Sender side of the notice channel.
    tx_notice: mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>,

    /// Map from replica ID I've grant lease to on current lease number -> T_guard
    /// timer; shared with the lease manager thread.
    promises_sent: flashmap::ReadHandle<ReplicaId, Timer>,

    /// Map from replica ID I've been granted lease from (i.e., held) on current
    /// lease number -> T_lease timer; shared with the lease manager thread.
    promises_held: flashmap::ReadHandle<ReplicaId, Timer>,

    /// Join handle of the lease manager thread.
    _lease_manager_handle: JoinHandle<()>,
}

// LeaseManager public API implementation
impl LeaseManager {
    /// Creates a new lease manager.
    pub(crate) fn new_and_setup(
        me: ReplicaId,
        population: u8,
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
        if 2 * hb_send_interval >= lease_timeout {
            return logged_err!(
                "heartbeat interval {:?} too short given lease timeout {:?}",
                hb_send_interval,
                lease_timeout
            );
        }

        let (tx_notice, rx_notice) = mpsc::unbounded_channel();
        let (tx_action, rx_action) = mpsc::unbounded_channel();

        let (promises_sent_write, promises_sent_read) =
            flashmap::new::<ReplicaId, Timer>();
        let (promises_held_write, promises_held_read) =
            flashmap::new::<ReplicaId, Timer>();

        let lease_manager_handle = tokio::spawn(Self::lease_manager_thread(
            me,
            population,
            guard_timeout,
            lease_timeout,
            tx_action,
            tx_notice.clone(),
            rx_notice,
            promises_sent_write,
            promises_held_write,
        ));

        Ok(LeaseManager {
            _me: me,
            population,
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
}

// LeaseManager lease_manager thread implementation
impl LeaseManager {
    /// Attempts new grants to peers.
    #[allow(clippy::too_many_arguments)]
    fn handle_new_grants(
        lease_num: LeaseNum,
        peers: Bitmap,
        me: ReplicaId,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        tx_notice: Arc<mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,
        guards_sent: &mut HashMap<ReplicaId, Timer>,
        promises_sent: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        let mut bcast_peers = peers.clone();
        for (peer, flag) in peers.iter() {
            if peer == me || !flag {
                continue;
            }

            if promises_sent.guard().contains_key(&peer) {
                // already granting promise to this peer with this number, skip
                bcast_peers.set(peer, false)?;
                continue;
            }

            // enter guard phase and create grant-side timer
            let tx_notice_ref = tx_notice.clone();
            let timer = Timer::new(
                false,
                Some(move || {
                    tx_notice_ref
                        .send((lease_num, LeaseNotice::GrantTimeout { peer }))
                        .expect("sending to tx_notice_ref should succeed");
                }),
            );
            guards_sent.insert(peer, timer);
        }

        // broadcast Guard messages to these peers
        tx_action.send((
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
        lease_num: LeaseNum,
        peers: Bitmap,
        me: ReplicaId,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        guards_sent: &mut HashMap<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        for (peer, flag) in peers.iter() {
            if peer == me || !flag {
                continue;
            }

            // remove existing guard about this peer if exists
            guards_sent.remove(&peer);
        }

        // broadcast Revoke messages to these peers
        tx_action.send((
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
        lease_num: LeaseNum,
        peer: ReplicaId,
        guard_timeout: Duration,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        tx_notice: Arc<mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>>,
        guards_held: &mut HashMap<ReplicaId, Timer>,
        promises_held: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        if promises_held.guard().contains_key(&peer) {
            // already held promise from this peer with this number, ignore
            return Ok(());
        }

        // create recv-side timer and kickoff for T_guard, and update guards_held
        let timer = Timer::new(
            false,
            Some(move || {
                tx_notice
                    .send((lease_num, LeaseNotice::LeaseTimeout { peer }))
                    .expect("sending to tx_notice_ref should succeed");
            }),
        );
        timer.kickoff(guard_timeout)?;
        guards_held.insert(peer, timer);

        // send GuardReply back
        tx_action.send((
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
        lease_num: LeaseNum,
        peer: ReplicaId,
        guard_timeout: Duration,
        lease_timeout: Duration,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        guards_sent: &mut HashMap<ReplicaId, Timer>,
        promises_sent: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        if !guards_sent.contains_key(&peer) {
            // not a GuardReply I'm expecting, ignore
            return Ok(());
        }

        // move peer from guards_sent to promises_sent and kickoff for
        // T_guard + T_promise
        let timer = guards_sent.remove(&peer).unwrap();
        timer.kickoff(guard_timeout + lease_timeout)?;
        promises_sent.guard().insert(peer, timer);

        // send Promise message to this peer
        tx_action.send((
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
        lease_num: LeaseNum,
        peer: ReplicaId,
        lease_timeout: Duration,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        guards_held: &mut HashMap<ReplicaId, Timer>,
        promises_held: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        if guards_held.contains_key(&peer) {
            // was in guard phase, this is the first promise
            let timer = guards_held.remove(&peer).unwrap();
            timer.kickoff(lease_timeout)?;
            promises_held.guard().insert(peer, timer);

            // send PromiseReply back
            tx_action.send((
                lease_num,
                LeaseAction::SendLeaseMsg {
                    peer,
                    msg: LeaseMsg::PromiseReply { held: true },
                },
            ))?;
        } else if promises_held.guard().contains_key(&peer) {
            // was in promise phase, this is a refresh
            promises_held
                .guard()
                .get(&peer)
                .unwrap()
                .kickoff(lease_timeout)?;

            // send PromiseReply back
            tx_action.send((
                lease_num,
                LeaseAction::SendLeaseMsg {
                    peer,
                    msg: LeaseMsg::PromiseReply { held: true },
                },
            ))?;
        } else {
            // was in neither, so not a Promise I'm expecting; reply with
            // `held` false
            tx_action.send((
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
        lease_num: LeaseNum,
        peer: ReplicaId,
        held: bool,
        lease_timeout: Duration,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        guards_sent: &mut HashMap<ReplicaId, Timer>,
        promises_sent: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        if !promises_sent.guard().contains_key(&peer) {
            // not a PromiseReply I'm expecting, ignore
            return Ok(());
        }
        if !held {
            // failed promise, proactively remove peer from guards_sent or
            // promises_sent for efficiency
            guards_sent.remove(&peer);
            promises_sent.guard().remove(peer);
            return Ok(());
        }

        // refresh timer for T_lease (can safely be min(curr_remaining,
        // lease_timeout) but nah doesn't matter)
        promises_sent
            .guard()
            .get(&peer)
            .unwrap()
            .kickoff(lease_timeout)?;

        // let protocol module mark next heartbeat as a promise refresh
        tx_action.send((lease_num, LeaseAction::NextRefresh { peer }))?;
        Ok(())
    }

    /// Received a Revoke message.
    fn handle_msg_revoke(
        lease_num: LeaseNum,
        peer: ReplicaId,
        tx_action: &mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        guards_held: &mut HashMap<ReplicaId, Timer>,
        promises_held: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        // remove existing guard or promise held from this peer
        Self::handle_lease_timeout(peer, guards_held, promises_held)?;

        // send RevokeReply back
        tx_action.send((
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
        _lease_num: LeaseNum,
        peer: ReplicaId,
        _held: bool,
        guards_sent: &mut HashMap<ReplicaId, Timer>,
        promises_sent: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        // remove existing guard or promise sent to this peer
        Self::handle_grant_timeout(peer, guards_sent, promises_sent)?;
        Ok(())
    }

    /// Timeout reached granting lease to a peer.
    fn handle_grant_timeout(
        peer: ReplicaId,
        guards_sent: &mut HashMap<ReplicaId, Timer>,
        promises_sent: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        // remove existing guard or promise sent to this peer
        guards_sent.remove(&peer);
        promises_sent.guard().remove(peer);
        Ok(())
    }

    /// Timeout reached without promise refresh from a peer.
    fn handle_lease_timeout(
        peer: ReplicaId,
        guards_held: &mut HashMap<ReplicaId, Timer>,
        promises_held: &mut flashmap::WriteHandle<ReplicaId, Timer>,
    ) -> Result<(), SummersetError> {
        // remove existing guard or promise held from this peer
        guards_held.remove(&peer);
        promises_held.guard().remove(peer);
        Ok(())
    }

    /// Lease manager thread function.
    #[allow(clippy::too_many_arguments)]
    async fn lease_manager_thread(
        me: ReplicaId,
        population: u8,
        guard_timeout: Duration,
        lease_timeout: Duration,
        tx_action: mpsc::UnboundedSender<(LeaseNum, LeaseAction)>,
        // will be cloned into every timer thread for its timeout trigger
        tx_notice: mpsc::UnboundedSender<(LeaseNum, LeaseNotice)>,
        mut rx_notice: mpsc::UnboundedReceiver<(LeaseNum, LeaseNotice)>,
        mut promises_sent: flashmap::WriteHandle<ReplicaId, Timer>,
        mut promises_held: flashmap::WriteHandle<ReplicaId, Timer>,
    ) {
        pf_debug!("lease_manager thread spawned");

        // the active lease number must be monotonically non-decreasing; old
        // lease numbers mean actions/notices for old leasing periods and are
        // simply ignored
        let mut active_num: LeaseNum = 0;

        // need to internally maintain a set of in-progress guards sent/held.
        // On either side, a peer must not appear in both guards and promises
        // at the same time
        let mut guards_sent: HashMap<ReplicaId, Timer> = HashMap::new();
        let mut guards_held: HashMap<ReplicaId, Timer> = HashMap::new();

        let tx_notice_arc = Arc::new(tx_notice);
        while let Some((lease_num, notice)) = rx_notice.recv().await {
            #[allow(clippy::comparison_chain)]
            if lease_num < active_num {
                pf_warn!(
                    "ignoring outdated lease number: {} < {}",
                    lease_num,
                    active_num
                );
                continue;
            } else if lease_num > active_num {
                // starting the grants of a new lease number, invalidate everything
                // with old number
                pf_debug!(
                    "observed higher lease number: {} > {}",
                    lease_num,
                    active_num
                );
                guards_sent.clear();
                guards_held.clear();
                {
                    let mut promises_sent_guard = promises_sent.guard();
                    for peer in (0..population).filter(|&p| p != me) {
                        promises_sent_guard.remove(peer);
                    }
                }
                {
                    let mut promises_held_guard = promises_held.guard();
                    for peer in (0..population).filter(|&p| p != me) {
                        promises_held_guard.remove(peer);
                    }
                }
                active_num = lease_num;
            }

            match notice {
                LeaseNotice::NewGrants { peers } => {
                    if let Err(e) = Self::handle_new_grants(
                        lease_num,
                        peers,
                        me,
                        &tx_action,
                        tx_notice_arc.clone(),
                        &mut guards_sent,
                        &mut promises_sent,
                    ) {
                        pf_error!("error handling lease NewGrants: {}", e);
                    }
                }
                LeaseNotice::DoRevoke { peers } => {
                    if let Err(e) = Self::handle_do_revoke(
                        lease_num,
                        peers,
                        me,
                        &tx_action,
                        &mut guards_sent,
                    ) {
                        pf_error!("error handling lease DoRevoke: {}", e);
                    }
                }
                LeaseNotice::RecvLeaseMsg { peer, msg } => {
                    debug_assert_ne!(peer, me);
                    if let Err(e) = match msg {
                        LeaseMsg::Guard => Self::handle_msg_guard(
                            lease_num,
                            peer,
                            guard_timeout,
                            &tx_action,
                            tx_notice_arc.clone(),
                            &mut guards_held,
                            &mut promises_held,
                        ),
                        LeaseMsg::GuardReply => Self::handle_msg_guard_reply(
                            lease_num,
                            peer,
                            guard_timeout,
                            lease_timeout,
                            &tx_action,
                            &mut guards_held,
                            &mut promises_held,
                        ),
                        LeaseMsg::Promise => Self::handle_msg_promise(
                            lease_num,
                            peer,
                            lease_timeout,
                            &tx_action,
                            &mut guards_held,
                            &mut promises_held,
                        ),
                        LeaseMsg::PromiseReply { held } => {
                            Self::handle_msg_promise_reply(
                                lease_num,
                                peer,
                                held,
                                lease_timeout,
                                &tx_action,
                                &mut guards_sent,
                                &mut promises_sent,
                            )
                        }
                        LeaseMsg::Revoke => Self::handle_msg_revoke(
                            lease_num,
                            peer,
                            &tx_action,
                            &mut guards_held,
                            &mut promises_held,
                        ),
                        LeaseMsg::RevokeReply { held } => {
                            Self::handle_msg_revoke_reply(
                                lease_num,
                                peer,
                                held,
                                &mut guards_sent,
                                &mut promises_sent,
                            )
                        }
                    } {
                        pf_error!("error handling lease message: {}", e);
                    }
                }
                LeaseNotice::GrantTimeout { peer } => {
                    debug_assert_ne!(peer, me);
                    if let Err(e) = Self::handle_grant_timeout(
                        peer,
                        &mut guards_sent,
                        &mut promises_sent,
                    ) {
                        pf_error!("error handling lease GrantTimeout: {}", e);
                    }
                }
                LeaseNotice::LeaseTimeout { peer } => {
                    debug_assert_ne!(peer, me);
                    if let Err(e) = Self::handle_lease_timeout(
                        peer,
                        &mut guards_held,
                        &mut promises_held,
                    ) {
                        pf_error!("error handling lease LeaseTimeout: {}", e);
                    }
                }
            }
        }

        pf_debug!("lease_manager thread exitted");
    }
}

// TODO: tests: grant_refresh, grant_expire, regrant_number, active_revoke
#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
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
            guard_timeout: Duration,
            lease_timeout: Duration,
        ) -> Result<Vec<TestNode>, SummersetError> {
            assert!(population >= 2);
            let mut nodes = Vec::with_capacity(population as usize);

            let mut channels = HashMap::with_capacity(
                (population * (population - 1)) as usize,
            );
            for i in 0..population {
                for j in (0..population).filter(|&p| p != i) {
                    let (tx_msg, rx_msg) = mpsc::unbounded_channel();
                    channels.insert((i, j), (tx_msg, rx_msg));
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
                    guard_timeout,
                    lease_timeout,
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
    async fn guard_expire() -> Result<(), SummersetError> {
        let nodes = TestNode::new_cluster(
            2,
            Duration::from_millis(100),
            Duration::from_millis(100),
        )
        .await?;
        let [mut n0, mut n1] = <[TestNode; 2]>::try_from(nodes)
            .unwrap_or_else(|_| panic!("nodes vec unpack failed"));
        let barrier0 = Arc::new(Barrier::new(2));
        let barrier1 = barrier0.clone();
        tokio::spawn(async move {
            // replica 1
            barrier1.wait().await;
            // wait for granting from 0
            assert_eq!(n1.recv_msg(0).await?, (1, LeaseMsg::Guard));
            n1.leaseman.add_notice(
                1,
                LeaseNotice::RecvLeaseMsg {
                    peer: 0,
                    msg: LeaseMsg::Guard,
                },
            )?;
            assert_eq!(
                n1.leaseman.get_action().await?,
                (
                    1,
                    LeaseAction::SendLeaseMsg {
                        peer: 0,
                        msg: LeaseMsg::GuardReply
                    }
                )
            );
            // deliberately not sending the GuardReply
            time::sleep(Duration::from_millis(20)).await;
            assert_eq!(n1.try_recv_msg(0), Ok(None));
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            // still so after T_guard expires
            time::sleep(Duration::from_millis(100)).await;
            assert_eq!(n1.try_recv_msg(0), Ok(None));
            assert_eq!(n1.leaseman.lease_cnt(), 0);
            Ok::<(), SummersetError>(())
        });
        // replica 0
        barrier0.wait().await;
        // initiate granting to 1
        let peers = Bitmap::from((2, vec![1]));
        n0.leaseman.add_notice(
            1,
            LeaseNotice::NewGrants {
                peers: peers.clone(),
            },
        )?;
        assert_eq!(
            n0.leaseman.get_action().await?,
            (
                1,
                LeaseAction::BcastLeaseMsgs {
                    peers: peers.clone(),
                    msg: LeaseMsg::Guard
                }
            )
        );
        n0.send_msg(1, (1, LeaseMsg::Guard))?;
        // no GuardReply received so no promise to make
        time::sleep(Duration::from_millis(10)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        // still so after T_guard expires
        time::sleep(Duration::from_millis(100)).await;
        assert_eq!(n0.try_recv_msg(1), Ok(None));
        assert_eq!(n0.leaseman.grant_set(), Bitmap::new(2, false));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_expire() -> Result<(), SummersetError> {
        let nodes = TestNode::new_cluster(
            2,
            Duration::from_millis(200),
            Duration::from_millis(200),
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn promise_refresh() -> Result<(), SummersetError> {
        let nodes = TestNode::new_cluster(
            2,
            Duration::from_millis(200),
            Duration::from_millis(200),
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn regrant_higher() -> Result<(), SummersetError> {
        let nodes = TestNode::new_cluster(
            2,
            Duration::from_millis(200),
            Duration::from_millis(200),
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mutual_grants() -> Result<(), SummersetError> {
        let nodes = TestNode::new_cluster(
            3,
            Duration::from_millis(200),
            Duration::from_millis(200),
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_revoke() -> Result<(), SummersetError> {
        let nodes = TestNode::new_cluster(
            2,
            Duration::from_millis(200),
            Duration::from_millis(200),
        )
        .await?;
        Ok(())
    }
}
