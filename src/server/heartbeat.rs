//! Summerset server heartbeats management module implementation.

use std::collections::HashMap;

use crate::server::ReplicaId;
use crate::utils::{Bitmap, SummersetError, Timer};

use rand::prelude::*;

use tokio::time::{self, Duration, Interval, MissedTickBehavior};

/// Multiplexed heartbeat timeout events type.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub(crate) enum HeartbeatEvent {
    /// Peer inactivity timeout.
    HearTimeout,

    /// Sending interval tick.
    SendTicked,
}

/// The heartbeats management module.
///
/// TODO: make this module channel-oriented like other modules and manage more
///       common things inherently, avoid polluting protocol modules
pub(crate) struct Heartbeater {
    /// My replica ID.
    _me: ReplicaId,

    /// Total number of replicas in the cluster.
    _population: u8,

    /// Timer for hearing heartbeat from, say, leader.
    hear_timer: Timer,

    /// Minimum hearing timeout interval.
    hear_timeout_min: Duration,

    /// Maximum hearing timeout interval.
    hear_timeout_max: Duration,

    /// Interval for sending heartbeat to peers.
    send_interval: Interval,

    /// True if sending ticks are enabled; false otherwise.
    is_sending: bool,

    /// Heartbeat reply counters for approximate detection of peer health.
    /// Tuple of (#hb_replied, #hb_replied seen at last send, repetition).
    reply_cnts: HashMap<ReplicaId, (u64, u64, u8)>,

    /// Approximate health status tracking of peer replicas.
    peer_alive: Bitmap,
}

impl Heartbeater {
    /// Creates a new heartbeats manager.
    pub(crate) fn new_and_setup(
        me: ReplicaId,
        population: u8,
        hear_timeout_min: Duration,
        hear_timeout_max: Duration,
        send_interval: Duration,
    ) -> Result<Heartbeater, SummersetError> {
        if hear_timeout_min < Duration::from_millis(100) {
            return logged_err!(
                "invalid heartbeat min hear_timeout {:?}",
                hear_timeout_min
            );
        }
        if hear_timeout_max < hear_timeout_min + Duration::from_millis(100) {
            return logged_err!(
                "heartbeat max hear_timeout {:?} must be >= 100ms + min hear_timeout {:?}",
                hear_timeout_max, hear_timeout_min
            );
        }
        if send_interval < Duration::from_millis(1)
            || send_interval > hear_timeout_max
        {
            return logged_err!(
                "invalid heartbeat send_interval {:?}",
                send_interval
            );
        }

        let mut send_interval = time::interval(send_interval);
        send_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let reply_cnts = (0..population)
            .filter_map(|p| if p == me { None } else { Some((p, (1, 0, 0))) })
            .collect();

        Ok(Heartbeater {
            _me: me,
            _population: population,
            hear_timer: Timer::default(),
            hear_timeout_min,
            hear_timeout_max,
            send_interval,
            is_sending: false,
            reply_cnts,
            peer_alive: Bitmap::new(population, true),
        })
    }

    /// Sets the sending flag.
    pub(crate) fn set_sending(&mut self, sending: bool) {
        self.is_sending = sending;
    }

    /// Waits for a heartbeat-related timeout event.
    pub(crate) async fn get_event(&mut self) -> HeartbeatEvent {
        tokio::select! {
            _ = self.hear_timer.timeout() => { HeartbeatEvent::HearTimeout },
            _ = self.send_interval.tick(), if self.is_sending => { HeartbeatEvent::SendTicked },
        }
    }

    /// Chooses a random timeout from the min-max range and kicks off the
    /// heartbeat hearing timer.
    pub(crate) fn kickoff_hear_timer(&mut self) -> Result<(), SummersetError> {
        self.hear_timer.cancel()?;

        let timeout_ms = thread_rng().gen_range(
            self.hear_timeout_min.as_millis()
                ..=self.hear_timeout_max.as_millis(),
        );
        // pf_trace!("kickoff hb_hear_timer @ {} ms", timeout_ms);
        self.hear_timer
            .kickoff(Duration::from_millis(timeout_ms as u64))
    }

    /// Gets the speculated liveness status of peers.
    pub(crate) fn peer_alive(&self) -> &Bitmap {
        &self.peer_alive
    }

    /// Clears peers' heartbeat reply counters statistics.
    pub(crate) fn clear_reply_cnts(&mut self) {
        for cnts in self.reply_cnts.values_mut() {
            *cnts = (1, 0, 0);
        }
    }

    /// Called upon each broadcast, updates peers' max heartbeat reply counters
    /// and their repetitions seen, and checks if we should speculate that the
    /// peer is down. On success, returns true if any peer death got speculated,
    /// and false otherwise.
    pub(crate) fn update_bcast_cnts(&mut self) -> Result<bool, SummersetError> {
        let mut peer_death = false;

        for (&peer, cnts) in self.reply_cnts.iter_mut() {
            if cnts.0 > cnts.1 {
                // more hb replies have been received from this peer; it is
                // probably alive
                cnts.1 = cnts.0;
                cnts.2 = 0;
            } else {
                // did not receive hb reply from this peer at least for the
                // last sent hb from me; increment repetition count
                cnts.2 += 1;
                let repeat_threshold = (self.hear_timeout_min.as_millis()
                    / self.send_interval.period().as_millis())
                    as u8;

                if cnts.2 > repeat_threshold {
                    // did not receive hb reply from this peer for too many
                    // past hbs sent from me; this peer is probably dead
                    if self.peer_alive.get(peer)? {
                        self.peer_alive.set(peer, false)?;
                        pf_info!("peer_alive updated: {:?}", self.peer_alive);
                        peer_death = true;
                    }
                    cnts.2 = 0;
                }
            }
        }

        Ok(peer_death)
    }

    /// Called upon each hearing, updates a peer's heard heartbeat counter,
    /// and checks if we should speculate that the peer is back up.
    pub(crate) fn update_heard_cnt(
        &mut self,
        peer: ReplicaId,
    ) -> Result<(), SummersetError> {
        if let Some(cnts) = self.reply_cnts.get_mut(&peer) {
            cnts.0 += 1;

            if !self.peer_alive.get(peer)? {
                self.peer_alive.set(peer, true)?;
                pf_info!("peer_alive updated: {:?}", self.peer_alive);
            }

            Ok(())
        } else {
            logged_err!("peer {} not found in reply_cnts", peer)
        }
    }
}

// TODO: add Heartbeater module unit tests after fleshing it up
