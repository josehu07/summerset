//! Fault injection for testing. Faults model a server process's environment
//! (its network, disk, and clock), so they are set process-wide through the
//! manager's `InjectFaults` and `CrashServers` control requests, and applied by
//! the transport hub, the storage hub, and timers.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rand::prelude::*;
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::server::ReplicaId;

/// Faults to inject into a server; the default injects none.
#[derive(Debug, Default, PartialEq, Eq, Clone, Serialize, Deserialize, SchemaWrite, SchemaRead)]
pub struct Faults {
    /// Seed of the random fault decisions.
    pub seed: u64,

    /// Percentage of outgoing peer messages dropped.
    pub msg_drop_pct: u8,

    /// Percentage of outgoing peer messages sent twice.
    pub msg_dup_pct: u8,

    /// Delay added to each outgoing peer message, in millisecs.
    pub msg_delay_ms: u64,

    /// Extra random delay of up to this many millisecs per outgoing peer
    /// message, which also reorders messages.
    pub msg_jitter_ms: u64,

    /// Peers that no outgoing message reaches (a one-way partition).
    pub cut_peers: HashSet<ReplicaId>,

    /// Delay added before each storage operation, in millisecs.
    pub disk_delay_ms: u64,

    /// Percentage of storage writes that fail with an I/O error.
    pub disk_fail_pct: u8,

    /// Percentage by which this server's clock runs fast (or slow if
    /// negative), which scales all timer durations.
    pub clock_drift_pct: i8,
}

/// Process-wide fault injection state.
#[derive(Default)]
struct FaultState {
    faults: Faults,
    rng: Option<StdRng>,
    /// Size up to which each storage file has been synced.
    synced: HashMap<PathBuf, usize>,
    /// Whether the next peer leave is skipped due to a crash.
    crash_leave: bool,
    /// Synced sizes of storage files at a power loss, applied when reopened.
    crashed: HashMap<PathBuf, usize>,
}

static FAULT_STATE: LazyLock<Mutex<FaultState>> = LazyLock::new(Mutex::default);

/// Whether any fault in `Faults` is active, to keep the common path cheap.
static ACTIVE: AtomicBool = AtomicBool::new(false);

fn fault_state() -> MutexGuard<'static, FaultState> {
    FAULT_STATE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Rolls a percentage-chance event.
fn roll_pct_dice(rng: &mut StdRng, pct: u8) -> bool {
    pct > 0 && rng.random_range(0..100) < pct
}

/// Sets the faults to inject from now on, replacing any previous ones.
pub(crate) fn inject_faults(faults: Faults) {
    pf_info!("injecting faults {:?}", faults);
    let mut st = fault_state();
    let none = Faults {
        seed: faults.seed,
        ..Faults::default()
    };
    ACTIVE.store(faults != none, Ordering::Release);
    st.rng = Some(StdRng::seed_from_u64(faults.seed));
    st.faults = faults;
}

/// Decides how an outgoing message to `peer` is sent: the delay of each copy
/// to send, none if dropped. Returns `None` if no faults are active, meaning
/// a single copy sent right away.
pub(crate) fn msg_copies_delays(peer: ReplicaId) -> Option<Vec<Duration>> {
    if !ACTIVE.load(Ordering::Acquire) {
        return None;
    }
    let mut st = fault_state();
    let FaultState { faults, rng, .. } = &mut *st;
    let rng = rng.as_mut()?;
    if faults.cut_peers.contains(&peer) || roll_pct_dice(rng, faults.msg_drop_pct) {
        pf_debug!("fault: dropped msg -> {}", peer);
        return Some(vec![]);
    }
    let copies = if roll_pct_dice(rng, faults.msg_dup_pct) {
        pf_debug!("fault: duplicated msg -> {}", peer);
        2
    } else {
        1
    };
    Some(
        (0..copies)
            .map(|_| {
                let jitter = rng.random_range(0..=faults.msg_jitter_ms);
                Duration::from_millis(faults.msg_delay_ms + jitter)
            })
            .collect(),
    )
}

/// Decides the fault of a storage operation: its delay, and whether it fails
/// (writes only). Returns `None` if no faults are active.
pub(crate) fn disk_delay_fail(write: bool) -> Option<(Duration, bool)> {
    if !ACTIVE.load(Ordering::Acquire) {
        return None;
    }
    let mut st = fault_state();
    let FaultState { faults, rng, .. } = &mut *st;
    let fail = write && roll_pct_dice(rng.as_mut()?, faults.disk_fail_pct);
    if fail {
        pf_debug!("fault: failed disk write");
    }
    Some((Duration::from_millis(faults.disk_delay_ms), fail))
}

/// Scales a timer duration by the injected clock drift.
pub(crate) fn clock_drifted(dur: Duration) -> Duration {
    if !ACTIVE.load(Ordering::Acquire) {
        return dur;
    }
    let drift = fault_state().faults.clock_drift_pct;
    // a clock running fast by x% makes timers fire after 100 / (100 + x)
    let rate = (100 + i16::from(drift)).max(1);
    dur.mul_f64(100.0 / f64::from(rate))
}

/// Records the size up to which a storage file is synced.
pub(crate) fn record_file_synced(path: &Path, size: usize) {
    let mut st = fault_state();
    if let Some(synced) = st.synced.get_mut(path) {
        *synced = size;
    } else {
        st.synced.insert(path.into(), size);
    }
}

/// Crashes this server process at its next reset, which then skips the peer
/// leave handshake. If `power_loss`, storage files also lose what they had not
/// synced; otherwise they keep everything written, as in the OS page cache.
pub(crate) fn crash_server(power_loss: bool) {
    pf_info!("crashing at next reset (power loss: {})", power_loss);
    let st = &mut *fault_state();
    st.crash_leave = true;
    if power_loss {
        st.crashed.clone_from(&st.synced);
    } else {
        st.crashed.clear();
    }
}

/// Whether the current peer leave is skipped due to a crash.
pub(crate) fn take_crash_leave() -> bool {
    std::mem::take(&mut fault_state().crash_leave)
}

/// Returns the size that a storage file keeps when reopened after a power
/// loss, if it had one.
pub(crate) fn take_crash_size(path: &Path) -> Option<usize> {
    fault_state().crashed.remove(path)
}
