//! Process lifetime and resource helpers.

use std::os::unix::process::parent_id;
use std::sync::Once;
use std::time::Duration;
use std::{env, process, thread};

/// If env var `SUMMERSET_EXIT_WITH_PARENT` holds a PID, exits this process as
/// soon as its parent is no longer that process (e.g., a test harness or a
/// launcher script that got killed), so that it never gets left behind.
pub fn exit_with_parent() {
    let Some(ppid) = env::var("SUMMERSET_EXIT_WITH_PARENT")
        .ok()
        .and_then(|pid| pid.parse::<u32>().ok())
    else {
        return;
    };
    thread::spawn(move || {
        while parent_id() == ppid {
            thread::sleep(Duration::from_millis(100));
        }
        pf_warn!("parent process {} is gone, exiting", ppid);
        process::exit(1);
    });
}

/// Soft limit of open files to ensure, which `setrlimit()` accepts everywhere
/// (being `OPEN_MAX` on macOS) while leaving ample room for many connections.
const FD_LIMIT: u64 = 10240;

/// Raises this process's soft limit of open files to `FD_LIMIT` (capped by the
/// hard limit) if lower, as defaults can be as low as 256 (e.g., on macOS).
/// Done once, before the first socket is opened; best-effort.
pub(crate) fn raise_fd_limit() {
    static RAISED: Once = Once::new();
    RAISED.call_once(|| {
        let _ = rlimit::increase_nofile_limit(FD_LIMIT);
    });
}
