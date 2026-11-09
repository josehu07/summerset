//! Process lifetime and resource helpers.

use std::os::unix::process::parent_id;
use std::sync::Once;
use std::time::Duration;
use std::{env, process, thread};

use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;

/// If env var `SUMMERSET_EXIT_WITH_PARENT` holds a PID, exits this process as
/// soon as that process is gone (e.g., a test harness or a launcher script that
/// got killed), so that it never gets left behind. That process is normally my
/// parent, but may be a further ancestor when launched through wrappers such as
/// `sudo`.
pub fn exit_with_parent() {
    let Some(ppid) = env::var("SUMMERSET_EXIT_WITH_PARENT")
        .ok()
        .and_then(|pid| pid.parse::<u32>().ok())
    else {
        return;
    };
    let is_parent = parent_id() == ppid;
    thread::spawn(move || {
        // being reparented also tells my parent is gone, even if it has not
        // been reaped yet
        while (!is_parent || parent_id() == ppid) && process_alive(ppid) {
            thread::sleep(Duration::from_millis(100));
        }
        pf_warn!("parent process {} is gone, exiting", ppid);
        process::exit(1);
    });
}

/// Whether a process exists (whether or not I could signal it).
fn process_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    !matches!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH))
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
