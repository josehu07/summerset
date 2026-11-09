//! Integration tests of the server executable, run against an in-process
//! cluster manager.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use summerset::{
    ApiReply, ApiRequest, Command, CommandResult, CtrlReply, CtrlRequest, Faults, GenericEndpoint,
    SmrProtocol,
};
use tempfile::TempDir;
use tokio::net::TcpSocket;
use tokio::process::{self, Child};
use tokio::sync::watch;
use tokio::time;

const EXE: &str = env!("CARGO_BIN_EXE_summerset_server");

/// Generous bound for operations that are expected to succeed.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Held while probing ports or spawning processes: as sockets cannot be made
/// close-on-exec atomically on macOS, a probe socket open during a spawn could
/// leak into the child and keep its port taken.
static SPAWNING: Mutex<()> = Mutex::new(());

/// Returns an unused local address. Ports are taken in turn, from a random
/// start, below the OS ephemeral range, so none can be grabbed by other tests'
/// connections before the node under test binds it.
fn free_addr() -> SocketAddr {
    static START: LazyLock<u32> = LazyLock::new(|| {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        now.subsec_nanos() ^ std::process::id()
    });
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let _spawning = SPAWNING.lock().unwrap_or_else(PoisonError::into_inner);
    for _ in 0..12000 {
        let n = START.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed));
        let port = u16::try_from(20000 + n % 12000).unwrap();
        // probe the wildcard address without SO_REUSEADDR, which fails if any
        // socket still uses the port, including connections in TIME_WAIT
        if TcpSocket::new_v4()
            .unwrap()
            .bind(([0, 0, 0, 0], port).into())
            .is_ok()
        {
            return ([127, 0, 0, 1], port).into();
        }
    }
    panic!("no free port");
}

/// Awaits a future, failing the test if it takes longer than `TIMEOUT`, so
/// that no test can hang (and linger around) forever.
#[track_caller]
fn within<T>(fut: impl Future<Output = T>) -> impl Future<Output = T> {
    let caller = std::panic::Location::caller();
    async move {
        time::timeout(TIMEOUT, fut)
            .await
            .unwrap_or_else(|_| panic!("timed out at {caller}"))
    }
}

/// Spawns a process; see `SPAWNING`.
fn spawn(cmd: &mut process::Command) -> Child {
    let _spawning = SPAWNING.lock().unwrap_or_else(PoisonError::into_inner);
    cmd.spawn().unwrap()
}

/// Prepares the server executable with given whitespace-separated args.
fn exe(args: &str) -> process::Command {
    let mut cmd = process::Command::new(EXE);
    cmd.args(args.split_whitespace())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env("SUMMERSET_EXIT_WITH_PARENT", std::process::id().to_string())
        .kill_on_drop(true);
    cmd
}

/// Waits for a process to exit, returning whether it succeeded.
async fn exits_ok(child: &mut Child) -> bool {
    within(child.wait()).await.unwrap().success()
}

/// Sends a control request to the manager and returns its reply.
async fn ctrl(ep: &mut dyn GenericEndpoint, req: CtrlRequest) -> CtrlReply {
    ep.ctrl_stub().send_req_insist(&req).unwrap();
    within(ep.ctrl_stub().recv_reply()).await.unwrap()
}

/// Issues a command to the connected server and returns its result.
async fn request(ep: &mut dyn GenericEndpoint, id: u64, cmd: Command) -> CommandResult {
    assert!(ep.send_req(Some(&ApiRequest::Req { id, cmd })).unwrap());
    match within(ep.recv_reply()).await.unwrap() {
        ApiReply::Reply {
            result: Some(result),
            ..
        } => result,
        reply => panic!("unexpected reply {reply:?}"),
    }
}

/// Starts an in-process manager of a single-server cluster, returning its
/// server-facing and client-facing addresses.
async fn start_manager() -> (SocketAddr, SocketAddr) {
    let (srv_addr, cli_addr) = (free_addr(), free_addr());
    let mut manager =
        within(SmrProtocol::RepNothing.new_cluster_manager_setup(srv_addr, cli_addr, 1))
            .await
            .unwrap();
    let (tx_term, rx_term) = watch::channel(false);
    tokio::spawn(async move {
        let _tx_term = tx_term; // run until the test ends
        manager.run(rx_term).await
    });
    (srv_addr, cli_addr)
}

/// Composes args for the server executable to join the manager at `srv_addr`
/// as server `id`, with fresh ports, its files under `dir`, and given extra
/// config (`+`-prefixed).
fn server_args(dir: &Path, srv_addr: SocketAddr, id: u8, config: &str) -> String {
    format!(
        "-p RepNothing -b 127.0.0.1 -a {} -i {} -m {srv_addr} --id {id} --threads 2 --config backer_path='{}'{config}",
        free_addr().port(),
        free_addr().port(),
        dir.join("wal").display()
    )
}

/// Starts an in-process manager and the server executable with given extra
/// config (`+`-prefixed), returning the server and a client endpoint connected
/// to it once it has joined.
async fn start(dir: &Path, config: &str) -> (Child, Box<dyn GenericEndpoint>) {
    let (srv_addr, cli_addr) = start_manager().await;
    let server = spawn(&mut exe(&server_args(dir, srv_addr, 0, config)));
    (server, connect_client(cli_addr).await)
}

/// Returns a client endpoint of the manager at `cli_addr`, connected to the
/// server once it has joined.
async fn connect_client(cli_addr: SocketAddr) -> Box<dyn GenericEndpoint> {
    let mut ep = within(SmrProtocol::RepNothing.new_client_endpoint(cli_addr, None))
        .await
        .unwrap();
    within(async {
        while matches!(ctrl(ep.as_mut(), CtrlRequest::QueryInfo).await,
            CtrlReply::QueryInfo { servers_info, .. } if servers_info.is_empty())
        {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    within(ep.connect()).await.unwrap();
    ep
}

#[tokio::test]
async fn rejects_bad_args() {
    for args in [
        "-p Unknown -m 127.0.0.1:52600",
        "-p RepNothing -m 127.0.0.1:52600 --threads 1",
        "-p RepNothing -m 127.0.0.1:52600 -a 1000",
        "-p RepNothing",
    ] {
        assert!(
            !exits_ok(&mut spawn(&mut exe(args))).await,
            "accepted args {args:?}"
        );
    }
}

#[tokio::test]
async fn reset_restarts_in_place() {
    let dir = TempDir::new().unwrap();
    let (mut server, mut ep) = start(dir.path(), "").await;
    let put = Command::Put {
        key: "k".into(),
        value: "v".into(),
    };
    request(ep.as_mut(), 0, put).await;
    within(ep.leave(false)).await.unwrap();
    let reset = CtrlRequest::ResetServers {
        servers: HashSet::from([0]),
        durable: true,
    };
    ctrl(ep.as_mut(), reset).await;
    assert!(
        server.try_wait().unwrap().is_none(),
        "process exited on reset"
    );
    within(ep.connect()).await.unwrap();
    let get = Command::Get { key: "k".into() };
    assert_eq!(
        request(ep.as_mut(), 1, get).await,
        CommandResult::Get {
            value: Some("v".into())
        }
    );
}

#[tokio::test]
async fn killed_server_rejoins() {
    let dir = TempDir::new().unwrap();
    let (srv_addr, cli_addr) = start_manager().await;
    let args = server_args(dir.path(), srv_addr, 0, "");
    let mut server = spawn(&mut exe(&args));
    let mut ep = connect_client(cli_addr).await;
    request(ep.as_mut(), 0, put_command("k", "v")).await;
    within(ep.leave(false)).await.unwrap();

    // a real crash, unknown to the manager: restarted under its ID, the server
    // rejoins with its durable state
    within(server.kill()).await.unwrap();
    let _server = spawn(&mut exe(&args));
    within(ep.connect()).await.unwrap();
    let get = Command::Get { key: "k".into() };
    assert_eq!(
        request(ep.as_mut(), 1, get).await,
        CommandResult::Get {
            value: Some("v".into())
        }
    );
}

#[tokio::test]
async fn unavailable_id_rejected() {
    let dir = TempDir::new().unwrap();
    let (srv_addr, _) = start_manager().await;
    // the manager replies to the request instead of leaving it waiting
    let args = server_args(dir.path(), srv_addr, 1, "");
    assert!(!exits_ok(&mut spawn(&mut exe(&args))).await);
}

#[tokio::test]
async fn sigterm_exits_cleanly() {
    let dir = TempDir::new().unwrap();
    let (mut server, _ep) = start(dir.path(), "").await;
    let pid = server.id().unwrap().to_string();
    let mut kill = spawn(process::Command::new("kill").args(["-TERM", &pid]));
    assert!(exits_ok(&mut kill).await && exits_ok(&mut server).await);
}

#[tokio::test]
async fn crashes_keep_expected_writes() {
    // a process crash keeps all writes; a power loss keeps only synced ones
    for (power_loss, sync, kept) in [
        (false, false, Some("v")),
        (true, false, None),
        (true, true, Some("v")),
    ] {
        let dir = TempDir::new().unwrap();
        let (_server, mut ep) = start(dir.path(), &format!("+logger_sync={sync}")).await;
        let put = Command::Put {
            key: "k".into(),
            value: "v".into(),
        };
        request(ep.as_mut(), 0, put).await;
        within(ep.leave(false)).await.unwrap();
        let crash = CtrlRequest::CrashServers {
            servers: HashSet::from([0]),
            power_loss,
        };
        ctrl(ep.as_mut(), crash).await;
        within(ep.connect()).await.unwrap();
        let get = Command::Get { key: "k".into() };
        assert_eq!(
            request(ep.as_mut(), 1, get).await,
            CommandResult::Get {
                value: kept.map(Into::into)
            },
            "power_loss={power_loss} logger_sync={sync}"
        );
    }
}

#[tokio::test]
async fn injected_disk_delay_applies() {
    let dir = TempDir::new().unwrap();
    let (_server, mut ep) = start(dir.path(), "").await;
    let faults = Faults {
        disk_delay_ms: 200,
        ..Faults::default()
    };
    let inject = CtrlRequest::InjectFaults {
        servers: HashSet::new(),
        faults,
    };
    ctrl(ep.as_mut(), inject).await;
    let begin = Instant::now();
    let put = Command::Put {
        key: "k".into(),
        value: "v".into(),
    };
    request(ep.as_mut(), 0, put).await;
    assert!(begin.elapsed() >= Duration::from_millis(200));
}

/// Composes a command that puts a key-value pair.
fn put_command(key: &str, value: &str) -> Command {
    Command::Put {
        key: key.into(),
        value: value.into(),
    }
}

/// Injects given faults into the server.
async fn inject_faults(ep: &mut dyn GenericEndpoint, faults: Faults) {
    let inject = CtrlRequest::InjectFaults {
        servers: HashSet::new(),
        faults,
    };
    ctrl(ep, inject).await;
}

#[tokio::test]
async fn torn_writes_keep_log_recoverable() {
    let dir = TempDir::new().unwrap();
    let (_server, mut ep) = start(dir.path(), "").await;
    request(ep.as_mut(), 0, put_command("k0", "v0")).await;
    let faults = Faults {
        disk_torn_pct: 100,
        ..Faults::default()
    };
    inject_faults(ep.as_mut(), faults).await;
    // the torn write fails, so this put gets no reply
    let req = ApiRequest::Req {
        id: 1,
        cmd: put_command("k1", "v1"),
    };
    assert!(ep.send_req(Some(&req)).unwrap());
    time::sleep(Duration::from_millis(200)).await;
    inject_faults(ep.as_mut(), Faults::default()).await;
    request(ep.as_mut(), 2, put_command("k2", "v2")).await;

    // the log recovers to exactly the good writes
    within(ep.leave(false)).await.unwrap();
    let crash = CtrlRequest::CrashServers {
        servers: HashSet::from([0]),
        power_loss: false,
    };
    ctrl(ep.as_mut(), crash).await;
    within(ep.connect()).await.unwrap();
    for (id, (key, value)) in [("k0", Some("v0")), ("k1", None), ("k2", Some("v2"))]
        .into_iter()
        .enumerate()
    {
        let get = Command::Get { key: key.into() };
        assert_eq!(
            request(ep.as_mut(), 3 + id as u64, get).await,
            CommandResult::Get {
                value: value.map(Into::into)
            },
            "key {key}"
        );
    }
}

#[tokio::test]
async fn bit_flips_fail_recovery() {
    let dir = TempDir::new().unwrap();
    let (mut server, mut ep) = start(dir.path(), "").await;
    let faults = Faults {
        disk_flip_pct: 100,
        ..Faults::default()
    };
    inject_faults(ep.as_mut(), faults).await;
    request(ep.as_mut(), 0, put_command("k0", "v0")).await;
    request(ep.as_mut(), 1, put_command("k1", "v1")).await;
    inject_faults(ep.as_mut(), Faults::default()).await;

    // recovery finds a corrupted record amid the log and refuses to go on
    within(ep.leave(false)).await.unwrap();
    let crash = CtrlRequest::CrashServers {
        servers: HashSet::from([0]),
        power_loss: false,
    };
    ep.ctrl_stub().send_req_insist(&crash).unwrap();
    assert!(!exits_ok(&mut server).await);
}
