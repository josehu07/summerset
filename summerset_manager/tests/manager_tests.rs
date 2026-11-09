//! Integration tests of the cluster manager executable, run against in-process
//! server replicas.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use summerset::{CtrlReply, CtrlRequest, GenericEndpoint, ReplicaId, ServerInfo, SmrProtocol};
use tempfile::TempDir;
use tokio::net::TcpSocket;
use tokio::process::{self, Child};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::time;

const EXE: &str = env!("CARGO_BIN_EXE_summerset_manager");

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

/// Prepares the manager executable with given whitespace-separated args.
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

/// Spawns the manager executable for `population` servers, returning it with
/// a client endpoint connected to it.
async fn start(population: u8) -> (Child, SocketAddr, Box<dyn GenericEndpoint>) {
    let (srv_addr, cli_addr) = (free_addr(), free_addr());
    let manager = spawn(&mut exe(&format!(
        "-p RepNothing -b 127.0.0.1 -s {} -c {} -n {population} --threads 2",
        srv_addr.port(),
        cli_addr.port()
    )));
    let ep = within(SmrProtocol::RepNothing.new_client_endpoint(cli_addr, None))
        .await
        .unwrap();
    (manager, srv_addr, ep)
}

/// Starts an in-process server replica that registers with the manager, on its
/// own thread and runtime (a fresh one per incarnation, so resets restart it).
fn spawn_server(manager: SocketAddr, wal: &Path) {
    let config = format!("backer_path='{}'", wal.display());
    thread::spawn(move || {
        let (_tx_term, rx_term) = watch::channel(false);
        while Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut replica = SmrProtocol::RepNothing
                    .new_server_replica_setup(
                        free_addr(),
                        free_addr(),
                        manager,
                        None,
                        Some(&config),
                    )
                    .await?;
                replica.run(rx_term.clone()).await
            })
            .unwrap_or(false)
        {}
    });
}

/// Sends a control request to the manager and returns its reply.
async fn ctrl(ep: &mut dyn GenericEndpoint, req: CtrlRequest) -> CtrlReply {
    ep.ctrl_stub().send_req_insist(&req).await.unwrap();
    within(ep.ctrl_stub().recv_reply()).await.unwrap()
}

/// Queries the manager for current servers info.
async fn servers(ep: &mut dyn GenericEndpoint) -> HashMap<ReplicaId, ServerInfo> {
    match ctrl(ep, CtrlRequest::QueryInfo).await {
        CtrlReply::QueryInfo { servers_info, .. } => servers_info,
        reply => panic!("unexpected reply {reply:?}"),
    }
}

#[tokio::test]
async fn rejects_bad_args() {
    for args in [
        "-p Unknown",
        "-p RepNothing -n 0",
        "-p RepNothing -s 1000",
        "-p RepNothing -s 52600 -c 52600",
    ] {
        assert!(
            !exits_ok(&mut spawn(&mut exe(args))).await,
            "accepted args {args:?}"
        );
    }
}

#[tokio::test]
async fn control_roundtrip() {
    let dir = TempDir::new().unwrap();
    let (_manager, srv_addr, mut ep) = start(2).await;
    spawn_server(srv_addr, &dir.path().join("0.wal"));
    spawn_server(srv_addr, &dir.path().join("1.wal"));
    within(async {
        while servers(ep.as_mut()).await.len() < 2 {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        servers(ep.as_mut())
            .await
            .into_keys()
            .collect::<BTreeSet<_>>(),
        (0..2).collect()
    );

    let s = HashSet::from([0]);
    let reply = ctrl(
        ep.as_mut(),
        CtrlRequest::PauseServers { servers: s.clone() },
    )
    .await;
    assert!(matches!(reply, CtrlReply::PauseServers { servers } if servers == s));
    assert!(servers(ep.as_mut()).await[&0].is_paused);
    ctrl(
        ep.as_mut(),
        CtrlRequest::ResumeServers { servers: s.clone() },
    )
    .await;
    assert!(!servers(ep.as_mut()).await[&0].is_paused);

    let reset = CtrlRequest::ResetServers {
        servers: s.clone(),
        durable: true,
    };
    let reply = ctrl(ep.as_mut(), reset).await;
    assert!(matches!(reply, CtrlReply::ResetServers { servers } if servers == s));
    assert_eq!(servers(ep.as_mut()).await.len(), 2);
}

#[tokio::test]
async fn sigterm_exits_cleanly() {
    let (mut manager, ..) = start(1).await;
    let pid = manager.id().unwrap().to_string();
    let mut kill = spawn(process::Command::new("kill").args(["-TERM", &pid]));
    assert!(exits_ok(&mut kill).await && exits_ok(&mut manager).await);
}

#[tokio::test]
async fn exits_with_watched_ancestor() {
    // as when launched through `sudo`, the watched process is not its parent
    let mut ancestor = spawn(process::Command::new("sleep").arg("30").kill_on_drop(true));
    let watched = ancestor.id().unwrap().to_string();
    let mut manager = spawn(
        exe(&format!(
            "-p RepNothing -b 127.0.0.1 -s {} -c {} -n 1 --threads 2",
            free_addr().port(),
            free_addr().port()
        ))
        .env("SUMMERSET_EXIT_WITH_PARENT", watched),
    );
    time::sleep(Duration::from_millis(500)).await;
    assert!(manager.try_wait().unwrap().is_none(), "exited too early");
    within(ancestor.kill()).await.unwrap();
    assert!(!exits_ok(&mut manager).await);
}
