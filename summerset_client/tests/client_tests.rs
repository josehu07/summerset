//! Integration tests of the client executable's utility modes, run against
//! in-process clusters.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, thread};

use summerset::{CtrlReply, CtrlRequest, GenericEndpoint, ReplicaId, ServerInfo, SmrProtocol};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::process::{self, Child};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::time;

const EXE: &str = env!("CARGO_BIN_EXE_summerset_client");

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

/// Prepares the client executable with given whitespace-separated args.
fn client(args: &str) -> process::Command {
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

/// Runs the client executable on a cluster in given utility mode, returning
/// whether it succeeded.
async fn run_client(manager: SocketAddr, utility: &str, params: &str) -> bool {
    let args = format!("-p RepNothing -m {manager} --threads 2 -u {utility} --params={params}");
    exits_ok(&mut spawn(&mut client(&args))).await
}

/// Queries the manager for current servers info.
async fn servers(ep: &mut dyn GenericEndpoint) -> HashMap<ReplicaId, ServerInfo> {
    ep.ctrl_stub()
        .send_req_insist(&CtrlRequest::QueryInfo)
        .unwrap();
    match within(ep.ctrl_stub().recv_reply()).await.unwrap() {
        CtrlReply::QueryInfo { servers_info, .. } => servers_info,
        reply => panic!("unexpected reply {reply:?}"),
    }
}

/// Starts an in-process cluster of one server, with the manager on the test's
/// runtime and the server on its own thread and runtime (a fresh one per
/// incarnation, so resets restart it). Returns the manager's client-facing
/// address and an endpoint connected to it, once the server has joined.
async fn start(dir: &Path) -> (SocketAddr, Box<dyn GenericEndpoint>) {
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

    let config = format!("backer_path='{}'", dir.join("wal").display());
    thread::spawn(move || {
        let (_tx_term, rx_term) = watch::channel(false);
        while Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut replica = SmrProtocol::RepNothing
                    .new_server_replica_setup(free_addr(), free_addr(), srv_addr, Some(&config))
                    .await?;
                replica.run(rx_term.clone()).await
            })
            .unwrap_or(false)
        {}
    });

    let mut ep = within(SmrProtocol::RepNothing.new_client_endpoint(cli_addr, None))
        .await
        .unwrap();
    within(async {
        while servers(ep.as_mut()).await.is_empty() {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    (cli_addr, ep)
}

#[tokio::test]
async fn rejects_bad_args() {
    let dir = TempDir::new().unwrap();
    let (manager, _ep) = start(dir.path()).await;
    assert!(!run_client(manager, "bogus", "").await);
    assert!(!run_client(manager, "bench", "bogus_param=1").await);
    let mut bad_protocol = spawn(&mut client("-p Unknown -m 127.0.0.1:52601 -u repl"));
    assert!(!exits_ok(&mut bad_protocol).await);
}

#[tokio::test]
async fn tester_reports_result() {
    let dir = TempDir::new().unwrap();
    let (manager, _ep) = start(dir.path()).await;
    assert!(run_client(manager, "tester", "test_name='primitive_ops'").await);
}

#[tokio::test]
async fn tester_rejects_unknown_test() {
    let dir = TempDir::new().unwrap();
    let (manager, _ep) = start(dir.path()).await;
    assert!(!run_client(manager, "tester", "test_name='bogus'").await);
}

#[tokio::test]
async fn mess_pauses_and_resumes() {
    let dir = TempDir::new().unwrap();
    let (manager, mut ep) = start(dir.path()).await;
    assert!(run_client(manager, "mess", "pause='0'").await);
    assert!(servers(ep.as_mut()).await[&0].is_paused);
    assert!(run_client(manager, "mess", "resume='0'").await);
    assert!(!servers(ep.as_mut()).await[&0].is_paused);
}

/// Runs a short benchmark, returning its output file's lines.
async fn bench_lines(extra: &str) -> Vec<String> {
    let dir = TempDir::new().unwrap();
    let (manager, _ep) = start(dir.path()).await;
    let output = dir.path().join("bench.out");
    let params = format!(
        "length_s=1+value_size='64'+output_path='{}'{extra}",
        output.display()
    );
    assert!(run_client(manager, "bench", &params).await);
    fs::read_to_string(output)
        .unwrap()
        .lines()
        .map(String::from)
        .collect()
}

#[tokio::test]
async fn bench_closed_loop_output() {
    let lines = bench_lines("").await;
    assert!(
        lines[0].contains("Tput (ops/s)") && lines.len() > 1,
        "{lines:?}"
    );
}

#[tokio::test]
async fn bench_open_loop_output() {
    let lines = bench_lines("+freq_target=200").await;
    assert!(
        lines[0].contains("Tput (ops/s)") && lines.len() > 1,
        "{lines:?}"
    );
}

#[tokio::test]
async fn repl_runs_commands() {
    let dir = TempDir::new().unwrap();
    let (manager, _ep) = start(dir.path()).await;
    let mut repl = spawn(
        client(&format!("-p RepNothing -m {manager} --threads 2 -u repl"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped()),
    );
    let mut stdin = repl.stdin.take().unwrap();
    within(stdin.write_all(b"put k v\nget k\nexit\n"))
        .await
        .unwrap();
    let out = within(repl.wait_with_output()).await.unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(out.status.success(), "{stdout}");
    assert!(stdout.contains(r#"Get { value: Some("v") }"#), "{stdout}");
}
