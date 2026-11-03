//! Integration tests of the generic framework over in-process clusters: cluster
//! management, client API, peer transport, durable storage, and control ops.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use summerset::{
    ApiReply, ApiRequest, Command, CommandResult, CtrlReply, CtrlRequest, GenericEndpoint,
    ReplicaId, ServerInfo, SmrProtocol,
};
use tempfile::TempDir;
use tokio::net::TcpSocket;
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::time;

/// Generous bound for operations that are expected to succeed.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Faster failure detection for leader-based protocols.
const FAST_HB: &str = "hb_hear_timeout_min=100\nhb_hear_timeout_max=200";

/// Returns an unused local address. Ports are taken in turn, from a random
/// start, below the OS ephemeral range, so none can be grabbed by other tests'
/// connections before the node under test binds it.
fn free_addr() -> SocketAddr {
    static START: LazyLock<u32> = LazyLock::new(|| {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        now.subsec_nanos() ^ std::process::id()
    });
    static NEXT: AtomicU32 = AtomicU32::new(0);
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

/// In-process cluster, with the manager on the test's runtime and each server
/// on its own thread and runtime; terminated when dropped.
struct Cluster {
    protocol: SmrProtocol,
    srv_addr: SocketAddr,
    cli_addr: SocketAddr,
    dir: TempDir,
    num_spawned: u8,
    tx_term: watch::Sender<bool>,
}

impl Cluster {
    /// Starts a manager for `population` servers, without any servers.
    async fn new(protocol: SmrProtocol, population: u8) -> Self {
        let (srv_addr, cli_addr) = (free_addr(), free_addr());
        let mut manager =
            within(protocol.new_cluster_manager_setup(srv_addr, cli_addr, population))
                .await
                .unwrap();
        let (tx_term, rx_term) = watch::channel(false);
        tokio::spawn(async move { manager.run(rx_term).await });
        Cluster {
            protocol,
            srv_addr,
            cli_addr,
            dir: TempDir::new().unwrap(),
            num_spawned: 0,
            tx_term,
        }
    }

    /// Starts a manager and `population` servers with given extra config,
    /// returning once all servers have joined.
    async fn start(protocol: SmrProtocol, population: u8, config: &str) -> Self {
        let mut cluster = Self::new(protocol, population).await;
        for _ in 0..population {
            cluster.spawn_server(config);
        }
        let mut ep = cluster.ctrl_client().await;
        wait_servers(ep.as_mut(), |s| s.len() == population as usize).await;
        cluster
    }

    /// Starts a server replica that registers with the manager. Like the
    /// server executable, each incarnation gets a fresh runtime so that reset
    /// requests can simulate crash-restarts.
    fn spawn_server(&mut self, config: &str) {
        self.num_spawned += 1;
        let path = |ext| {
            let file = format!("{}.{ext}", self.num_spawned);
            self.dir.path().join(file).display().to_string()
        };
        let snapshot = if matches!(
            self.protocol,
            SmrProtocol::RepNothing | SmrProtocol::SimplePush | SmrProtocol::ChainRep
        ) {
            String::new()
        } else {
            format!("\nsnapshot_path='{}'", path("snap"))
        };
        let config = format!("backer_path='{}'\n{config}{snapshot}", path("wal"));
        let (protocol, manager) = (self.protocol, self.srv_addr);
        let (api_addr, p2p_addr, rx_term) = (free_addr(), free_addr(), self.tx_term.subscribe());
        thread::spawn(move || {
            while Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let mut replica = protocol
                        .new_server_replica_setup(api_addr, p2p_addr, manager, Some(&config))
                        .await?;
                    replica.run(rx_term.clone()).await
                })
                .unwrap_or(false)
            {}
        });
    }

    /// Creates a client endpoint connected to the manager only.
    async fn ctrl_client(&self) -> Box<dyn GenericEndpoint> {
        within(self.protocol.new_client_endpoint(self.cli_addr, None))
            .await
            .unwrap()
    }

    /// Creates a client endpoint connected to the cluster.
    async fn client(&self) -> Box<dyn GenericEndpoint> {
        self.client_with(None).await
    }

    /// Creates a client endpoint with given protocol-specific config.
    async fn client_with(&self, config: Option<&str>) -> Box<dyn GenericEndpoint> {
        let mut ep = within(self.protocol.new_client_endpoint(self.cli_addr, config))
            .await
            .unwrap();
        within(ep.connect()).await.unwrap();
        ep
    }
}

/// Sends a control request to the manager and returns its reply.
async fn ctrl(ep: &mut dyn GenericEndpoint, req: CtrlRequest) -> CtrlReply {
    ep.ctrl_stub().send_req_insist(&req).unwrap();
    within(ep.ctrl_stub().recv_reply()).await.unwrap()
}

/// Queries the manager for current servers info.
async fn servers(ep: &mut dyn GenericEndpoint) -> HashMap<ReplicaId, ServerInfo> {
    match ctrl(ep, CtrlRequest::QueryInfo).await {
        CtrlReply::QueryInfo { servers_info, .. } => servers_info,
        reply => panic!("unexpected reply {reply:?}"),
    }
}

/// Polls the manager until servers info satisfies `pred`.
async fn wait_servers(
    ep: &mut dyn GenericEndpoint,
    pred: impl Fn(&HashMap<ReplicaId, ServerInfo>) -> bool,
) -> HashMap<ReplicaId, ServerInfo> {
    within(async {
        loop {
            let info = servers(ep).await;
            if pred(&info) {
                return info;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

/// Issues a command, following redirects; returns `None` on timeout.
async fn call(
    ep: &mut dyn GenericEndpoint,
    cmd: Command,
    timeout: Duration,
) -> Option<CommandResult> {
    static REQ_ID: AtomicU64 = AtomicU64::new(0);
    let id = REQ_ID.fetch_add(1, Ordering::Relaxed);
    time::timeout(timeout, async {
        loop {
            let req = ApiRequest::Req {
                id,
                cmd: cmd.clone(),
            };
            let mut sent = ep.send_req(Some(&req)).unwrap();
            while !sent {
                // yield so the runtime can poll socket writability
                tokio::task::yield_now().await;
                sent = ep.send_req(None).unwrap();
            }
            loop {
                if let ApiReply::Reply {
                    id: reply_id,
                    result,
                    ..
                } = ep.recv_reply().await.unwrap()
                    && reply_id == id
                {
                    match result {
                        Some(result) => return result,
                        None => break, // redirected or failed: retry
                    }
                }
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}

/// Puts a value, returning the old one.
async fn put(ep: &mut dyn GenericEndpoint, key: &str, value: &str) -> Option<String> {
    let cmd = Command::Put {
        key: key.into(),
        value: value.into(),
    };
    match call(ep, cmd, TIMEOUT).await {
        Some(CommandResult::Put { old_value }) => old_value,
        result => panic!("unexpected put result {result:?}"),
    }
}

/// Gets a value.
async fn get(ep: &mut dyn GenericEndpoint, key: &str) -> Option<String> {
    match call(ep, Command::Get { key: key.into() }, TIMEOUT).await {
        Some(CommandResult::Get { value }) => value,
        result => panic!("unexpected get result {result:?}"),
    }
}

#[tokio::test]
async fn bootstrap_assigns_ids() {
    let c = Cluster::start(SmrProtocol::RepNothing, 3, "").await;
    let mut ep = c.client().await;
    let info = servers(ep.as_mut()).await;
    assert_eq!(
        info.keys().copied().collect::<BTreeSet<_>>(),
        (0..3).collect()
    );
    assert!(info.values().all(|s| !s.is_paused && !s.is_leader));
    let addrs: HashSet<_> = info
        .values()
        .flat_map(|s| [s.api_addr, s.p2p_addr])
        .collect();
    assert_eq!(addrs.len(), 6);
}

#[tokio::test]
async fn rejects_extra_server() {
    let mut c = Cluster::new(SmrProtocol::RepNothing, 1).await;
    c.spawn_server("");
    c.spawn_server("");
    let mut ep = c.ctrl_client().await;
    wait_servers(ep.as_mut(), |s| s.len() == 1).await;
    time::sleep(Duration::from_millis(100)).await;
    assert_eq!(servers(ep.as_mut()).await.len(), 1);
}

#[tokio::test]
async fn put_get_roundtrip() {
    let c = Cluster::start(SmrProtocol::RepNothing, 1, "").await;
    let mut ep = c.client().await;
    assert_eq!(get(ep.as_mut(), "k").await, None);
    assert_eq!(put(ep.as_mut(), "k", "v1").await, None);
    assert_eq!(put(ep.as_mut(), "k", "v2").await.as_deref(), Some("v1"));
    assert_eq!(get(ep.as_mut(), "k").await.as_deref(), Some("v2"));
}

#[tokio::test]
async fn client_reconnects() {
    let c = Cluster::start(SmrProtocol::RepNothing, 1, "").await;
    let mut ep = c.client().await;
    put(ep.as_mut(), "k", "v").await;
    for _ in 0..3 {
        within(ep.leave(false)).await.unwrap();
        within(ep.connect()).await.unwrap();
        assert_eq!(get(ep.as_mut(), "k").await.as_deref(), Some("v"));
    }
    within(ep.leave(true)).await.unwrap();
}

#[tokio::test]
async fn concurrent_clients() {
    let c = Cluster::start(SmrProtocol::RepNothing, 1, "").await;
    let mut eps = Vec::new();
    for _ in 0..8 {
        eps.push(c.client().await);
    }
    assert_eq!(
        eps.iter().map(|ep| ep.id()).collect::<HashSet<_>>().len(),
        8
    );
    futures::future::join_all(eps.iter_mut().enumerate().map(|(i, ep)| async move {
        for j in 0..20 {
            put(ep.as_mut(), &format!("k{i}"), &format!("v{j}")).await;
            assert_eq!(
                get(ep.as_mut(), &format!("k{i}")).await,
                Some(format!("v{j}"))
            );
        }
    }))
    .await;
}

#[tokio::test]
async fn large_value_roundtrip() {
    // larger than wincode's default 4 MiB preallocation limit, through the
    // client API, peer pushes, and durable logging
    let c = Cluster::start(SmrProtocol::SimplePush, 3, "").await;
    let mut ep = c.client().await;
    let value = "x".repeat(5 << 20);
    put(ep.as_mut(), "k", &value).await;
    assert_eq!(get(ep.as_mut(), "k").await, Some(value));
}

#[tokio::test]
async fn pause_blocks_requests() {
    let c = Cluster::start(SmrProtocol::RepNothing, 1, "").await;
    let mut ep = c.client().await;
    let s = HashSet::from([0]);
    ctrl(
        ep.as_mut(),
        CtrlRequest::PauseServers { servers: s.clone() },
    )
    .await;
    assert!(servers(ep.as_mut()).await[&0].is_paused);
    let get_k = || Command::Get { key: "k".into() };
    assert!(
        call(ep.as_mut(), get_k(), Duration::from_millis(200))
            .await
            .is_none()
    );
    ctrl(ep.as_mut(), CtrlRequest::ResumeServers { servers: s }).await;
    assert!(!servers(ep.as_mut()).await[&0].is_paused);
    assert_eq!(get(ep.as_mut(), "k").await, None);
}

/// Puts a value, resets the only server, and returns the value read back.
async fn put_reset_get(durable: bool) -> Option<String> {
    let c = Cluster::start(SmrProtocol::RepNothing, 1, "").await;
    let mut ep = c.client().await;
    put(ep.as_mut(), "k", "v").await;
    within(ep.leave(false)).await.unwrap();
    let reset = CtrlRequest::ResetServers {
        servers: HashSet::from([0]),
        durable,
    };
    assert!(matches!(
        ctrl(ep.as_mut(), reset).await,
        CtrlReply::ResetServers { .. }
    ));
    within(ep.connect()).await.unwrap();
    get(ep.as_mut(), "k").await
}

#[tokio::test]
async fn reset_durable_keeps_data() {
    assert_eq!(put_reset_get(true).await.as_deref(), Some("v"));
}

#[tokio::test]
async fn reset_fresh_drops_data() {
    assert_eq!(put_reset_get(false).await, None);
}

/// Resets given servers (all if empty) of a `SimplePush` cluster, whose puts
/// complete only after pushing to all peers, then puts again.
async fn put_after_reset(servers: HashSet<ReplicaId>) {
    let c = Cluster::start(SmrProtocol::SimplePush, 3, "").await;
    let mut ep = c.client().await;
    put(ep.as_mut(), "k", "v1").await;
    within(ep.leave(false)).await.unwrap();
    let reset = CtrlRequest::ResetServers {
        servers,
        durable: true,
    };
    ctrl(ep.as_mut(), reset).await;
    within(ep.connect()).await.unwrap();
    // restarted servers reconnect to peers right after rejoining, and pushes
    // sent before that are lost; retry until they are back in the mesh
    let put_v2 = || Command::Put {
        key: "k".into(),
        value: "v2".into(),
    };
    within(async {
        while call(ep.as_mut(), put_v2(), Duration::from_millis(100))
            .await
            .is_none()
        {}
    })
    .await;
    assert_eq!(get(ep.as_mut(), "k").await.as_deref(), Some("v2"));
}

#[tokio::test]
async fn peers_reconnect_after_reset_one() {
    put_after_reset(HashSet::from([1])).await;
}

#[tokio::test]
async fn peers_reconnect_after_reset_all() {
    put_after_reset(HashSet::new()).await;
}

#[tokio::test]
async fn leader_redirects_client() {
    let c = Cluster::start(SmrProtocol::MultiPaxos, 3, FAST_HB).await;
    let mut ep = c.client().await;
    let info = wait_servers(ep.as_mut(), |s| {
        s.values().filter(|s| s.is_leader).count() == 1
    })
    .await;
    let follower = info
        .iter()
        .find(|(_, s)| !s.is_leader)
        .map(|(&id, _)| id)
        .unwrap();
    let mut ep = c
        .client_with(Some(&format!("init_server_id={follower}")))
        .await;
    assert_eq!(put(ep.as_mut(), "k", "v").await, None);
    assert_eq!(get(ep.as_mut(), "k").await.as_deref(), Some("v"));
}

#[tokio::test]
async fn snapshot_advances_start() {
    let c = Cluster::start(SmrProtocol::MultiPaxos, 3, FAST_HB).await;
    let mut ep = c.client().await;
    wait_servers(ep.as_mut(), |s| s.values().any(|s| s.is_leader)).await;
    for i in 0..5 {
        put(ep.as_mut(), &format!("k{i}"), "v").await;
    }
    // servers can only snapshot what all have executed, learnt via heartbeats
    let snapshot = || CtrlRequest::TakeSnapshot {
        servers: HashSet::from([0, 1, 2]),
    };
    within(async {
        while !matches!(ctrl(ep.as_mut(), snapshot()).await,
            CtrlReply::TakeSnapshot { snapshot_up_to } if snapshot_up_to.values().all(|&s| s > 0))
        {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    wait_servers(ep.as_mut(), |s| s.values().all(|s| s.start_slot > 0)).await;
    assert_eq!(get(ep.as_mut(), "k4").await.as_deref(), Some("v"));
}
