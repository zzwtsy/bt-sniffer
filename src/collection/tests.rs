//! 通过本机 UDP/TCP 与临时数据库验证发现到入库的闭环，以及故障、取消和恢复。
use super::{
    worker::{Outcome, run_job},
    *,
};
use crate::app::session::FaultLog;
use crate::app::session::FaultReporter;
use crate::app::session::Session;
use crate::app::session::SessionFault;
use crate::collection::peer::VerifiedMetadata;
use crate::collection::peer::wire as peer_wire;
use crate::collection::peer::wire::PeerId;
use crate::dht::NodeId;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::dispatcher::RemoteNode;
use crate::dht::krpc::CompactNodesV4;
use crate::dht::krpc::CompactNodesV6;
use crate::dht::krpc::CompactPeerAddress;
use crate::dht::krpc::InfoHashSamples;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::QueryArgs;
use crate::dht::krpc::QueryMethod;
use crate::dht::krpc::ResponseArgs;
use crate::dht::krpc::Token;
use crate::dht::routing::AddressFamily;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;
use crate::info_hash::InfoHashV1;
use crate::storage::StorageConfig;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_util::codec::LengthDelimitedCodec;

fn now() -> Result<i64, StorageError> {
    Ok(unix_millis(std::time::SystemTime::now())?)
}

const INFO: &[u8] = b"d4:name4:test6:pieces0:e";
fn hash() -> InfoHashV1 {
    InfoHashV1(Sha1::digest(INFO).into())
}
fn config(dir: &std::path::Path) -> Config {
    Config {
        metadata: MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        },
        concurrency: 2,
        max_active: 16,
        sample_backpressure: SampleBackpressure::Capacity,
        state_max_bytes: 1024 * 1024 * 1024,
        directory: dir.into(),
        policy: AddressPolicy::LocalUnicast,
    }
}
async fn udp(family: AddressFamily) -> UdpTransport {
    UdpTransport::bind(
        if family == AddressFamily::Ipv6 {
            "[::1]:0"
        } else {
            "127.0.0.1:0"
        },
        Default::default(),
    )
    .await
    .unwrap()
}
async fn tcp(family: AddressFamily) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    tcp_info(family, INFO.to_vec()).await
}
async fn tcp_info(
    family: AddressFamily,
    info: Vec<u8>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    tcp_info_with_order(family, info, false).await
}
async fn tcp_info_with_order(
    family: AddressFamily,
    info: Vec<u8>,
    compatible: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let info_hash = InfoHashV1(Sha1::digest(&info).into());
    let listener = TcpListener::bind(if family == AddressFamily::Ipv6 {
        "[::1]:0"
    } else {
        "127.0.0.1:0"
    })
    .await
    .unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut hello = [0; 68];
        socket.read_exact(&mut hello).await.unwrap();
        peer_wire::parse_handshake(&hello, info_hash).unwrap();
        socket
            .write_all(&peer_wire::handshake(info_hash, PeerId([7; 20])))
            .await
            .unwrap();
        let mut stream = LengthDelimitedCodec::builder().new_framed(socket);
        stream.next().await.unwrap().unwrap();
        stream
            .send(peer_wire::extended(
                0,
                if compatible {
                    format!("d13:metadata_sizei{}e1:md11:ut_metadatai7eee", info.len())
                } else {
                    format!("d1:md11:ut_metadatai7ee13:metadata_sizei{}ee", info.len())
                }
                .as_bytes(),
            ))
            .await
            .unwrap();
        let request = stream.next().await.unwrap().unwrap();
        assert_eq!(&request[..2], &[20, 7]);
        let mut body =
            format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
        body.extend_from_slice(&info);
        stream.send(peer_wire::extended(1, &body)).await.unwrap();
    });
    (address, task)
}
fn response(
    t: ByteBuf,
    family: AddressFamily,
    peer: Option<SocketAddr>,
    method: QueryMethod,
) -> KrpcMessage {
    KrpcMessage {
        t,
        y: MessageType::Response,
        q: None,
        a: None,
        e: None,
        ro: None,
        r: Some(ResponseArgs {
            id: NodeId([8; 20]),
            token: Some(Token(ByteBuf::from(b"token".to_vec()))),
            nodes: (family == AddressFamily::Ipv4).then(|| CompactNodesV4(vec![])),
            nodes6: (family == AddressFamily::Ipv6).then(|| CompactNodesV6(vec![])),
            values: peer.map(|a| {
                vec![match a {
                    SocketAddr::V4(a) => CompactPeerAddress::V4(a),
                    SocketAddr::V6(a) => CompactPeerAddress::V6(a),
                }]
            }),
            samples: (method == QueryMethod::SampleInfohashes)
                .then(|| InfoHashSamples(vec![hash()])),
            interval: (method == QueryMethod::SampleInfohashes).then_some(60),
            num: (method == QueryMethod::SampleInfohashes).then_some(1),
        }),
    }
}
/// 会话拥有节点与数据库；测试保留 handle 发命令，address 供模拟远端访问。
struct Fixture {
    session: Session,
    handle: DhtHandle,
    address: SocketAddr,
}

async fn fixture(dir: &std::path::Path, family: AddressFamily) -> Fixture {
    let mut session = Session::open(StorageConfig::new(dir)).await.unwrap();
    let transport = udp(family).await;
    let address = transport.local_addr().unwrap();
    let mut cfg = DhtDispatcherConfig::default();
    cfg.maintenance.enabled = false;
    cfg.peer_store.address_policy = AddressPolicy::LocalUnicast;
    let handle = session
        .add_node(
            "test",
            transport,
            TransactionManager::new(Duration::from_secs(1), 32),
            cfg,
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    Fixture {
        session,
        handle,
        address,
    }
}
async fn await_metadata(store: &CollectionStore) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(bytes) = store.metadata(hash()).await.unwrap() {
                assert_eq!(bytes, INFO);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("闭环必须自动入库");
    assert_eq!(store.fetch_stats().await.unwrap().succeeded, 1);
}
/// 两种入口共用同一采集闭环，区别只在 hash 从采样产生还是从磁盘恢复。
#[derive(Clone, Copy, PartialEq, Eq)]
enum DiscoverySource {
    Sampling,
    BackloggedSampling,
    History,
}

async fn sample_or_history(family: AddressFamily, source: DiscoverySource) {
    // 准备：使用独立目录和本机节点，模拟 peer 返回固定的原始 info 字节。
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture(dir.path(), family).await;
    let store = session.test_store();
    if source == DiscoverySource::History {
        store
            .save_hashes(&[hash(), hash()], now().unwrap())
            .await
            .unwrap();
    }
    if source == DiscoverySource::BackloggedSampling {
        let old: Vec<_> = (10..74).map(|n| InfoHashV1([n; 20])).collect();
        store.save_hashes(&old, 0).await.unwrap();
    }
    let (peer, tcp_task) = tcp(family).await;
    let server = udp(family).await;
    let remote = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        loop {
            let request = server.recv().await.unwrap();
            let method = request.message.q.as_ref().unwrap();
            if *method == QueryMethod::GetPeers && source != DiscoverySource::BackloggedSampling {
                assert_eq!(request.message.a.as_ref().unwrap().info_hash, Some(hash()));
                assert!(request.message.a.as_ref().unwrap().target.is_none());
            }
            let wanted = request.message.a.as_ref().and_then(|a| a.info_hash) == Some(hash());
            let message = response(
                request.message.t,
                family,
                (*method == QueryMethod::GetPeers && wanted).then_some(peer),
                method.clone(),
            );
            server.send_to(request.source, &message).await.unwrap();
        }
    });
    handle
        .ping(RemoteNode {
            address: remote,
            expected_id: Some(NodeId([8; 20])),
        })
        .await
        .unwrap();
    let mut fetch_config = config(dir.path());
    if source == DiscoverySource::BackloggedSampling {
        fetch_config.max_active = 10000;
        fetch_config.sample_backpressure = SampleBackpressure::Freshness;
    }
    session.start_fetch(fetch_config).await.unwrap();
    if source != DiscoverySource::History {
        session
            .start_sampling(
                0,
                crate::dht::dispatcher::SamplerConfig {
                    address_policy: AddressPolicy::LocalUnicast,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    // 断言：观察数据库和任务状态，不能只凭网络请求结束认定完成。
    await_metadata(&store).await;
    // 收尾：先确认模拟 peer 完成，再关闭会话，最后停止持续服务的 UDP 任务。
    tcp_task.await.unwrap();
    session.shutdown().await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    // 重启只保留一份结果，不重新领取成功任务。
    let Fixture {
        mut session,
        handle: _,
        address: _,
    } = fixture(dir.path(), family).await;
    session.start_fetch(config(dir.path())).await.unwrap();
    if source != DiscoverySource::BackloggedSampling {
        assert_eq!(session.test_store().active_jobs().await.unwrap(), 0);
    }
    assert_eq!(
        session
            .test_store()
            .metadata(hash())
            .await
            .unwrap()
            .unwrap(),
        INFO
    );
    session.shutdown().await.unwrap();
}
/// 存在首次发现时间为 0 的历史积压时，主动采样仍能发现并提交新的 metadata。
#[tokio::test]
async fn historical_backlog_keeps_active_discovery_and_new_completion() {
    sample_or_history(AddressFamily::Ipv4, DiscoverySource::BackloggedSampling).await;
}
/// 通过本机 IPv4 完成主动采样、查找 peer、下载校验和入库，重启不重复领取成功任务。
#[tokio::test]
async fn sample_to_metadata_v4() {
    sample_or_history(AddressFamily::Ipv4, DiscoverySource::Sampling).await;
}
/// 通过本机 IPv6 消费历史 hash，完成下载与入库，验证历史入口不依赖主动采样。
#[tokio::test]
async fn history_to_metadata_v6() {
    sample_or_history(AddressFamily::Ipv6, DiscoverySource::History).await;
}

fn announce_query(token: Option<Token>, port: u16) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(b"announce-test".to_vec()),
        y: MessageType::Query,
        q: Some(if token.is_some() {
            QueryMethod::AnnouncePeer
        } else {
            QueryMethod::GetPeers
        }),
        a: Some(QueryArgs {
            id: NodeId([4; 20]),
            target: None,
            info_hash: Some(hash()),
            port: token.as_ref().map(|_| port),
            token,
            implied_port: None,
            want: vec![],
        }),
        r: None,
        e: None,
        ro: Some(1),
    }
}
/// 合法宣布可直接触发下载；非法 token 不能创建任务，即使宣布者不在路由表中。
#[tokio::test]
async fn valid_announce_downloads_without_routing_and_invalid_token_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle: _,
        address,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    session.start_fetch(config(dir.path())).await.unwrap();
    let (peer, tcp_task) = tcp(AddressFamily::Ipv4).await;
    let sender = udp(AddressFamily::Ipv4).await;
    sender
        .send_to(
            address,
            &announce_query(Some(Token(ByteBuf::from(b"invalid".to_vec()))), peer.port()),
        )
        .await
        .unwrap();
    assert_eq!(sender.recv().await.unwrap().message.y, MessageType::Error);
    assert_eq!(store.active_jobs().await.unwrap(), 0);
    sender
        .send_to(address, &announce_query(None, 0))
        .await
        .unwrap();
    let token = sender
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .token
        .unwrap();
    for _ in 0..3 {
        sender
            .send_to(address, &announce_query(Some(token.clone()), peer.port()))
            .await
            .unwrap();
        sender.recv().await.unwrap();
    }
    await_metadata(&store).await;
    tcp_task.await.unwrap();
    session.shutdown().await.unwrap();
}

/// 取消查找后立即回收 transaction，不等待远端响应超时才释放名额。
#[tokio::test]
async fn cancelled_get_peers_releases_transactions_without_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let server = udp(AddressFamily::Ipv4).await;
    let remote = RemoteNode {
        address: server.local_addr().unwrap(),
        expected_id: None,
    };
    let h = handle.clone();
    let task = tokio::spawn(async move { h.get_peers(remote, hash()).await });
    server.recv().await.unwrap();
    assert_eq!(handle.status().await.unwrap().pending, 1);
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_millis(200), async {
        while handle.status().await.unwrap().pending != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    session.shutdown().await.unwrap();
}

/// 采集容量保护触发时仍能服务 DHT，已接纳任务继续保留以便恢复。
#[tokio::test]
async fn capacity_protection_keeps_dht_live_and_preserves_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    store.save_hashes(&[hash()], now().unwrap()).await.unwrap();
    let mut cfg = config(dir.path());
    cfg.state_max_bytes = 64 * 1024 * 1024;
    session.start_fetch(cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(handle.status().await.is_ok());
    assert_eq!(store.fetch_stats().await.unwrap().metadata_count, 0);
    session.shutdown().await.unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    storage.handle.enable_fetch(1);
    storage.handle.backfill_jobs(now().unwrap()).await.unwrap();
    assert!(
        storage
            .handle
            .claim_job(now().unwrap())
            .await
            .unwrap()
            .is_some()
    );
    storage.shutdown().await.unwrap();
}

/// 容量保护完成暂停后输出可消费的状态事件，字段值与实际资源状态一致。
#[tokio::test]
async fn capacity_pause_event_reports_state_and_byte_limit() {
    use tracing::instrument::WithSubscriber;
    let directory = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(directory.path()))
            .await
            .unwrap();
    let logs = tempfile::NamedTempFile::new().unwrap();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::WARN)
        .with_writer(logs.reopen().unwrap())
        .finish();
    let mut config = config(directory.path());
    config.state_max_bytes = 64 * 1024 * 1024;
    let (notifications, _) = watch::channel(());
    let mut collector = Collector::new(
        storage.handle.clone(),
        Vec::new(),
        config,
        Clock::default(),
        CancellationToken::new(),
        notifications,
        Box::new(|_| {}),
    )
    .await
    .unwrap();
    let cancel = CancellationToken::new();
    let mut bytes = 0;
    collector
        .check_storage_capacity(&cancel, &mut bytes)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert!(cancel.is_cancelled());
    assert!(collector.storage_paused);
    let text = std::fs::read_to_string(logs.path()).unwrap();
    let events: Vec<serde_json::Value> = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["level"], "WARN");
    let fields = &events[0]["fields"];
    assert_eq!(fields["event"], "storage_capacity_paused");
    assert_eq!(fields["schema_version"].as_u64(), Some(1));
    assert_eq!(fields["phase"], "running");
    assert_eq!(fields["action"], "pause_collection");
    assert_eq!(fields["state_bytes"].as_u64(), Some(bytes));
    assert_eq!(fields["limit_bytes"].as_u64(), Some(64 * 1024 * 1024));
    storage.shutdown().await.unwrap();
}

/// 同 IP 的第二次许可申请等待首个持有者释放；不建立 TCP 连接。
#[tokio::test(start_paused = true)]
async fn rpc_spacing_and_tcp_ip_exclusion() {
    let network = Arc::new(super::tcp_limits::TcpLimits::default());
    let ip = "127.0.0.1".parse().unwrap();
    let connection_permit = network.acquire_for_ip(ip).await;
    let n = network.clone();
    let waiting = tokio::spawn(async move { n.acquire_for_ip(ip).await });
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(!waiting.is_finished());
    drop(connection_permit);
    drop(waiting.await.unwrap());
    assert_eq!(network.tracked_tcp_ips(), 0);
}

/// 显式运行的真实时间耐久测试；混合合法宣布、重复 hash、无效 token 和 DHT 服务查询。
#[tokio::test]
#[ignore = "需要 30 分钟真实时间和本机 socket"]
async fn sustained_mixed_loopback_30_minutes() {
    let signal = crate::app::shutdown_signal().expect("注册验收退出信号");
    let dir = tempfile::Builder::new()
        .prefix("bt-sniffer-local-acceptance-")
        .tempdir()
        .unwrap()
        .keep();
    let mut report = crate::acceptance::Report::new(
        "local-thirty-minutes",
        &dir,
        serde_json::json!({"planned_seconds":1800,"workers":2,"max_active":16,"rss_growth_limit_bytes":33554432,"dht_defaults":true}),
    );
    let Fixture {
        mut session,
        handle,
        address,
    } = fixture(&dir, AddressFamily::Ipv4).await;
    report.running();
    let budget = session.test_budget();
    let store = session.test_store();
    session.start_fetch(config(&dir)).await.unwrap();
    let sender = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let flood = UdpTransport::bind("127.0.0.3:0", Default::default())
        .await
        .unwrap();
    let pressure = tokio::spawn(async move {
        loop {
            for _ in 0..20 {
                flood
                    .send_to(address, &announce_query(None, 0))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let start = tokio::time::Instant::now();
    let mut rounds = 0u64;
    let mut memory_floor = None;
    let mut peak_rss = 0;
    let mut servers = Vec::new();
    let mut completed = false;
    let work = async {
        let mut previous = InfoHashV1([0; 20]);
        while start.elapsed() < Duration::from_secs(1800) {
            // 新鲜合法任务、实际失败 peer、重复宣布和无效 token 并存。
            sender
                .send_to(address, &announce_query(None, 0))
                .await
                .unwrap();
            let token = tokio::time::timeout(Duration::from_secs(2), sender.recv())
                .await
                .unwrap()
                .unwrap()
                .message
                .r
                .unwrap()
                .token
                .unwrap();
            let mut target = InfoHashV1([0; 20]);
            target.0[..8].copy_from_slice(&rounds.to_be_bytes());
            let mut port = 1;
            if rounds.is_multiple_of(10) {
                let name = format!("soak-{rounds}");
                let info = format!("d4:name{}:{}6:pieces0:e", name.len(), name).into_bytes();
                target = InfoHashV1(Sha1::digest(&info).into());
                let (peer, task) = tcp_info(AddressFamily::Ipv4, info).await;
                port = peer.port();
                servers.push(task);
            }
            if rounds % 5 == 1 {
                target = previous;
            } else {
                previous = target;
            }
            let mut query = announce_query(Some(token), port);
            query.a.as_mut().unwrap().info_hash = Some(target);
            // 宣布地址由真实 UDP 来源决定，成功服务绑定同一 loopback 地址。
            if rounds % 3 == 2 {
                query.a.as_mut().unwrap().token = Some(Token(ByteBuf::from(b"invalid".to_vec())));
            }
            sender.send_to(address, &query).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), sender.recv())
                .await
                .unwrap()
                .unwrap();
            if rounds.is_multiple_of(10) {
                // 饱和时可以拒绝新任务，不为被拒绝的测试 peer 留监听器。
                while servers.last().is_some_and(|task| task.is_finished()) {
                    servers.pop().unwrap().await.unwrap();
                }
                if servers.len() > 2 {
                    let task = servers.remove(0);
                    task.abort();
                    let _ = task.await;
                }
            }
            if rounds.is_multiple_of(20) {
                let stats = store.fetch_stats().await.unwrap();
                assert!(stats.active() <= 16 && stats.running <= 2);
                assert!(
                    tokio::time::timeout(Duration::from_secs(2), handle.status())
                        .await
                        .unwrap()
                        .unwrap()
                        .pending
                        <= 32
                );
                #[cfg(target_os = "linux")]
                {
                    let status = std::fs::read_to_string("/proc/self/status").unwrap();
                    let rss = status
                        .lines()
                        .find(|s| s.starts_with("VmRSS:"))
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .parse::<u64>()
                        .unwrap();
                    peak_rss = peak_rss.max(rss);
                    if start.elapsed() >= Duration::from_secs(60) {
                        let floor = memory_floor.get_or_insert(rss);
                        *floor = (*floor).min(rss);
                        assert!(rss <= *floor + 32 * 1024, "预热后 RSS 增量超过 32 MiB");
                    }
                }
                eprintln!(
                    "local soak rounds={rounds} elapsed={}s stats={stats:?}",
                    start.elapsed().as_secs()
                );
            }
            rounds += 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let result = std::panic::AssertUnwindSafe(async {
        tokio::select! { biased; _=signal=>{}, _=work=>completed=true }
    })
    .catch_unwind()
    .await;
    pressure.abort();
    let _ = pressure.await;
    for task in servers {
        task.abort();
        let _ = task.await;
    }
    let stats = store.fetch_stats().await.unwrap();
    let shutdown = session.shutdown().await;
    report.value["statistics"] = serde_json::json!({"rounds":rounds,"peak_rss_kib":peak_rss,"rss_floor_kib":memory_floor,"storage":stats,"dht":budget.snapshot()});
    report.value["families"] =
        serde_json::json!({"ipv4":"loopback exercised","ipv6":"not exercised by this scenario"});
    report.value["verification"] = serde_json::json!([
        "active and running bounds",
        "healthy control within 2 seconds",
        "rss growth after 60 second warmup",
        "normal shutdown"
    ]);
    report.finish(
        completed || result.is_err(),
        result.is_ok() && shutdown.is_ok() && stats.metadata_count > 0,
    );
    shutdown.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert!(completed, "本机验收已提前停止，未完成 30 分钟");
    assert!(stats.metadata_count > 0, "本机验收需要成功 metadata");
}

async fn verified() -> VerifiedMetadata {
    let (peer, task) = tcp(AddressFamily::Ipv4).await;
    let metadata = PeerClient::new(MetadataConfig {
        address_policy: AddressPolicy::LocalUnicast,
        ..Default::default()
    })
    .unwrap()
    .fetch_one(
        hash(),
        peer,
        &CancellationToken::new(),
        super::peer::PeerContext::default(),
    )
    .await
    .unwrap();
    task.await.unwrap();
    metadata
}

/// 完成事务失败不能只保存 metadata；旧 generation 的成功结果同样不能写入。
#[tokio::test]
async fn completion_rollback_and_stale_success_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let store = &storage.handle;
    store.enable_fetch(2);
    store.save_hashes(&[hash()], 100).await.unwrap();
    let old = store.claim_job(100).await.unwrap().unwrap();
    store.recover_jobs(200).await.unwrap();
    let current = store.claim_job(200).await.unwrap().unwrap();
    assert_eq!(
        store
            .complete_job(old, verified().await, 201)
            .await
            .unwrap(),
        UpdateResult::Stale
    );
    assert!(store.metadata(hash()).await.unwrap().is_none());
    store
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_completion
                 BEFORE UPDATE OF state ON fetch_jobs
                 WHEN new.state='succeeded'
                 BEGIN
                     SELECT RAISE(ABORT,'injected commit failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .complete_job(current.clone(), verified().await, 202)
            .await,
        Err(StorageError::Database(_))
    ));
    assert!(store.metadata(hash()).await.unwrap().is_none());
    assert_eq!(store.fetch_stats().await.unwrap().running, 1);
    store
        .call(|connection| {
            connection.execute_batch("DROP TRIGGER fail_completion;")?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .complete_job(current, verified().await, 203)
            .await
            .unwrap(),
        UpdateResult::Applied
    );
    assert_eq!(store.metadata(hash()).await.unwrap().unwrap(), INFO);
    assert_eq!(store.fetch_stats().await.unwrap().succeeded, 1);
    storage.shutdown().await.unwrap();
}

/// 关闭会话会取消正在进行的 TCP 下载，未完成任务在重新打开后仍可领取。
#[tokio::test]
async fn graceful_shutdown_cancels_tcp_and_keeps_task_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle: _,
        address,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    session.start_fetch(config(dir.path())).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender = udp(AddressFamily::Ipv4).await;
    sender
        .send_to(address, &announce_query(None, 0))
        .await
        .unwrap();
    let token = sender
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .token
        .unwrap();
    sender
        .send_to(
            address,
            &announce_query(Some(token), listener.local_addr().unwrap().port()),
        )
        .await
        .unwrap();
    sender.recv().await.unwrap();
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut hello = [0; 68];
    socket.read_exact(&mut hello).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), session.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(socket.read(&mut hello).await.unwrap(), 0);
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    assert_eq!(storage.handle.fetch_stats().await.unwrap().running, 0);
    assert_eq!(storage.handle.active_jobs().await.unwrap(), 1);
    assert!(
        storage
            .handle
            .claim_job(now().unwrap() + 61_000)
            .await
            .unwrap()
            .is_some()
    );
    storage.shutdown().await.unwrap();
}

/// 采集写库故障可被上层观察，同时保留 DHT 网络服务。
#[tokio::test]
async fn fetch_storage_failure_is_reported_while_dht_remains_alive() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    store.save_hashes(&[hash()], now().unwrap()).await.unwrap();
    store
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_job
                 BEFORE INSERT ON fetch_jobs
                 BEGIN
                     SELECT RAISE(ABORT,'injected storage failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    session.start_fetch(config(dir.path())).await.unwrap();
    let fault = tokio::time::timeout(Duration::from_secs(3), session.next_fault())
        .await
        .unwrap();
    assert!(matches!(fault, SessionFault::StorageWrite(_)));
    assert!(handle.status().await.is_ok());
    assert!(session.shutdown().await.is_err());
}

/// 从尚未验证的候选继续迭代查询，取得可用 peer 后完成真实 metadata 下载。
#[tokio::test]
async fn iterative_lookup_follows_untrusted_candidate_then_downloads() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    store.save_hashes(&[hash()], now().unwrap()).await.unwrap();
    let (peer, peer_task) = tcp(AddressFamily::Ipv4).await;
    let a = udp(AddressFamily::Ipv4).await;
    let a_addr = a.local_addr().unwrap();
    let b = udp(AddressFamily::Ipv4).await;
    let b_addr = b.local_addr().unwrap();
    let b_id = NodeId(hash().0);
    let (seen, seen_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let b_task = tokio::spawn(async move {
        let request = b.recv().await.unwrap();
        assert_eq!(request.message.q, Some(QueryMethod::GetPeers));
        seen.send(()).unwrap();
        release_rx.await.unwrap();
        let mut reply = response(
            request.message.t,
            AddressFamily::Ipv4,
            Some(peer),
            QueryMethod::GetPeers,
        );
        reply.r.as_mut().unwrap().id = b_id;
        b.send_to(request.source, &reply).await.unwrap();
    });
    let a_task = tokio::spawn(async move {
        loop {
            let request = a.recv().await.unwrap();
            let mut reply = response(
                request.message.t,
                AddressFamily::Ipv4,
                None,
                QueryMethod::GetPeers,
            );
            if request.message.q == Some(QueryMethod::GetPeers) {
                let SocketAddr::V4(address) = b_addr else {
                    unreachable!()
                };
                reply.r.as_mut().unwrap().nodes =
                    Some(CompactNodesV4(vec![crate::dht::krpc::CompactNodeV4 {
                        id: b_id,
                        address,
                    }]));
            }
            a.send_to(request.source, &reply).await.unwrap();
        }
    });
    handle
        .ping(RemoteNode {
            address: a_addr,
            expected_id: Some(NodeId([8; 20])),
        })
        .await
        .unwrap();
    session.start_fetch(config(dir.path())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), seen_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !handle
            .fetch_seeds(hash())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == b_id)
    );
    release.send(()).unwrap();
    await_metadata(&store).await;
    assert!(
        handle
            .fetch_seeds(hash())
            .await
            .unwrap()
            .iter()
            .any(|n| n.id == b_id)
    );
    peer_task.await.unwrap();
    b_task.await.unwrap();
    session.shutdown().await.unwrap();
    a_task.abort();
    let _ = a_task.await;
}

/// get_peers 响应的身份错误或缺少 token 时，不能将其作为有效查找结果。
#[tokio::test]
async fn get_peers_rejects_wrong_identity_and_missing_token() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let server = udp(AddressFamily::Ipv4).await;
    let address = server.local_addr().unwrap();
    for wrong_id in [true, false] {
        let h = handle.clone();
        let query = tokio::spawn(async move {
            h.get_peers(
                RemoteNode {
                    address,
                    expected_id: Some(NodeId([8; 20])),
                },
                hash(),
            )
            .await
        });
        let request = server.recv().await.unwrap();
        let mut reply = response(
            request.message.t,
            AddressFamily::Ipv4,
            None,
            QueryMethod::GetPeers,
        );
        if wrong_id {
            reply.r.as_mut().unwrap().id = NodeId([9; 20]);
        } else {
            reply.r.as_mut().unwrap().token = None;
        }
        server.send_to(request.source, &reply).await.unwrap();
        let error = query.await.unwrap().unwrap_err();
        if wrong_id {
            assert!(matches!(
                error,
                crate::dht::dispatcher::QueryError::UnexpectedNodeId { .. }
            ));
        } else {
            assert!(matches!(
                error,
                crate::dht::dispatcher::QueryError::InvalidResponse(_)
            ));
        }
        assert!(handle.fetch_seeds(hash()).await.unwrap().is_empty());
    }
    session.shutdown().await.unwrap();
}

// 协调器报告 worker 异常后仍等待其他结果，成功事务不能随 JoinSet 一同丢弃。
#[tokio::test]
async fn worker_failure_reports_claim_and_drains_success() {
    for abort in [false, true] {
        let metadata = verified().await;
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
                .await
                .unwrap();
        let (report, mut faults) = FaultReporter::new();
        let collector = Collector::new(
            storage.handle.clone(),
            vec![],
            config(dir.path()),
            Clock::default(),
            CancellationToken::new(),
            report.pause_notifications(),
            report.collector_callback(FaultLog::default()),
        )
        .await
        .unwrap();
        let bad_hash = InfoHashV1([1; 20]);
        storage
            .handle
            .save_hashes(&[bad_hash, hash()], collector.now().unwrap())
            .await
            .unwrap();
        let bad = storage
            .handle
            .claim_job(collector.now().unwrap())
            .await
            .unwrap()
            .unwrap();
        let good = storage
            .handle
            .claim_job(collector.now().unwrap())
            .await
            .unwrap()
            .unwrap();
        let (bad, good) = if bad.hash == bad_hash {
            (bad, good)
        } else {
            (good, bad)
        };
        let generation = bad.generation;
        let network = super::tcp_limits::TcpLimits::default();
        let connection_permit = network.acquire_for_ip("127.0.0.1".parse().unwrap()).await;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut workers = Workers::default();
        let task = workers.spawn(bad.clone(), async move {
            let (_connection_permit, _permit) = (connection_permit, permit);
            if abort {
                std::future::pending::<()>().await;
            }
            panic!("注入 metadata worker 崩溃");
        });
        if abort {
            task.abort();
        }
        let (release, ready) = tokio::sync::oneshot::channel();
        workers.spawn(good, async move {
            ready.await.unwrap();
            Outcome::Success(metadata)
        });
        let metrics = collector.metrics.clone();
        let run = tokio::spawn(collector.supervise(workers));
        faults.changed().await.unwrap();
        assert!(faults.borrow().as_ref().unwrap().fatal());
        assert!(!run.is_finished(), "报告故障后仍须回收其余 worker");
        release.send(()).unwrap();
        let errors = run.await.unwrap().unwrap_err();
        assert!(errors.iter().any(|error| matches!(error,
            CollectorError::Worker { hash, generation: actual, source }
            if *hash == bad_hash && *actual == generation && source.is_cancelled() == abort)));
        assert_eq!(network.tracked_tcp_ips(), 0);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(
            storage.handle.metadata(hash()).await.unwrap().unwrap(),
            INFO
        );
        let (state, attempts): (String, u32) = storage
            .handle
            .call(move |c| {
                Ok(c.query_row(
                    "SELECT state,attempts FROM fetch_jobs WHERE hash=?1",
                    [bad_hash.0.as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!((state.as_str(), attempts), ("retry_wait", 0));
        assert_eq!(
            metrics.diagnostics.report()["attempt_commits"][0]["committed"],
            1
        );
        storage.shutdown().await.unwrap();
    }
}

/// 控制通道关闭属于致命错误，另一地址族的节点仍须进入清理流程。
#[tokio::test]
async fn control_failure_is_fatal_and_other_node_is_cleaned_up() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle: closed,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let live = session
        .add_node(
            "other",
            udp(AddressFamily::Ipv4).await,
            TransactionManager::new(Duration::from_secs(2), 8),
            DhtDispatcherConfig::default(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    let (report, faults) = FaultReporter::new();
    let collector = Collector::new(
        session.test_store(),
        vec![closed.clone(), live.clone()],
        config(dir.path()),
        Clock::default(),
        CancellationToken::new(),
        report.pause_notifications(),
        report.collector_callback(FaultLog::default()),
    )
    .await
    .unwrap();
    let ingress = collector.ingress.clone();
    closed.shutdown().await.unwrap();
    let errors = collector.run().await.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, CollectorError::Control { .. }))
    );
    assert!(faults.borrow().as_ref().unwrap().fatal());
    assert!(live.status().await.is_ok());
    assert_eq!(ingress.sender.strong_count(), 1, "存活节点也必须撤销入口");
    // 关闭节点本身是显式请求，session 仍能完成幂等清理。
    session.shutdown().await.unwrap();
}

/// 注入时钟决定重试期限，关闭重开后沿用磁盘期限而不是重新开始退避。
#[tokio::test(start_paused = true)]
async fn collector_retry_uses_injected_clock_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    for (anchor, initial) in [(100, true), (101, false)] {
        let storage = crate::collection::test_storage::TestStorage::open(settings.clone())
            .await
            .unwrap();
        let (report, _) = FaultReporter::new();
        let clock = Clock::new(
            tokio::time::Instant::now().into_std(),
            std::time::UNIX_EPOCH + Duration::from_secs(anchor),
        );
        let collector = Collector::new(
            storage.handle.clone(),
            vec![],
            config(dir.path()),
            clock,
            CancellationToken::new(),
            report.pause_notifications(),
            report.collector_callback(FaultLog::default()),
        )
        .await
        .unwrap();
        if initial {
            storage
                .handle
                .save_hashes(&[hash()], collector.now().unwrap())
                .await
                .unwrap();
            let job = storage
                .handle
                .claim_job(collector.now().unwrap())
                .await
                .unwrap()
                .unwrap();
            collector
                .save_outcome(job, Outcome::Retry(RetryReason::Deferred))
                .await
                .unwrap();
        } else {
            assert!(
                storage
                    .handle
                    .claim_job(collector.now().unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            tokio::time::advance(Duration::from_millis(58_999)).await;
            assert!(
                storage
                    .handle
                    .claim_job(collector.now().unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(
                storage
                    .handle
                    .claim_job(collector.now().unwrap())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        drop(collector);
        storage.shutdown().await.unwrap();
    }
}

/// 同 IP 等待者由许可释放唤醒，不同 IP 可同时持有许可；取消等待不能遗留 IP 登记。
#[tokio::test(start_paused = true)]
async fn tcp_waiters_wake_without_time_and_cancellation_leaks_nothing() {
    use futures_util::FutureExt;
    let network = super::tcp_limits::TcpLimits::default();
    let ip = "127.0.0.1".parse().unwrap();
    let held = network.acquire_for_ip(ip).await;
    let mut cancelled = Box::pin(network.acquire_for_ip(ip));
    assert!(cancelled.as_mut().now_or_never().is_none());
    drop(cancelled);
    let mut first = Box::pin(network.acquire_for_ip(ip));
    let mut second = Box::pin(network.acquire_for_ip(ip));
    assert!(first.as_mut().now_or_never().is_none());
    assert!(second.as_mut().now_or_never().is_none());
    let other = network
        .acquire_for_ip("127.0.0.2".parse().unwrap())
        .now_or_never()
        .unwrap();
    let instant = tokio::time::Instant::now();
    drop(held);
    let acquired = first.as_mut().now_or_never().expect("释放应立即唤醒等待者");
    assert!(second.as_mut().now_or_never().is_none());
    drop(acquired);
    drop(second.as_mut().now_or_never().unwrap());
    drop(other);
    assert_eq!(tokio::time::Instant::now(), instant);
    assert_eq!(network.tracked_tcp_ips(), 0);
}

/// 清理写入失败仍要回收其他 worker，留下的领取记录可在重启时恢复。
#[tokio::test]
async fn cleanup_write_failure_still_drains_workers_and_preserves_recovery() {
    let metadata = verified().await;
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let (report, faults) = FaultReporter::new();
    let mut collector = Collector::new(
        storage.handle.clone(),
        vec![],
        config(dir.path()),
        Clock::default(),
        CancellationToken::new(),
        report.pause_notifications(),
        report.collector_callback(FaultLog::default()),
    )
    .await
    .unwrap();
    storage
        .handle
        .save_hashes(&[hash(), InfoHashV1([1; 20])], collector.now().unwrap())
        .await
        .unwrap();
    let first = storage
        .handle
        .claim_job(collector.now().unwrap())
        .await
        .unwrap()
        .unwrap();
    let second = storage
        .handle
        .claim_job(collector.now().unwrap())
        .await
        .unwrap()
        .unwrap();
    let (success, cancelled) = if first.hash == hash() {
        (first, second)
    } else {
        (second, first)
    };
    let network = super::tcp_limits::TcpLimits::default();
    let connection_permit = network.acquire_for_ip("127.0.0.1".parse().unwrap()).await;
    let mut workers = Workers::default();
    workers.spawn(success, async move { Outcome::Success(metadata) });
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    workers.spawn(cancelled, async move {
        let _connection_permit = connection_permit;
        token.cancelled().await;
        Outcome::Retry(RetryReason::Deferred)
    });
    // 所有状态更新失败，验证即使第一个结果无法保存，第二个任务仍被 join。
    storage
        .handle
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_cleanup
                 BEFORE UPDATE ON fetch_jobs
                 BEGIN
                     SELECT RAISE(ABORT,'injected cleanup failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let mut errors = Vec::new();
    collector.finish(&mut workers, &cancel, &mut errors).await;
    assert!(errors.iter().any(CollectorError::is_storage));
    assert!(matches!(
        *faults.borrow(),
        Some(SessionFault::StorageWrite(_))
    ));
    assert!(workers.is_empty());
    assert_eq!(network.tracked_tcp_ips(), 0);
    assert!(storage.handle.metadata(hash()).await.unwrap().is_none());
    assert_eq!(storage.handle.fetch_stats().await.unwrap().running, 2);
    storage
        .handle
        .call(|connection| {
            connection.execute_batch("DROP TRIGGER fail_cleanup;")?;
            Ok(())
        })
        .await
        .unwrap();
    storage
        .handle
        .recover_jobs(collector.now().unwrap())
        .await
        .unwrap();
    assert_eq!(storage.handle.fetch_stats().await.unwrap().pending, 2);
    drop(collector);
    storage.shutdown().await.unwrap();
}

/// 一条成功近邻返回七个协议失败近邻和一个存活备用，查找必须补位。
#[tokio::test]
async fn lookup_promotes_reserve_on_both_families() {
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        let dir = tempfile::tempdir().unwrap();
        let Fixture {
            session, handle, ..
        } = fixture(dir.path(), family).await;
        let mut servers = Vec::new();
        let mut contacts = Vec::new();
        for id in 1..=9 {
            let server = udp(family).await;
            contacts.push((NodeId([id; 20]), server.local_addr().unwrap()));
            servers.push(server);
        }
        let peer: SocketAddr = if family == AddressFamily::Ipv4 {
            "127.0.0.1:6881"
        } else {
            "[::1]:6881"
        }
        .parse()
        .unwrap();
        let mut tasks = Vec::new();
        for (index, server) in servers.into_iter().enumerate() {
            let contacts = contacts.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let request = server.recv().await.unwrap();
                    let method = request.message.q.unwrap();
                    let mut message = response(request.message.t, family, None, method.clone());
                    let args = message.r.as_mut().unwrap();
                    args.id = contacts[index].0;
                    if method == QueryMethod::GetPeers {
                        if index == 0 {
                            args.nodes = (family == AddressFamily::Ipv4).then(|| {
                                CompactNodesV4(
                                    contacts
                                        .iter()
                                        .skip(1)
                                        .map(|(id, a)| crate::dht::krpc::CompactNodeV4 {
                                            id: *id,
                                            address: match a {
                                                SocketAddr::V4(a) => *a,
                                                _ => unreachable!(),
                                            },
                                        })
                                        .collect(),
                                )
                            });
                            args.nodes6 = (family == AddressFamily::Ipv6).then(|| {
                                CompactNodesV6(
                                    contacts
                                        .iter()
                                        .skip(1)
                                        .map(|(id, a)| crate::dht::krpc::CompactNodeV6 {
                                            id: *id,
                                            address: match a {
                                                SocketAddr::V6(a) => *a,
                                                _ => unreachable!(),
                                            },
                                        })
                                        .collect(),
                                )
                            });
                        } else if index == 8 {
                            args.values = Some(vec![match peer {
                                SocketAddr::V4(a) => CompactPeerAddress::V4(a),
                                SocketAddr::V6(a) => CompactPeerAddress::V6(a),
                            }]);
                        } else {
                            args.token = None;
                        }
                    }
                    server.send_to(request.source, &message).await.unwrap();
                }
            }));
        }
        let target = InfoHashV1([0; 20]);
        // 直接查询作为正向对照，但不污染随后查找使用的初始路由。
        handle
            .ping(RemoteNode {
                address: contacts[0].1,
                expected_id: Some(contacts[0].0),
            })
            .await
            .unwrap();
        let result = lookup::lookup(
            std::slice::from_ref(&handle),
            target,
            Arc::new(lookup::LookupPacer::default()),
        )
        .await;
        assert_eq!(result.peers, vec![peer]);
        let direct = handle
            .get_peers(
                RemoteNode {
                    address: contacts[8].1,
                    expected_id: Some(contacts[8].0),
                },
                target,
            )
            .await
            .unwrap();
        assert_eq!(direct.peers, result.peers);
        assert_eq!(handle.status().await.unwrap().pending, 0);
        session.shutdown().await.unwrap();
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// 本地排队吃掉总期限时延期；释放持有者后跟踪表清空。
#[tokio::test(start_paused = true)]
async fn local_tcp_wait_timeout_does_not_consume_failure_attempt() {
    let network = Arc::new(WorkerResources::new(
        PeerClient::new(MetadataConfig::default()).unwrap(),
        Arc::default(),
    ));
    let peer: SocketAddr = "127.0.0.1:6881".parse().unwrap();
    let held = network.tcp.acquire_for_ip(peer.ip()).await;
    let outcome = run_job(
        Job {
            had_valid_hint: true,
            class: jobs::ClaimClass::Hint,
            hash: hash(),
            generation: 1,
            failed_attempts_before: 0,
            peers: vec![peer],
        },
        vec![],
        network.clone(),
        AddressPolicy::LocalUnicast,
        vec![AddressFamily::Ipv4],
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(
        outcome,
        Outcome::Retry(RetryReason::Local(LocalReason::ResourceWait))
    ));
    drop(held);
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
}

// 固定输入比较与默认回归共用真实 peer/DHT，不以减少接纳任务提高完成率。
#[derive(Default)]
struct PipelineProbe {
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    first_connections: std::sync::Mutex<Vec<u64>>,
}
async fn pipeline_peer(
    family: AddressFamily,
    info: Vec<u8>,
    probe: Arc<PipelineProbe>,
    start: tokio::time::Instant,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(if family == AddressFamily::Ipv4 {
        "127.0.0.1:0"
    } else {
        "[::1]:0"
    })
    .await
    .unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        probe
            .first_connections
            .lock()
            .unwrap()
            .push(start.elapsed().as_millis() as u64);
        let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
        probe.peak.fetch_max(active, Ordering::SeqCst);
        let h = InfoHashV1(Sha1::digest(&info).into());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let mut hello = [0; 68];
        socket.read_exact(&mut hello).await.unwrap();
        peer_wire::parse_handshake(&hello, h).unwrap();
        socket
            .write_all(&peer_wire::handshake(h, PeerId([7; 20])))
            .await
            .unwrap();
        let mut stream = LengthDelimitedCodec::builder().new_framed(socket);
        stream.next().await.unwrap().unwrap();
        stream
            .send(peer_wire::extended(
                0,
                format!("d1:md11:ut_metadatai7ee13:metadata_sizei{}ee", info.len()).as_bytes(),
            ))
            .await
            .unwrap();
        stream.next().await.unwrap().unwrap();
        let mut body =
            format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
        body.extend_from_slice(&info);
        stream.send(peer_wire::extended(1, &body)).await.unwrap();
        drop(stream);
        probe.active.fetch_sub(1, Ordering::SeqCst);
    });
    (address, task)
}
async fn fixed_pipeline_scenario(family: AddressFamily, jobs: usize) -> serde_json::Value {
    use crate::dht::dispatcher::DhtDispatcher;
    use crate::dht::routing::RoutingTable;
    use crate::dht::traffic::Budget;
    let start = tokio::time::Instant::now();
    let probe = Arc::new(PipelineProbe::default());
    let mut peers = std::collections::HashMap::new();
    let mut peer_tasks = Vec::new();
    let mut hashes = Vec::new();
    for n in 0..jobs {
        let info = format!("d4:name1:{n}6:pieces0:e").into_bytes();
        let h = InfoHashV1(Sha1::digest(&info).into());
        let (addr, task) = pipeline_peer(family, info, probe.clone(), start).await;
        hashes.push(h);
        peers.insert(h, addr);
        peer_tasks.push(task);
    }
    let server = udp(family).await;
    let mut silent = Vec::new();
    for _ in 0..3 {
        silent.push(udp(family).await);
    }
    let routing = RoutingTable::new(NodeId([1; 20]), family, start.into_std());
    let mut config = DhtDispatcherConfig::default();
    config.maintenance.enabled = false;
    config.peer_store.address_policy = AddressPolicy::LocalUnicast;
    let budget = Arc::new(Budget::default());
    let (dispatcher, handle) = DhtDispatcher::with_budget(
        udp(family).await,
        routing,
        TransactionManager::new(Duration::from_secs(60), 128),
        config,
        budget.clone(),
    )
    .unwrap();
    let dispatcher_task = tokio::spawn(dispatcher.run());
    let seed = server.local_addr().unwrap();
    let addresses: Vec<_> = silent.iter().map(|s| s.local_addr().unwrap()).collect();
    let server_task = tokio::spawn(async move {
        loop {
            let query = server.recv().await.unwrap();
            if query.message.q == Some(QueryMethod::Ping) {
                server
                    .send_to(
                        query.source,
                        &response(query.message.t, family, None, QueryMethod::Ping),
                    )
                    .await
                    .unwrap();
                continue;
            }
            let h = query.message.a.unwrap().info_hash.unwrap();
            let mut reply = response(
                query.message.t,
                family,
                Some(peers[&h]),
                QueryMethod::GetPeers,
            );
            let args = reply.r.as_mut().unwrap();
            args.nodes = (family == AddressFamily::Ipv4).then(|| {
                CompactNodesV4(
                    addresses
                        .iter()
                        .enumerate()
                        .map(|(n, a)| crate::dht::krpc::CompactNodeV4 {
                            id: NodeId([n as u8 + 20; 20]),
                            address: match a {
                                SocketAddr::V4(a) => *a,
                                _ => unreachable!(),
                            },
                        })
                        .collect(),
                )
            });
            args.nodes6 = (family == AddressFamily::Ipv6).then(|| {
                CompactNodesV6(
                    addresses
                        .iter()
                        .enumerate()
                        .map(|(n, a)| crate::dht::krpc::CompactNodeV6 {
                            id: NodeId([n as u8 + 20; 20]),
                            address: match a {
                                SocketAddr::V6(a) => *a,
                                _ => unreachable!(),
                            },
                        })
                        .collect(),
                )
            });
            server.send_to(query.source, &reply).await.unwrap();
        }
    });
    handle
        .ping(RemoteNode {
            address: seed,
            expected_id: Some(NodeId([8; 20])),
        })
        .await
        .unwrap();
    let fetcher = PeerClient::new(MetadataConfig {
        address_policy: AddressPolicy::LocalUnicast,
        ..Default::default()
    })
    .unwrap();
    let network = Arc::new(WorkerResources::new(
        fetcher.clone(),
        fetcher.test_metrics(),
    ));
    let outcomes = tokio::time::timeout(
        Duration::from_secs(40),
        futures_util::future::join_all(hashes.into_iter().map(|h| {
            let handle = handle.clone();
            let network = network.clone();
            async move {
                let outcome = run_job(
                    Job {
                        had_valid_hint: false,
                        class: jobs::ClaimClass::Recent,
                        hash: h,
                        generation: 1,
                        failed_attempts_before: 0,
                        peers: vec![],
                    },
                    vec![handle],
                    network,
                    AddressPolicy::LocalUnicast,
                    vec![family],
                    CancellationToken::new(),
                )
                .await;
                assert!(matches!(outcome, Outcome::Success(_)));
                start.elapsed().as_millis() as u64
            }
        })),
    )
    .await
    .unwrap();
    // 取消通知之外，观察 dispatcher 已实际清理登记。
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if handle.status().await.unwrap().pending == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
    for task in peer_tasks {
        task.await.unwrap();
    }
    handle.shutdown().await.unwrap();
    dispatcher_task.await.unwrap().unwrap();
    server_task.abort();
    let _ = server_task.await;
    let first = probe.first_connections.lock().unwrap().clone();
    serde_json::json!({"family":format!("{family:?}"),"accepted":jobs,"completed":outcomes.len(),"completion_ms":outcomes,"first_connection_ms":first,"peak_tcp":probe.peak.load(Ordering::SeqCst),"dht":budget.snapshot()})
}
#[tokio::test]
async fn streaming_peer_connects_before_slow_lookup_finishes_on_both_families() {
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        let report = fixed_pipeline_scenario(family, 1).await;
        assert!(
            report["completion_ms"][0].as_u64().unwrap() < 5000,
            "{report}"
        );
        assert_eq!(report["peak_tcp"], 1);
        assert!(report["dht"]["inflight_cancelled"][0].as_u64().unwrap() > 0);
    }
}
#[tokio::test]
#[ignore = "独立固定输入 Release 比较，不是公网或长测"]
async fn pipeline_release_comparison() {
    let report = fixed_pipeline_scenario(AddressFamily::Ipv4, 4).await;
    assert_eq!(report["completed"], 4);
    assert!(report["peak_tcp"].as_u64().unwrap() <= 4);
    println!("PIPELINE_REPORT={report}");
}

/// 首个 hint 连接后断开，worker 等待流式 DHT 补位；另一地址族无路由不阻止成功。
#[tokio::test]
async fn failed_hint_is_replaced_by_streamed_peer_with_other_family_unrouted() {
    let dir = tempfile::tempdir().unwrap();
    let mut f = fixture(dir.path(), AddressFamily::Ipv4).await;
    let other = f
        .session
        .add_node(
            "other",
            udp(AddressFamily::Ipv6).await,
            TransactionManager::new(Duration::from_secs(1), 32),
            DhtDispatcherConfig {
                maintenance: crate::dht::dispatcher::MaintenanceConfig {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    let bad = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bad_address = bad.local_addr().unwrap();
    let bad_task = tokio::spawn(async move {
        drop(bad.accept().await.unwrap());
    });
    let (good, good_task) = tcp(AddressFamily::Ipv4).await;
    let remote = udp(AddressFamily::Ipv4).await;
    let remote_address = remote.local_addr().unwrap();
    let remote_task = tokio::spawn(async move {
        for method in [QueryMethod::Ping, QueryMethod::GetPeers] {
            let packet = remote.recv().await.unwrap();
            assert_eq!(packet.message.q.as_ref(), Some(&method));
            if method == QueryMethod::GetPeers {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            remote
                .send_to(
                    packet.source,
                    &response(
                        packet.message.t,
                        AddressFamily::Ipv4,
                        (method == QueryMethod::GetPeers).then_some(good),
                        method,
                    ),
                )
                .await
                .unwrap();
        }
    });
    f.handle
        .ping(RemoteNode {
            address: remote_address,
            expected_id: Some(NodeId([8; 20])),
        })
        .await
        .unwrap();
    let fetcher = PeerClient::new(MetadataConfig {
        address_policy: AddressPolicy::LocalUnicast,
        ..Default::default()
    })
    .unwrap();
    let network = Arc::new(WorkerResources::new(
        fetcher.clone(),
        fetcher.test_metrics(),
    ));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_job(
            Job {
                had_valid_hint: true,
                class: jobs::ClaimClass::Hint,
                hash: hash(),
                generation: 1,
                failed_attempts_before: 0,
                peers: vec![bad_address],
            },
            vec![f.handle.clone(), other],
            network.clone(),
            AddressPolicy::LocalUnicast,
            vec![AddressFamily::Ipv4, AddressFamily::Ipv6],
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(result, Outcome::Success(_)));
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
    bad_task.await.unwrap();
    good_task.await.unwrap();
    remote_task.await.unwrap();
    assert_eq!(f.handle.status().await.unwrap().pending, 0);
    f.session.shutdown().await.unwrap();
}

/// 构造末尾订阅不补发旧暂停通知；唯一报告端退出仍按监督通道关闭报告错误。
#[tokio::test]
async fn late_fault_subscription_and_closed_notifications_preserve_contract() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let (pause, _) = watch::channel(());
    pause.send_replace(());
    let collector = Collector::new(
        storage.handle.clone(),
        vec![],
        config(dir.path()),
        Clock::default(),
        CancellationToken::new(),
        pause.clone(),
        Box::new(|_| {}),
    )
    .await
    .unwrap();
    assert!(!collector.faults.has_changed().unwrap());
    drop(pause);
    let errors = collector.run().await.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, CollectorError::SupervisorClosed))
    );
    storage.shutdown().await.unwrap();
}

/// 真实 collector → dispatcher 暂停/恢复命令；重试存量不阻碍恢复，存储暂停仍优先。
#[tokio::test]
async fn first_attempt_backpressure_reaches_sampler_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        ..
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    let mut cfg = config(dir.path());
    cfg.concurrency = 4;
    cfg.max_active = 10000;
    cfg.sample_backpressure = SampleBackpressure::Freshness;
    let (notifications, _) = watch::channel(());
    let start = 3_600_000;
    let make_clock = |second: u64| {
        Clock::new(
            tokio::time::Instant::now().into_std(),
            std::time::UNIX_EPOCH + Duration::from_millis(start as u64 + second * 1000),
        )
    };
    let mut collector = Collector::new(
        store.clone(),
        vec![handle.clone()],
        cfg,
        make_clock(0),
        CancellationToken::new(),
        notifications,
        Box::new(|_| {}),
    )
    .await
    .unwrap();
    session
        .start_sampling(
            0,
            crate::dht::dispatcher::SamplerConfig {
                address_policy: AddressPolicy::LocalUnicast,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let hashes: Vec<_> = (0..66).map(|n| InfoHashV1([n; 20])).collect();
    // 先准备 50 条非到期重试，再统一补建 16 个首试。
    store.enable_recent_admission(0, AddressPolicy::LocalUnicast);
    store.save_hashes(&hashes[..50], start).await.unwrap();
    store
        .call(move |c| {
            c.execute(
                "UPDATE fetch_jobs SET generation=1,state='retry_wait',due_at=?1+300000",
                [start],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    store.enable_recent_admission(16, AddressPolicy::LocalUnicast);
    store.save_hashes(&hashes[50..], start).await.unwrap();
    store.backfill_recent_page(start, None).await.unwrap();
    collector.update_sampling_backpressure(true).await.unwrap();
    assert!(handle.sampling_status().await.unwrap().collector_paused);
    // 用真实领取事务释放 Q，不伪造统计数。网络工作时序由独立 SQLite 比较覆盖。
    for _ in 0..16 {
        let claim = store
            .claim_class(
                start,
                Some(jobs::ClaimClass::Recent),
                AddressPolicy::LocalUnicast,
            )
            .await
            .unwrap()
            .unwrap();
        store
            .retry_job(claim.job, start, RetryReason::Deferred)
            .await
            .unwrap();
    }
    for second in [5, 10, 15, 20, 25, 30] {
        collector.clock = make_clock(second);
        collector.update_sampling_backpressure(true).await.unwrap();
        assert!(handle.sampling_status().await.unwrap().collector_paused);
    }
    collector.clock = make_clock(36);
    collector.update_sampling_backpressure(true).await.unwrap();
    let status = handle.sampling_status().await.unwrap();
    assert!(status.running);
    assert!(!status.collector_paused);
    assert_eq!(store.recent_active_jobs(start + 36000).await.unwrap(), 66);
    collector.storage_paused = true;
    collector
        .backpressure
        .update(start + 40000, 66, Some(0), true);
    collector.pause(true).await.unwrap();
    assert!(handle.sampling_status().await.unwrap().collector_paused);
    let mut errors = Vec::new();
    collector
        .finish(
            &mut Workers::default(),
            &CancellationToken::new(),
            &mut errors,
        )
        .await;
    assert!(errors.is_empty());
    assert_eq!(store.fetch_stats().await.unwrap().running, 0);
    session.shutdown().await.unwrap();
}

/// 正常保存和退出保存均只统计 Applied；故障回滚、旧结果和重复完成不能增加提交数。
#[tokio::test]
async fn classified_commits_require_applied_in_running_and_cleanup_paths() {
    async fn save(collector: &Collector, job: Job, cleanup: bool) -> Result<(), CollectorError> {
        let (peer, task) = tcp_info_with_order(AddressFamily::Ipv4, INFO.to_vec(), true).await;
        let metadata = PeerClient::new(MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap()
        .with_metrics(collector.metrics.clone())
        .fetch_one(
            hash(),
            peer,
            &CancellationToken::new(),
            super::peer::PeerContext::default(),
        )
        .await
        .unwrap();
        task.await.unwrap();
        assert!(metadata.used_extension_compatibility());
        let outcome = Outcome::Success(metadata);
        if cleanup {
            collector.save_outcome(job, outcome).await
        } else {
            collector
                .apply_running_outcome(job, outcome, &mut CompletionTotals::default())
                .await
        }
    }
    for cleanup in [false, true] {
        for recover in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let storage =
                crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
                    .await
                    .unwrap();
            let (report, _) = FaultReporter::new();
            let collector = Collector::new(
                storage.handle.clone(),
                vec![],
                config(dir.path()),
                Clock::default(),
                CancellationToken::new(),
                report.pause_notifications(),
                report.collector_callback(FaultLog::default()),
            )
            .await
            .unwrap();
            let store = &storage.handle;
            store.save_hashes(&[hash()], 100).await.unwrap();
            let old = store.claim_job(100).await.unwrap().unwrap();
            let job = if recover {
                store.recover_jobs(200).await.unwrap();
                let current = store.claim_job(200).await.unwrap().unwrap();
                save(&collector, old, cleanup).await.unwrap();
                current
            } else {
                let mut stale = old.clone();
                stale.generation += 100;
                save(&collector, stale, cleanup).await.unwrap();
                old
            };
            assert!(
                collector.metrics.diagnostics.report()["attempt_commits"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                collector.metrics.diagnostics.report()["extension_compatibility"]["committed"],
                0
            );
            store
                .call(|connection| {
                    connection.execute_batch(
                        "CREATE TRIGGER fail_classified_commit
                     BEFORE UPDATE OF state ON fetch_jobs
                     WHEN new.state='succeeded'
                     BEGIN
                         SELECT RAISE(ABORT, 'injected write failure');
                     END;",
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            assert!(save(&collector, job.clone(), cleanup).await.is_err());
            assert!(store.metadata(hash()).await.unwrap().is_none());
            assert!(
                collector.metrics.diagnostics.report()["attempt_commits"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                collector.metrics.diagnostics.report()["extension_compatibility"]["committed"],
                0
            );
            store
                .call(|connection| {
                    connection.execute_batch("DROP TRIGGER fail_classified_commit")?;
                    Ok(())
                })
                .await
                .unwrap();
            save(&collector, job.clone(), cleanup).await.unwrap();
            save(&collector, job, cleanup).await.unwrap();
            let report = collector.metrics.diagnostics.report();
            let commits = report["attempt_commits"].as_array().unwrap();
            assert_eq!(commits.len(), 1);
            assert_eq!(
                commits[0]["attempt_kind"],
                if recover { "Repeat" } else { "First" }
            );
            assert_eq!(commits[0]["failed_attempts_before"], 0);
            assert_eq!(commits[0]["committed"], 1);
            assert_eq!(report["extension_compatibility"]["committed"], 1);
            assert_eq!(report["attempt_hints"][0]["committed"], 1);
            let class_commits = report["attempt_class_commits"].as_array().unwrap();
            assert_eq!(class_commits.len(), 1);
            assert_eq!(class_commits[0]["committed"], 1);
            assert_eq!(
                class_commits[0]["claim_class"],
                if recover { "Retry" } else { "Recent" }
            );
            assert_eq!(store.fetch_stats().await.unwrap().succeeded, 1);
            storage.shutdown().await.unwrap();
        }
    }
}

/// 调度配置拒绝零并发及超过 Semaphore 许可上限的并发值。
#[test]
fn invalid_worker_concurrency_is_rejected_by_collection() {
    let directory = tempfile::tempdir().unwrap();
    for concurrency in [0, tokio::sync::Semaphore::MAX_PERMITS + 1] {
        let mut settings = config(directory.path());
        settings.concurrency = concurrency;
        assert!(matches!(
            settings.validate(),
            Err(peer::PeerInitError::InvalidConfig)
        ));
    }
}
