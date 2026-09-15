//! 手动短比较：六组顺序执行，每组真实 loopback 运行 120 秒，资源归各组所有。
use super::*;
use crate::collection::peer::wire as peer_wire;
use crate::collection::peer::wire::PeerId;
use crate::collection::test_storage::TestStorage as Storage;
use crate::dht::dispatcher::DhtDispatcher;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::dispatcher::RemoteNode;
use crate::dht::krpc::*;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::RoutingTable;
use crate::dht::traffic::Budget;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;
use crate::info_hash::InfoHashV1;
use crate::storage::StorageConfig;
use futures_util::{SinkExt, StreamExt};
use sha1::{Digest, Sha1};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::codec::LengthDelimitedCodec;

type Catalog = Arc<HashMap<InfoHashV1, Vec<u8>>>;
async fn serve_peer(mut socket: TcpStream, slow: bool, catalog: Catalog) -> std::io::Result<()> {
    let mut hello = [0; 68];
    socket.read_exact(&mut hello).await?;
    if slow {
        tokio::time::sleep(Duration::from_secs(6)).await;
        return Ok(());
    }
    let hash = InfoHashV1(hello[28..48].try_into().unwrap());
    let Some(info) = catalog.get(&hash) else {
        return Ok(());
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    socket
        .write_all(&peer_wire::handshake(hash, PeerId([7; 20])))
        .await?;
    let mut frames = LengthDelimitedCodec::builder()
        .max_frame_length(65536)
        .new_framed(socket);
    let _ = frames.next().await;
    frames
        .send(peer_wire::extended(
            0,
            format!("d1:md11:ut_metadatai7ee13:metadata_sizei{}ee", info.len()).as_bytes(),
        ))
        .await?;
    let _ = frames.next().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut body = format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).into_bytes();
    body.extend_from_slice(info);
    frames.send(peer_wire::extended(1, &body)).await?;
    Ok(())
}
async fn listener(
    address: &str,
    slow: bool,
    catalog: Catalog,
    stop: CancellationToken,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(address).await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                result = clients.join_next(), if !clients.is_empty() => {
                    result.unwrap().unwrap();
                },
                accepted = listener.accept(), if clients.len() < 16 => {
                    let (socket, _) = accepted.unwrap();
                    let catalog = catalog.clone();
                    let stop = stop.clone();
                    clients.spawn(async move {
                        tokio::select! {
                            _ = stop.cancelled() => {},
                            result = serve_peer(socket, slow, catalog) => {
                                if let Err(error) = result {
                                    assert!(
                                        matches!(
                                            error.kind(),
                                            std::io::ErrorKind::BrokenPipe
                                                | std::io::ErrorKind::ConnectionReset
                                                | std::io::ErrorKind::UnexpectedEof
                                                | std::io::ErrorKind::ConnectionAborted
                                        ),
                                        "{error}"
                                    );
                                }
                            }
                        }
                    });
                }
            }
        }
        while let Some(result) = clients.join_next().await {
            result.unwrap();
        }
    });
    (address, task)
}
/// /proc 是本机测试观测，失败明确返回 None，不把缺失值当作零。
fn resources() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect();
    let cpu = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let rss = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((cpu, rss))
}
async fn run_group(concurrency: usize, repetition: usize) -> serde_json::Value {
    let dir = tempfile::Builder::new()
        .prefix("bt-optimization-loopback-")
        .tempdir()
        .unwrap()
        .keep();
    let mut report = crate::acceptance::Report::new(
        "optimization-loopback",
        &dir,
        serde_json::json!({
            "concurrency": concurrency,
            "repetition": repetition,
            "duration_seconds": 120,
            "jobs": 1000,
            "slow_handshake_seconds": 6,
            "successful_stage_delay_ms": 100,
            "families": ["ipv4","ipv6"],
        }),
    );
    report.running();
    let mut catalog = HashMap::new();
    let mut order = Vec::new();
    for n in 0..1000 {
        let info = format!("d4:name8:{n:08}6:pieces0:e").into_bytes();
        let hash = InfoHashV1(Sha1::digest(&info).into());
        order.push(hash);
        catalog.insert(hash, info);
    }
    let catalog = Arc::new(catalog);
    let fixture_stop = CancellationToken::new();
    let mut fixtures = Vec::new();
    let mut addresses = Vec::new();
    for n in 0..8 {
        let (slow, t) = listener(
            &format!("127.0.0.{}:0", n * 2 + 2),
            true,
            catalog.clone(),
            fixture_stop.clone(),
        )
        .await;
        fixtures.push(t);
        let (good, t) = listener(
            &format!("127.0.0.{}:0", n * 2 + 3),
            false,
            catalog.clone(),
            fixture_stop.clone(),
        )
        .await;
        fixtures.push(t);
        addresses.push((slow, good));
    }
    let (slow6, t) = listener("[::1]:0", true, catalog.clone(), fixture_stop.clone()).await;
    fixtures.push(t);
    let (good6, t) = listener("[::1]:0", false, catalog, fixture_stop.clone()).await;
    fixtures.push(t);
    let targets: Arc<HashMap<_, _>> = Arc::new(
        order
            .iter()
            .enumerate()
            .map(|(n, h)| {
                (
                    *h,
                    if n % 4 == 0 {
                        (slow6, good6)
                    } else {
                        addresses[n % 8]
                    },
                )
            })
            .collect(),
    );
    let budget = Arc::new(Budget::default());
    let mut handles = Vec::new();
    let mut dispatchers = Vec::new();
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        let bind = if family == AddressFamily::Ipv4 {
            "127.0.0.1:0"
        } else {
            "[::1]:0"
        };
        let server = UdpTransport::bind(bind, Default::default()).await.unwrap();
        let seed = server.local_addr().unwrap();
        let transport = UdpTransport::bind(bind, Default::default()).await.unwrap();
        let mut config = DhtDispatcherConfig::default();
        config.maintenance.enabled = false;
        config.peer_store.address_policy = AddressPolicy::LocalUnicast;
        let (dispatcher, handle) = DhtDispatcher::with_budget(
            transport,
            RoutingTable::new(NodeId([1; 20]), family, std::time::Instant::now()),
            TransactionManager::new(Duration::from_secs(5), 64),
            config,
            budget.clone(),
        )
        .unwrap();
        dispatchers.push(tokio::spawn(dispatcher.run()));
        let stop = fixture_stop.clone();
        let targets = targets.clone();
        fixtures.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    query = server.recv() => {
                        let query = query.unwrap();
                        let peers = query.message.a.as_ref()
                            .and_then(|args| args.info_hash)
                            .and_then(|hash| targets.get(&hash))
                            .map(|&(slow, good)| vec![slow, good]);
                        let values = peers.map(|peers| {
                            peers.into_iter().map(|peer| match peer {
                                SocketAddr::V4(address) => CompactPeerAddress::V4(address),
                                SocketAddr::V6(address) => CompactPeerAddress::V6(address),
                            }).collect()
                        });
                        let reply = KrpcMessage {
                            t: query.message.t,
                            y: MessageType::Response,
                            q: None,
                            a: None,
                            e: None,
                            ro: None,
                            r: Some(ResponseArgs {
                                id: NodeId([8; 20]),
                                token: Some(Token(serde_bytes::ByteBuf::from(b"token".to_vec()))),
                                nodes: None,
                                nodes6: None,
                                values,
                                samples: None,
                                interval: None,
                                num: None,
                            }),
                        };
                        server.send_to(query.source, &reply).await.unwrap();
                    }
                }
            }
        }));
        handle
            .ping(RemoteNode {
                address: seed,
                expected_id: Some(NodeId([8; 20])),
            })
            .await
            .unwrap();
        handles.push(handle);
    }
    let storage = Storage::open(StorageConfig::new(&dir)).await.unwrap();
    let store = storage.handle.clone();
    store.save_hashes(&order, 0).await.unwrap();
    let stop = CancellationToken::new();
    let (faults, _) = watch::channel(());
    let collector = Collector::new(
        store.clone(),
        handles.clone(),
        Config {
            metadata: MetadataConfig {
                address_policy: AddressPolicy::LocalUnicast,
                ..Default::default()
            },
            concurrency,
            max_active: 10000,
            sample_backpressure: SampleBackpressure::Freshness,
            state_max_bytes: 1024 * 1024 * 1024,
            directory: dir.clone(),
            policy: AddressPolicy::LocalUnicast,
        },
        Clock::default(),
        stop.clone(),
        faults.clone(),
        Box::new(|_| {}),
    )
    .await
    .unwrap();
    let metrics = collector.metrics.clone();
    let cpu_start = resources().unwrap();
    let start = tokio::time::Instant::now();
    let task = tokio::spawn(collector.run());
    let mut rss_peak = cpu_start.1;
    let mut storage_ms = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let signal = crate::app::shutdown_signal().unwrap();
    tokio::pin!(signal);
    let completed = loop {
        tokio::select! {
            _ = &mut signal => break false,
            _ = tokio::time::sleep_until(start + Duration::from_secs(120)) => break true,
            _ = tick.tick() => {
                let at = std::time::Instant::now();
                store.fetch_stats().await.unwrap();
                storage_ms.push(at.elapsed().as_micros() as u64);
                rss_peak = rss_peak.max(resources().unwrap().1);
            }
        }
    };
    let cpu_end = resources().unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let stats = store.fetch_stats().await.unwrap();
    assert_eq!(stats.running, 0);
    assert!(stats.succeeded > 0);
    for handle in &handles {
        handle.shutdown().await.unwrap();
    }
    for task in dispatchers {
        task.await.unwrap().unwrap();
    }
    fixture_stop.cancel();
    for task in fixtures {
        task.await.unwrap();
    }
    storage.shutdown().await.unwrap();
    assert!(!dir.join("state.sqlite3-wal").exists());
    assert!(!dir.join("state.sqlite3-shm").exists());
    storage_ms.sort_unstable();
    let p95 = storage_ms[(storage_ms.len() * 95).div_ceil(100) - 1];
    let hz = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .unwrap();
    assert!(hz.status.success());
    let hz = String::from_utf8(hz.stdout)
        .unwrap()
        .trim()
        .parse::<f64>()
        .unwrap();
    let dht = budget.snapshot();
    let value = serde_json::json!({
        "concurrency": concurrency,
        "repetition": repetition,
        "elapsed_seconds": elapsed,
        "metadata_per_second": stats.succeeded as f64 / elapsed,
        "metrics": metrics.report(),
        "cpu_seconds": (cpu_end.0 - cpu_start.0) as f64 / hz,
        "rss_peak_kib": rss_peak,
        "storage_stats_call_p95_us": p95,
        "dht_queue_p95": dht.queue_wait[0].quantile(95),
        "dht_packets": dht.packets,
        "validated_v4": dht.validated_v4,
        "validated_v6": dht.validated_v6,
        "shutdown_ok": true,
        "directory": dir,
    });
    assert!(dht.validated_v4 > 0 && dht.validated_v6 > 0);
    report.value["statistics"] = value.clone();
    report
        .finish(completed, true)
        .expect("验收报告必须成功保存");
    println!("LOOPBACK_GROUP={value}");
    assert!(completed, "用户中止，未完成验收");
    value
}
#[tokio::test]
#[ignore = "独立 6 × 120 秒双栈 loopback 比较，无公网"]
async fn concurrency_release_comparison() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "使用 Release 配置"
    );
    let mut results = Vec::new();
    for repetition in 1..=3 {
        for concurrency in [4, 8] {
            results.push(run_group(concurrency, repetition).await);
        }
    }
    println!(
        "CONCURRENCY_COMPARISON={}",
        serde_json::json!({"kind":"loopback","results":results})
    );
}
