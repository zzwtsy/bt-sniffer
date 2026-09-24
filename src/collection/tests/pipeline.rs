//! 流式查找与下载交叠，含显式 Release 比较。
use super::*;

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
        let h = SwarmKey(Sha1::digest(&info).into());
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
        let h = SwarmKey(Sha1::digest(&info).into());
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
