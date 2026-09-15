//! worker 候选边界与登记时点；固定地址不发公网请求。
use super::*;

fn candidate_job(peers: Vec<SocketAddr>) -> Job {
    Job {
        hash: hash(),
        generation: 1,
        failed_attempts_before: 0,
        class: jobs::ClaimClass::Hint,
        had_valid_hint: !peers.is_empty(),
        peers,
    }
}

fn observed_network() -> (Arc<WorkerResources>, crate::observation::Observer) {
    let observer = crate::observation::Observer::new("candidate-test".into());
    // worker 允许 loopback，PeerClient 拒绝 loopback：确定性失败且不创建连接。
    let peer = PeerClient::new(MetadataConfig::default())
        .unwrap()
        .with_observer(observer.clone());
    (
        Arc::new(WorkerResources::new(peer.clone(), peer.test_metrics())),
        observer,
    )
}

fn candidates(observer: &crate::observation::Observer) -> Vec<serde_json::Value> {
    observer
        .page(0, 200, &Default::default())
        .events
        .into_iter()
        .filter(|e| e["step"] == "candidate")
        .collect()
}

#[tokio::test]
async fn hints_filter_before_priority_and_unique_addresses_stop_at_eight() {
    for count in [0, 1, 2, 3, 10] {
        let (network, observer) = observed_network();
        let addresses: Vec<SocketAddr> = (1..=count)
            .map(|n| format!("127.0.0.1:{}", 6000 + n).parse().unwrap())
            .collect();
        let mut hints = vec![
            "0.0.0.0:6000".parse().unwrap(),
            "[::1]:6000".parse().unwrap(),
        ];
        for &address in &addresses {
            hints.extend([address, address]);
        }
        let outcome = run_job(
            candidate_job(hints),
            vec![],
            network.clone(),
            AddressPolicy::LocalUnicast,
            vec![AddressFamily::Ipv4],
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(outcome, Outcome::Retry(_)));
        let events = candidates(&observer);
        let selected: Vec<_> = events
            .iter()
            .filter(|e| e["result"] == "selected")
            .collect();
        assert_eq!(selected.len(), count.min(8));
        for (event, address) in selected.iter().zip(&addresses) {
            assert_eq!(event["data"]["peer"], address.to_string());
            assert_eq!(event["data"]["source"], "announce");
        }
        assert_eq!(
            events.iter().filter(|e| e["result"] == "duplicate").count(),
            if count >= 8 { 7 } else { count }
        );
        assert!(
            events
                .iter()
                .all(|e| e["result"] == "selected" || e["result"] == "duplicate"),
            "初始过滤不新增拒绝事件"
        );
        assert_eq!(network.metrics.report()["peer_attempts"], 0);
        assert_eq!(network.tcp.tracked_tcp_ips(), 0);
    }
}

#[tokio::test]
async fn cancellation_during_tcp_permit_wait_keeps_selection_without_connecting() {
    let (network, observer) = observed_network();
    let address = "127.0.0.1:6001".parse::<SocketAddr>().unwrap();
    let held = network.tcp.acquire_for_ip(address.ip()).await;
    let cancel = CancellationToken::new();
    let mut work = Box::pin(run_job(
        candidate_job(vec![address]),
        vec![],
        network.clone(),
        AddressPolicy::LocalUnicast,
        vec![AddressFamily::Ipv4],
        cancel.clone(),
    ));
    assert!(work.as_mut().now_or_never().is_none());
    let events = candidates(&observer);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["result"], "selected");
    assert_eq!(network.metrics.report()["peer_attempts"], 0);
    cancel.cancel();
    assert!(matches!(
        work.await,
        Outcome::Retry(RetryReason::Local(LocalReason::Cancelled))
    ));
    drop(held);
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
}

/// 已关闭 handle 的 lookup 与立即拒绝的 attempt 同时可就绪，控制故障优先。
#[tokio::test]
async fn ready_lookup_control_fault_precedes_ready_peer_failure() {
    let dir = tempfile::tempdir().unwrap();
    let f = fixture(dir.path(), AddressFamily::Ipv4).await;
    let handle = f.handle.clone();
    f.session.shutdown().await.unwrap();
    let (network, observer) = observed_network();
    let outcome = run_job(
        candidate_job(vec!["127.0.0.1:6001".parse().unwrap()]),
        vec![handle],
        network.clone(),
        AddressPolicy::LocalUnicast,
        vec![AddressFamily::Ipv4],
        CancellationToken::new(),
    )
    .await;
    assert!(matches!(outcome, Outcome::Control(_)));
    assert_eq!(candidates(&observer).len(), 1);
    assert!(
        !observer
            .page(0, 200, &Default::default())
            .events
            .iter()
            .any(|e| e["step"] == "tcp_permit"),
        "lookup 优先返回，attempt 尚未被 poll"
    );
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
}

/// 许可屏障让 lookup 先完成；随后必须继续消费通道，保持跨来源去重与八地址上限。
#[tokio::test]
async fn completed_lookup_queue_preserves_candidate_order_and_budget() {
    for streamed in [2, 10] {
        let dir = tempfile::tempdir().unwrap();
        let f = fixture(dir.path(), AddressFamily::Ipv4).await;
        let remote = udp(AddressFamily::Ipv4).await;
        let remote_address = remote.local_addr().unwrap();
        let a: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        let wrong_family: SocketAddr = "[::1]:6000".parse().unwrap();
        let delivered: Vec<SocketAddr> = (0..streamed)
            .map(|n| format!("127.0.0.1:{}", 6100 + n).parse().unwrap())
            .collect();
        let values: Vec<_> = [wrong_family, a]
            .into_iter()
            .chain(delivered.iter().copied())
            .map(|address| match address {
                SocketAddr::V4(a) => CompactPeerAddress::V4(a),
                SocketAddr::V6(a) => CompactPeerAddress::V6(a),
            })
            .collect();
        let server = tokio::spawn(async move {
            for method in [QueryMethod::Ping, QueryMethod::GetPeers] {
                let packet = remote.recv().await.unwrap();
                assert_eq!(packet.message.q, Some(method.clone()));
                let mut reply =
                    response(packet.message.t, AddressFamily::Ipv4, None, method.clone());
                if method == QueryMethod::GetPeers {
                    reply.r.as_mut().unwrap().values = Some(values.clone());
                }
                remote.send_to(packet.source, &reply).await.unwrap();
            }
        });
        f.handle
            .ping(RemoteNode {
                address: remote_address,
                expected_id: Some(NodeId([8; 20])),
            })
            .await
            .unwrap();
        let (network, observer) = observed_network();
        let held = network.tcp.acquire_for_ip(a.ip()).await;
        let work = tokio::spawn(run_job(
            candidate_job(vec!["0.0.0.0:6000".parse().unwrap(), wrong_family, a, a, b]),
            vec![f.handle.clone()],
            network.clone(),
            AddressPolicy::LocalUnicast,
            vec![AddressFamily::Ipv4],
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if observer
                    .page(0, 200, &Default::default())
                    .events
                    .iter()
                    .any(|e| e["step"] == "lookup" && e["result"] == "finished")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!work.is_finished());
        assert_eq!(
            candidates(&observer).len(),
            1,
            "等待许可期间不能提前选择下一个地址"
        );
        assert_eq!(f.handle.status().await.unwrap().pending, 0);
        drop(held);
        let outcome = tokio::time::timeout(Duration::from_secs(3), work)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, Outcome::Retry(RetryReason::Failed(_))));
        let events = candidates(&observer);
        let mut expected = vec![
            (a, "selected", Some("announce")),
            (a, "duplicate", None),
            (wrong_family, "address_family", None),
            (a, "duplicate", None),
        ];
        expected.extend(
            delivered
                .iter()
                .take(7)
                .map(|&a| (a, "selected", Some("dht"))),
        );
        if streamed < 7 {
            expected.push((b, "selected", Some("announce")));
        }
        assert_eq!(events.len(), expected.len());
        for (event, (address, result, source)) in events.iter().zip(expected) {
            assert_eq!(event["data"]["peer"], address.to_string());
            assert_eq!(event["result"], result);
            assert_eq!(event["data"]["source"], serde_json::json!(source));
        }
        assert_eq!(network.tcp.tracked_tcp_ips(), 0);
        assert_eq!(network.metrics.report()["peer_attempts"], 0);
        server.await.unwrap();
        f.session.shutdown().await.unwrap();
    }
}

/// DHT 允许本地部署地址，worker 使用更严格策略：同时违反策略和族时优先报告策略。
#[tokio::test]
async fn streamed_candidate_checks_address_policy_before_family() {
    let dir = tempfile::tempdir().unwrap();
    let f = fixture(dir.path(), AddressFamily::Ipv4).await;
    let remote = udp(AddressFamily::Ipv4).await;
    let remote_address = remote.local_addr().unwrap();
    let peer: SocketAddr = "127.0.0.1:6001".parse().unwrap();
    let server = tokio::spawn(async move {
        for method in [QueryMethod::Ping, QueryMethod::GetPeers] {
            let packet = remote.recv().await.unwrap();
            remote
                .send_to(
                    packet.source,
                    &response(
                        packet.message.t,
                        AddressFamily::Ipv4,
                        (method == QueryMethod::GetPeers).then_some(peer),
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
    let (network, observer) = observed_network();
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        run_job(
            candidate_job(vec![]),
            vec![f.handle.clone()],
            network.clone(),
            AddressPolicy::PublicOnly,
            vec![AddressFamily::Ipv6],
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        Outcome::Retry(RetryReason::Failed(
            super::super::failure::AttemptFailure::NoPeers
        ))
    ));
    let events = candidates(&observer);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["result"], "address_policy");
    assert_eq!(network.metrics.report()["peer_attempts"], 0);
    assert_eq!(network.tcp.tracked_tcp_ips(), 0);
    assert_eq!(f.handle.status().await.unwrap().pending, 0);
    server.await.unwrap();
    f.session.shutdown().await.unwrap();
}
