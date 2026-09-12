//! BEP 51 服务端测试：复用 peer 宣布流程和真实 UDP 编解码。
use super::*;

fn sample_query() -> KrpcMessage {
    let mut message = query(QueryMethod::SampleInfohashes);
    let args = message.a.as_mut().unwrap();
    args.info_hash = None;
    args.target = Some(NodeId([0; 20]));
    message
}

/// 通过实际 UDP 完成获取 token、宣布、采样；查询过但没宣布的 hash 不得泄漏。
async fn roundtrip(v6: bool) {
    let Some((dispatcher, handle, peer)) = fixture(v6, 4096).await else {
        return;
    };
    let address = dispatcher.transport.local_addr().unwrap();
    let task = tokio::spawn(dispatcher.run());
    peer.send_to(address, &query(QueryMethod::GetPeers))
        .await
        .unwrap();
    let token = receive(&peer).await.message.r.unwrap().token.unwrap();
    let mut announce = query(QueryMethod::AnnouncePeer);
    announce.a.as_mut().unwrap().token = Some(token);
    announce.a.as_mut().unwrap().implied_port = Some(1);
    peer.send_to(address, &announce).await.unwrap();
    assert_eq!(receive(&peer).await.message.y, MessageType::Response);
    let mut unannounced = query(QueryMethod::GetPeers);
    unannounced.a.as_mut().unwrap().info_hash = Some(InfoHashV1([8; 20]));
    peer.send_to(address, &unannounced).await.unwrap();
    receive(&peer).await;
    peer.send_to(address, &sample_query()).await.unwrap();
    let response = receive(&peer).await;
    assert!(response.encoded_len <= 1024);
    let r = response.message.r.unwrap();
    assert_eq!(r.id, NodeId([1; 20]));
    assert_eq!(r.samples.unwrap().0, vec![HASH]);
    assert_eq!(r.num, Some(1));
    assert_eq!(r.interval, Some(300));
    assert_eq!(r.nodes.is_some(), !v6);
    assert_eq!(r.nodes6.is_some(), v6);
    assert!(r.token.is_none() && r.values.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.recv())
            .await
            .is_err()
    );
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// IPv4 的采样也必须来自已经完成 token 校验的宣布。
#[tokio::test]
async fn ipv4_announce_to_sample_roundtrip() {
    roundtrip(false).await;
}

/// IPv6 使用 nodes6，但 hash 的 wire 格式仍是连续的 20 字节记录。
#[tokio::test]
async fn ipv6_announce_to_sample_roundtrip() {
    roundtrip(true).await;
}

/// 空样本必须编码为空字节串，不能靠缺省字段假装支持 BEP 51。
#[tokio::test]
async fn empty_samples_and_want_fields_are_present() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    for (want, n4, n6) in [
        (vec![], true, false),
        (vec!["future"], true, false),
        (vec!["n6"], false, true),
        (vec!["n4", "n6"], true, true),
    ] {
        let mut message = sample_query();
        message.a.as_mut().unwrap().want = want.into_iter().map(str::to_owned).collect();
        let response = dispatch(&mut dispatcher, &peer, message, Instant::now()).await;
        let encoded = bendy::serde::to_bytes(&response).unwrap();
        assert!(
            encoded
                .windows(b"7:samples0:".len())
                .any(|bytes| bytes == b"7:samples0:")
        );
        let r = response.r.unwrap();
        assert_eq!(r.num, Some(0));
        assert_eq!(r.interval, Some(300));
        assert!(r.samples.unwrap().0.is_empty());
        assert_eq!(r.nodes.is_some(), n4);
        assert_eq!(r.nodes6.is_some(), n6);
    }
}

/// 非法请求不能先生成空快照；紧接着的合法请求应立刻看到刚宣布的 hash。
#[tokio::test]
async fn invalid_queries_do_not_initialize_cache_or_verify_sender() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    for case in 0..5 {
        let mut message = sample_query();
        message.ro = None;
        let args = message.a.as_mut().unwrap();
        match case {
            0 => args.target = None,
            1 => args.info_hash = Some(HASH),
            2 => args.port = Some(6881),
            3 => args.token = Some(Token(vec![0; 32].into())),
            _ => args.implied_port = Some(1),
        }
        assert_eq!(
            dispatch(&mut dispatcher, &peer, message, now)
                .await
                .e
                .unwrap()
                .0,
            203
        );
        assert!(dispatcher.verifications.is_empty());
    }
    dispatcher
        .peers
        .announce(HASH, peer.local_addr().unwrap(), now)
        .unwrap();
    assert_eq!(
        dispatch(&mut dispatcher, &peer, sample_query(), now)
            .await
            .r
            .unwrap()
            .samples
            .unwrap()
            .0,
        vec![HASH]
    );
}

/// target 只改变节点顺序，不改变相同缓存轮次里的样本；普通请求仍需反向验证。
#[tokio::test]
async fn target_affects_only_nodes_and_normal_query_requires_verification() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    dispatcher
        .peers
        .announce(HASH, peer.local_addr().unwrap(), now)
        .unwrap();
    for value in [3, 4] {
        dispatcher.routing.observe_response(
            NodeId([value; 20]),
            SocketAddr::new(peer.local_addr().unwrap().ip(), 2000 + u16::from(value)),
            now,
        );
    }
    let mut first = sample_query();
    first.a.as_mut().unwrap().target = Some(NodeId([3; 20]));
    let first = dispatch(&mut dispatcher, &peer, first, now)
        .await
        .r
        .unwrap();
    let mut second = sample_query();
    second.a.as_mut().unwrap().target = Some(NodeId([4; 20]));
    second.ro = None;
    let second = dispatch(&mut dispatcher, &peer, second, now)
        .await
        .r
        .unwrap();
    assert_eq!(first.samples, second.samples);
    assert_eq!(first.nodes.unwrap().0[0].id, NodeId([3; 20]));
    assert_eq!(second.nodes.unwrap().0[0].id, NodeId([4; 20]));
    assert_eq!(receive(&peer).await.message.q, Some(QueryMethod::Ping));
    assert_eq!(dispatcher.verifications.len(), 1);
}

/// 长 ID 和小 transport 上限只裁剪响应副本；必需字段始终保留，且不发送超大报文。
#[tokio::test]
async fn response_budget_trims_samples_before_nodes_and_keeps_cache() {
    for limit in [1024, 180] {
        let (mut dispatcher, _handle, peer) = fixture(false, limit).await.unwrap();
        let now = Instant::now();
        for value in 0..40 {
            dispatcher
                .peers
                .announce(InfoHashV1([value; 20]), peer.local_addr().unwrap(), now)
                .unwrap();
        }
        for value in 3..11 {
            dispatcher.routing.observe_response(
                NodeId([value; 20]),
                SocketAddr::new(peer.local_addr().unwrap().ip(), 2000 + u16::from(value)),
                now,
            );
        }
        let mut message = sample_query();
        message.t = vec![42; if limit == 1024 { 600 } else { 1 }].into();
        let response = dispatch(&mut dispatcher, &peer, copy_message(&message), now).await;
        assert!(bendy::serde::to_bytes(&response).unwrap().len() <= limit);
        assert_eq!(response.t, message.t);
        let r = response.r.unwrap();
        assert_eq!(r.num, Some(40));
        assert_eq!(r.interval, Some(300));
        if limit == 1024 {
            assert_eq!(r.nodes.unwrap().0.len(), 8);
            assert!(r.samples.unwrap().0.len() < 32);
            assert_eq!(
                dispatch(&mut dispatcher, &peer, sample_query(), now)
                    .await
                    .r
                    .unwrap()
                    .samples
                    .unwrap()
                    .0
                    .len(),
                32
            );
        } else {
            assert!(r.samples.unwrap().0.is_empty());
            assert!(r.nodes.unwrap().0.len() < 8);
        }
        message.t = vec![42; 1024].into();
        message.ro = None;
        dispatcher
            .handle_query(
                ReceivedMessage {
                    source: peer.local_addr().unwrap(),
                    message,
                    encoded_len: 1100,
                },
                now,
            )
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), peer.recv())
                .await
                .is_err()
        );
        assert!(dispatcher.verifications.is_empty());
    }
}

/// num 实时反映有效 hash，即使缓存还没刷新；TTL 到期后响应也不能含旧样本。
#[tokio::test]
async fn live_count_changes_without_refilling_cached_samples() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let start = Instant::now();
    dispatcher
        .peers
        .announce(HASH, peer.local_addr().unwrap(), start)
        .unwrap();
    assert_eq!(
        dispatch(&mut dispatcher, &peer, sample_query(), start)
            .await
            .r
            .unwrap()
            .num,
        Some(1)
    );
    dispatcher
        .peers
        .announce(
            InfoHashV1([8; 20]),
            peer.local_addr().unwrap(),
            start + Duration::from_secs(10),
        )
        .unwrap();
    let r = dispatch(
        &mut dispatcher,
        &peer,
        sample_query(),
        start + Duration::from_secs(10),
    )
    .await
    .r
    .unwrap();
    assert_eq!(r.num, Some(2));
    assert_eq!(r.samples.unwrap().0, vec![HASH]);
    let r = dispatch(
        &mut dispatcher,
        &peer,
        sample_query(),
        start + Duration::from_secs(30),
    )
    .await
    .r
    .unwrap();
    assert_eq!(r.num, Some(1));
    assert!(r.samples.unwrap().0.is_empty());
    assert!(
        dispatcher.peers.next_deadline().is_some(),
        "这里没有依赖物理清理才能过滤过期样本"
    );
}

/// IPv6 满节点列表加 32 个 hash 超过预算，仍须保留所有节点并按 20 字节裁剪样本。
#[tokio::test]
async fn ipv6_full_sampling_response_fits_actual_udp_payload() {
    let Some((mut dispatcher, _handle, peer)) = fixture(true, 4096).await else {
        return;
    };
    let now = Instant::now();
    for value in 0..40 {
        dispatcher
            .peers
            .announce(InfoHashV1([value; 20]), peer.local_addr().unwrap(), now)
            .unwrap();
    }
    for value in 3..11 {
        dispatcher.routing.observe_response(
            NodeId([value; 20]),
            SocketAddr::new(peer.local_addr().unwrap().ip(), 2000 + u16::from(value)),
            now,
        );
    }
    let mut message = sample_query();
    message.t = vec![42; 100].into();
    dispatcher
        .handle_query(
            ReceivedMessage {
                source: peer.local_addr().unwrap(),
                encoded_len: 200,
                message,
            },
            now,
        )
        .await;
    let received = receive(&peer).await;
    assert!(received.encoded_len <= 1024);
    let response = received.message.r.unwrap();
    assert_eq!(response.num, Some(40));
    assert_eq!(response.interval, Some(300));
    assert!(response.nodes.is_none());
    assert_eq!(response.nodes6.unwrap().0.len(), 8);
    let samples = response.samples.unwrap().0;
    assert!(!samples.is_empty() && samples.len() < 32);
    assert_eq!(
        samples
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        samples.len()
    );
}
