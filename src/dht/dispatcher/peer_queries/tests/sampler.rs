//! 主动采样 UDP 集成测试：只使用 loopback，并通过结果流和控制接口观察行为。
use super::*;
use crate::dht::dispatcher::SamplerConfig;
use crate::dht::dispatcher::SamplerError;
use crate::dht::krpc::CompactNodesV4;
use crate::dht::krpc::CompactNodesV6;
use crate::dht::krpc::InfoHashSamples;

fn config() -> SamplerConfig {
    SamplerConfig {
        address_policy: PeerAddressPolicy::LocalUnicast,
        send_spacing: Duration::from_millis(10),
        ..Default::default()
    }
}
fn sample_response(t: ByteBuf, v6: bool) -> KrpcMessage {
    let mut r = crate::dht::dispatcher::query::empty_response(NodeId([2; 20]));
    if v6 {
        r.nodes6 = Some(CompactNodesV6(vec![]));
    } else {
        r.nodes = Some(CompactNodesV4(vec![]));
    }
    r.samples = Some(InfoHashSamples(vec![HASH, HASH]));
    r.interval = Some(300);
    r.num = Some(1);
    KrpcMessage {
        t,
        y: MessageType::Response,
        q: None,
        a: None,
        r: Some(r),
        e: None,
        ro: None,
    }
}

/// 两个完整 dispatcher 联机：服务端先收到合法宣布，采样端随后从它读取 hash。
async fn full_roundtrip(v6: bool) {
    let Some((mut server, server_handle, peer)) = fixture(v6, 4096).await else {
        return;
    };
    let Some((mut client, client_handle, _unused)) = fixture(v6, 4096).await else {
        return;
    };
    let now = Instant::now();
    // 两个 fixture 默认 ID 相同，替换客户端路由 ID，避免把服务端误认为自己。
    client.routing = RoutingTable::new(
        NodeId([3; 20]),
        if v6 {
            AddressFamily::Ipv6
        } else {
            AddressFamily::Ipv4
        },
        now,
    );
    let server_address = server.transport.local_addr().unwrap();
    client
        .routing
        .observe_response(NodeId([1; 20]), server_address, now);
    let get = dispatch(&mut server, &peer, query(QueryMethod::GetPeers), now)
        .await
        .r
        .unwrap();
    let mut announce = query(QueryMethod::AnnouncePeer);
    let args = announce.a.as_mut().unwrap();
    args.token = get.token;
    args.implied_port = Some(1);
    assert_eq!(
        dispatch(&mut server, &peer, announce, now).await.y,
        MessageType::Response
    );
    let client_address = client.transport.local_addr().unwrap();
    let server_task = tokio::spawn(server.run());
    let client_task = tokio::spawn(client.run());
    let mut batches = client_handle.start_sampling(config()).await.unwrap();
    assert!(matches!(
        client_handle.start_sampling(config()).await,
        Err(SamplerError::AlreadyRunning)
    ));
    let batch = tokio::time::timeout(Duration::from_secs(2), batches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.samples, vec![HASH]);
    assert_eq!(batch.num, 1);
    assert_eq!(batch.interval, Duration::from_secs(300));
    assert_eq!(batch.responder.address, server_address);
    assert_eq!(batch.responder.id, NodeId([1; 20]));
    assert!(batch.received_at >= now);
    // 采样结果绝不能变成自身“有 peer 的 hash”，否则会污染对外提供的数据。
    let mut sample = query(QueryMethod::SampleInfohashes);
    let args = sample.a.as_mut().unwrap();
    args.info_hash = None;
    args.target = Some(NodeId([0; 20]));
    peer.send_to(client_address, &sample).await.unwrap();
    let r = receive(&peer).await.message.r.unwrap();
    assert_eq!(r.num, Some(0));
    assert!(r.samples.unwrap().0.is_empty());
    client_handle.stop_sampling().await.unwrap();
    client_handle.stop_sampling().await.unwrap();
    assert!(!client_handle.sampling_status().await.unwrap().running);
    assert!(batches.recv().await.is_none());
    client_handle.shutdown().await.unwrap();
    server_handle.shutdown().await.unwrap();
    client_task.await.unwrap().unwrap();
    server_task.await.unwrap().unwrap();
}

/// IPv4 服务闭环通过实际 socket 验证，不只测试编解码函数。
#[tokio::test]
async fn active_ipv4_service_roundtrip() {
    full_roundtrip(false).await;
}
/// IPv6 只在 fixture 明确识别环境不支持时跳过。
#[tokio::test]
async fn active_ipv6_service_roundtrip() {
    full_roundtrip(true).await;
}

/// 冒用来源地址的响应不能消费 transaction，合法来源仍能完成它；receiver 关闭会停止采样。
#[tokio::test]
async fn source_matching_and_receiver_drop() {
    let (mut dispatcher, handle, peer) = fixture(false, 4096).await.unwrap();
    let address = dispatcher.transport.local_addr().unwrap();
    dispatcher.routing.observe_response(
        NodeId([2; 20]),
        peer.local_addr().unwrap(),
        Instant::now(),
    );
    let task = tokio::spawn(dispatcher.run());
    let mut batches = handle.start_sampling(config()).await.unwrap();
    let request = receive(&peer).await.message;
    assert_eq!(request.q, Some(QueryMethod::SampleInfohashes));
    assert!(request.ro.is_none());
    assert!(request.a.unwrap().target.is_some());
    let impostor = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    impostor
        .send_to(address, &sample_response(request.t.clone(), false))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), batches.recv())
            .await
            .is_err()
    );
    peer.send_to(address, &sample_response(request.t, false))
        .await
        .unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(2), batches.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.samples, vec![HASH], "重复 hash 在批内去重");
    drop(batches);
    tokio::time::timeout(Duration::from_secs(2), async {
        while handle.sampling_status().await.unwrap().running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 204 触发一次 find_node，但普通节点查找式响应不产生伪造的空采样批次。
#[tokio::test]
async fn unsupported_rpc_falls_back_without_emitting_batch() {
    let (mut dispatcher, handle, peer) = fixture(false, 4096).await.unwrap();
    let address = dispatcher.transport.local_addr().unwrap();
    dispatcher.routing.observe_response(
        NodeId([2; 20]),
        peer.local_addr().unwrap(),
        Instant::now(),
    );
    let task = tokio::spawn(dispatcher.run());
    let mut batches = handle.start_sampling(config()).await.unwrap();
    let request = receive(&peer).await.message;
    peer.send_to(
        address,
        &KrpcMessage {
            t: request.t,
            y: MessageType::Error,
            q: None,
            a: None,
            r: None,
            e: Some((204, b"unsupported".to_vec().into())),
            ro: None,
        },
    )
    .await
    .unwrap();
    let fallback = receive(&peer).await.message;
    assert_eq!(fallback.q, Some(QueryMethod::FindNode));
    let mut response = sample_response(fallback.t, false);
    let r = response.r.as_mut().unwrap();
    r.samples = None;
    r.num = None;
    r.interval = None;
    peer.send_to(address, &response).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), batches.recv())
            .await
            .is_err()
    );
    let status = handle.sampling_status().await.unwrap();
    assert_eq!(status.unsupported, 1);
    assert_eq!(status.successful, 0);
    assert_eq!(status.in_flight, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.recv())
            .await
            .is_err()
    );
    handle.shutdown().await.unwrap();
    assert!(batches.recv().await.is_none());
    task.await.unwrap().unwrap();
}

/// 停止必须取消在途请求和预留槽位，旧响应及立即重启都不能绕过冷却。
#[tokio::test]
async fn stopping_cancels_pending_sampling_and_preserves_cooldown() {
    let (mut dispatcher, handle, peer) = fixture(false, 4096).await.unwrap();
    let address = dispatcher.transport.local_addr().unwrap();
    dispatcher.routing.observe_response(
        NodeId([2; 20]),
        peer.local_addr().unwrap(),
        Instant::now(),
    );
    let task = tokio::spawn(dispatcher.run());
    let mut batches = handle.start_sampling(config()).await.unwrap();
    let request = receive(&peer).await.message;
    handle.stop_sampling().await.unwrap();
    assert!(batches.recv().await.is_none());
    peer.send_to(address, &sample_response(request.t, false))
        .await
        .unwrap();
    let _next = handle.start_sampling(config()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv())
            .await
            .is_err()
    );
    assert_eq!(handle.sampling_status().await.unwrap().in_flight, 0);
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 发包失败立即撤销 transaction 和槽位，但不能据此把远端标记为失效。
#[tokio::test]
async fn send_failure_releases_capacity_without_penalizing_remote() {
    let (mut dispatcher, _handle, peer) = fixture(false, 16).await.unwrap();
    let now = Instant::now();
    dispatcher
        .routing
        .observe_response(NodeId([2; 20]), peer.local_addr().unwrap(), now);
    let _receiver = dispatcher.sampler.start(config(), 16, now).unwrap();
    dispatcher.advance_sampler(now).await;
    assert_eq!(dispatcher.transactions.len(), 0);
    assert!(dispatcher.pending.is_empty());
    assert_eq!(dispatcher.sampler.status().in_flight, 0);
    assert_eq!(dispatcher.sampler.status().failed, 0);
    assert_eq!(dispatcher.routing.closest_good(&[0; 20], 8, now).len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.recv())
            .await
            .is_err()
    );
}

/// 身份错误或方法字段错误结束查询，但不能产生批次；超时也要释放预留结果槽位。
#[tokio::test]
async fn malformed_identity_and_timeout_do_not_emit_results() {
    for case in 0..3 {
        let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
        let now = Instant::now();
        dispatcher
            .routing
            .observe_response(NodeId([2; 20]), peer.local_addr().unwrap(), now);
        let mut receiver = dispatcher.sampler.start(config(), 16, now).unwrap();
        dispatcher.advance_sampler(now).await;
        let request = receive(&peer).await.message;
        if case == 2 {
            dispatcher
                .expire_transactions(now + Duration::from_secs(2))
                .await;
        } else {
            let mut message = sample_response(request.t, false);
            if case == 0 {
                message.r.as_mut().unwrap().id = NodeId([8; 20]);
            } else {
                message.r.as_mut().unwrap().interval = Some(21601);
            }
            dispatcher
                .handle_response(
                    ReceivedMessage {
                        source: peer.local_addr().unwrap(),
                        encoded_len: 100,
                        message,
                    },
                    now,
                )
                .await;
        }
        assert_eq!(dispatcher.sampler.status().failed, 1);
        assert_eq!(dispatcher.sampler.status().in_flight, 0);
        assert_eq!(dispatcher.transactions.len(), 0);
        assert!(dispatcher.pending.is_empty());
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        dispatcher.stop_sampler(now);
        assert!(receiver.recv().await.is_none());
    }
}

/// 即使 transaction 总容量很小，采样占用一条后仍能发送用户 ping。
#[tokio::test]
async fn sampling_preserves_the_last_user_transaction() {
    let (mut dispatcher, handle, peer) = fixture(false, 4096).await.unwrap();
    dispatcher.transactions = TransactionManager::new(Duration::from_secs(2), 2);
    dispatcher.routing.observe_response(
        NodeId([2; 20]),
        peer.local_addr().unwrap(),
        Instant::now(),
    );
    let address = dispatcher.transport.local_addr().unwrap();
    let task = tokio::spawn(dispatcher.run());
    let mut receiver = handle
        .start_sampling(SamplerConfig {
            parallelism: 1,
            ..config()
        })
        .await
        .unwrap();
    let _sampling = receive(&peer).await.message;
    let user_handle = handle.clone();
    let remote = peer.local_addr().unwrap();
    let ping = tokio::spawn(async move {
        user_handle
            .ping(crate::dht::dispatcher::RemoteNode {
                address: remote,
                expected_id: Some(NodeId([2; 20])),
            })
            .await
    });
    let request = receive(&peer).await.message;
    assert_eq!(request.q, Some(QueryMethod::Ping));
    let mut message = sample_response(request.t, false);
    message.r = Some(crate::dht::dispatcher::query::empty_response(NodeId(
        [2; 20],
    )));
    peer.send_to(address, &message).await.unwrap();
    assert_eq!(ping.await.unwrap().unwrap().responder_id, NodeId([2; 20]));
    handle.shutdown().await.unwrap();
    assert!(receiver.recv().await.is_none());
    task.await.unwrap().unwrap();
}

/// 响应提供的新联系人先成为候选，收到它自己的合法响应后才允许加入路由表。
#[tokio::test]
async fn discovered_contact_is_followed_but_not_trusted_before_reply() {
    let (mut dispatcher, _handle, seed) = fixture(false, 4096).await.unwrap();
    // 用另一个 loopback IP，避免这个测试绕过实际启用的“同 IP 冷却”。
    let discovered = UdpTransport::bind("127.0.0.2:0", Default::default())
        .await
        .unwrap();
    let now = Instant::now();
    dispatcher
        .routing
        .observe_response(NodeId([2; 20]), seed.local_addr().unwrap(), now);
    let mut receiver = dispatcher.sampler.start(config(), 16, now).unwrap();
    dispatcher.advance_sampler(now).await;
    let request = receive(&seed).await.message;
    let mut message = sample_response(request.t, false);
    message.r.as_mut().unwrap().nodes =
        Some(CompactNodesV4(vec![crate::dht::krpc::CompactNodeV4 {
            id: NodeId([3; 20]),
            address: match discovered.local_addr().unwrap() {
                SocketAddr::V4(address) => address,
                _ => unreachable!(),
            },
        }]));
    dispatcher
        .handle_response(
            ReceivedMessage {
                source: seed.local_addr().unwrap(),
                encoded_len: 150,
                message,
            },
            now,
        )
        .await;
    receiver.recv().await.unwrap();
    assert!(dispatcher.routing.contact(NodeId([3; 20])).is_none());
    dispatcher
        .advance_sampler(now + Duration::from_millis(10))
        .await;
    let request = receive(&discovered).await.message;
    assert_eq!(request.q, Some(QueryMethod::SampleInfohashes));
    assert!(dispatcher.routing.contact(NodeId([3; 20])).is_none());
    let mut message = sample_response(request.t, false);
    message.r.as_mut().unwrap().id = NodeId([3; 20]);
    dispatcher
        .handle_response(
            ReceivedMessage {
                source: discovered.local_addr().unwrap(),
                encoded_len: 100,
                message,
            },
            now + Duration::from_millis(10),
        )
        .await;
    assert_eq!(receiver.recv().await.unwrap().responder.id, NodeId([3; 20]));
    assert_eq!(
        dispatcher.routing.contact(NodeId([3; 20])).unwrap().address,
        discovered.local_addr().unwrap()
    );
    assert!(dispatcher.peers.next_deadline().is_none());
    dispatcher.stop_sampler(now);
}
