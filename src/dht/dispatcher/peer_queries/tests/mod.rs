//! 全部数据报仅发往 loopback；本地地址策略必须由测试显式开启。

use super::*;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::dispatcher::DhtHandle;
use crate::dht::dispatcher::MaintenanceConfig;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryMethod;
use crate::dht::krpc::Token;
use crate::dht::peer_store::PeerAddressPolicy;
use crate::dht::peer_store::PeerStoreConfig;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::RoutingTable;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::ReceivedMessage;
use crate::dht::udp::UdpTransport;
use crate::dht::udp::UdpTransportConfig;
use crate::dht::udp::UdpTransportError;
use crate::info_hash::InfoHashV1;
use std::time::Duration;

const HASH: InfoHashV1 = InfoHashV1([9; 20]);

// BEP 51 复用这里的本机 UDP 测试工具，不额外启动公网服务。
mod sampler;
mod sampling;

/// 经过 wire 编解码复制请求，也顺便确保测试构造的是可传输的消息。
fn copy_message(message: &KrpcMessage) -> KrpcMessage {
    bendy::serde::from_bytes(&bendy::serde::to_bytes(message).unwrap()).unwrap()
}

/// IPv6 只允许因系统不支持或没有 loopback 地址而跳过，其他错误直接失败。
async fn fixture(v6: bool, limit: usize) -> Option<(DhtDispatcher, DhtHandle, UdpTransport)> {
    let bind = if v6 { "[::1]:0" } else { "127.0.0.1:0" };
    let transport = match UdpTransport::bind(
        bind,
        UdpTransportConfig {
            max_message_size: limit,
        },
    )
    .await
    {
        Ok(value) => value,
        Err(UdpTransportError::Io(error))
            if v6
                && (error.kind() == std::io::ErrorKind::AddrNotAvailable
                    || matches!(error.raw_os_error(), Some(97 | 93))) =>
        {
            eprintln!("跳过 IPv6 环境测试：{error}");
            return None;
        }
        Err(error) => panic!("loopback 绑定失败：{error}"),
    };
    let family = if v6 {
        AddressFamily::Ipv6
    } else {
        AddressFamily::Ipv4
    };
    let config = DhtDispatcherConfig {
        maintenance: MaintenanceConfig {
            enabled: false,
            ..Default::default()
        },
        peer_store: PeerStoreConfig {
            address_policy: PeerAddressPolicy::LocalUnicast,
            ttl: Duration::from_secs(30),
            ..Default::default()
        },
        ..Default::default()
    };
    let (dispatcher, handle) = DhtDispatcher::with_config(
        transport,
        RoutingTable::new(NodeId([1; 20]), family, Instant::now()),
        TransactionManager::new(Duration::from_secs(1), 16),
        config,
    )
    .unwrap();
    let peer = UdpTransport::bind(bind, Default::default()).await.unwrap();
    Some((dispatcher, handle, peer))
}

fn query(method: QueryMethod) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(b"peer-query".to_vec()),
        y: MessageType::Query,
        q: Some(method),
        a: Some(QueryArgs {
            id: NodeId([2; 20]),
            target: None,
            info_hash: Some(HASH),
            port: None,
            token: None,
            implied_port: None,
            want: vec![],
        }),
        r: None,
        e: None,
        ro: Some(1),
    }
}

async fn receive(peer: &UdpTransport) -> ReceivedMessage {
    tokio::time::timeout(Duration::from_secs(2), peer.recv())
        .await
        .expect("响应不能一直等待")
        .unwrap()
}

/// 直接控制处理时刻，避免为令牌过期测试真实等待十分钟。
async fn dispatch(
    dispatcher: &mut DhtDispatcher,
    peer: &UdpTransport,
    message: KrpcMessage,
    now: Instant,
) -> KrpcMessage {
    dispatcher
        .handle_query(
            ReceivedMessage {
                source: peer.local_addr().unwrap(),
                encoded_len: bendy::serde::to_bytes(&message).unwrap().len(),
                message,
            },
            now,
        )
        .await;
    receive(peer).await.message
}

/// 真正穿过 UDP 和 run 事件循环，检查 token、显式端口、implied_port 和双栈 compact 编码。
async fn service_roundtrip(v6: bool) {
    let Some((dispatcher, handle, peer)) = fixture(v6, 4096).await else {
        return;
    };
    let address = dispatcher.transport.local_addr().unwrap();
    let task = tokio::spawn(dispatcher.run());
    let get = query(QueryMethod::GetPeers);
    peer.send_to(address, &get).await.unwrap();
    let first = receive(&peer).await.message.r.unwrap();
    assert!(first.values.is_none());
    assert_eq!(first.nodes6.is_some(), v6);
    assert_eq!(first.nodes.is_some(), !v6);
    let token = first.token.unwrap();
    assert_eq!(token.0.len(), 32);
    // 同一个 IP 换了 UDP 来源端口，之前拿到的 token 仍然有效。
    let second = UdpTransport::bind(
        if v6 { "[::1]:0" } else { "127.0.0.1:0" },
        Default::default(),
    )
    .await
    .unwrap();
    for implied in [None, Some(1)] {
        let mut announce = query(QueryMethod::AnnouncePeer);
        let args = announce.a.as_mut().unwrap();
        args.token = Some(token.clone());
        args.port = Some(6881);
        args.implied_port = implied;
        second.send_to(address, &announce).await.unwrap();
        let ack = receive(&second).await.message;
        assert_eq!(ack.y, MessageType::Response);
        assert_eq!(ack.r.unwrap().id, NodeId([1; 20]));
    }
    peer.send_to(address, &get).await.unwrap();
    let response = receive(&peer).await;
    assert!(response.encoded_len <= 1024);
    let values = response.message.r.unwrap().values.unwrap();
    for port in [6881, second.local_addr().unwrap().port()] {
        let expected = SocketAddr::new(second.local_addr().unwrap().ip(), port);
        assert!(values.iter().any(|value| match value {
            CompactPeerAddress::V4(a) => SocketAddr::V4(*a) == expected,
            CompactPeerAddress::V6(a) => SocketAddr::V6(*a) == expected,
        }));
    }
    // ro=1 可以写入和读取，但不会额外收到反向验证 ping。
    assert!(
        tokio::time::timeout(Duration::from_millis(20), second.recv())
            .await
            .is_err()
    );
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 本机 IPv4 完成获取 token、合法宣布及读取 peer 的往返。
#[tokio::test]
async fn ipv4_peer_service_roundtrip() {
    service_roundtrip(false).await;
}

/// 本机 IPv6 使用对应地址编码完成 peer 服务往返。
#[tokio::test]
async fn ipv6_peer_service_roundtrip() {
    service_roundtrip(true).await;
}

/// 错误请求既不能写缓存，也不能触发反向 ping；错误 token 不能给已有记录续命。
#[tokio::test]
async fn invalid_announces_do_not_write_or_verify() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    let mut valid = query(QueryMethod::AnnouncePeer);
    valid.ro = None;
    let args = valid.a.as_mut().unwrap();
    args.token = Some(
        dispatcher
            .tokens
            .issue(peer.local_addr().unwrap().ip(), now)
            .unwrap(),
    );
    args.port = Some(6881);
    for case in 0..10 {
        let mut invalid = copy_message(&valid);
        let args = invalid.a.as_mut().unwrap();
        match case {
            0 => args.info_hash = None,
            1 => args.token = None,
            2 => args.port = None,
            3 => args.port = Some(0),
            4 => args.implied_port = Some(2),
            5 => args.target = Some(NodeId([3; 20])),
            6 => args.want.push("n4".into()),
            7 => args.token = Some(Token(ByteBuf::from(vec![0; 32]))),
            8 => {
                args.token = Some(
                    dispatcher
                        .tokens
                        .issue("127.0.0.2".parse().unwrap(), now)
                        .unwrap(),
                )
            }
            _ => args.token = Some(Token(ByteBuf::from(vec![]))),
        }
        let response = dispatch(&mut dispatcher, &peer, invalid, now).await;
        assert_eq!(response.e.unwrap().0, 203, "case {case}");
        assert!(dispatcher.peers.next_deadline().is_none());
        assert!(dispatcher.verifications.is_empty());
    }
    valid.ro = Some(1);
    assert_eq!(
        dispatch(&mut dispatcher, &peer, copy_message(&valid), now)
            .await
            .y,
        MessageType::Response
    );
    let deadline = dispatcher.peers.next_deadline();
    valid.a.as_mut().unwrap().token = Some(Token(ByteBuf::from(vec![0; 32])));
    assert_eq!(
        dispatch(&mut dispatcher, &peer, valid, now + Duration::from_secs(20))
            .await
            .y,
        MessageType::Error
    );
    assert_eq!(dispatcher.peers.next_deadline(), deadline);
}

/// 缺少 hash 或携带其他方法参数时返回 203；只查询一个 hash 不会创建缓存记录。
#[tokio::test]
async fn invalid_get_peers_does_not_create_hashes() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    for case in 0..5 {
        let mut message = query(QueryMethod::GetPeers);
        let args = message.a.as_mut().unwrap();
        match case {
            0 => args.info_hash = None,
            1 => args.target = Some(NodeId([0; 20])),
            2 => args.port = Some(6881),
            3 => args.token = Some(Token(ByteBuf::from(vec![]))),
            _ => args.implied_port = Some(1),
        }
        assert_eq!(
            dispatch(&mut dispatcher, &peer, message, Instant::now())
                .await
                .e
                .unwrap()
                .0,
            203
        );
    }
    assert_eq!(
        dispatch(
            &mut dispatcher,
            &peer,
            query(QueryMethod::GetPeers),
            Instant::now()
        )
        .await
        .y,
        MessageType::Response
    );
    assert!(dispatcher.peers.next_deadline().is_none());
}

/// want 的已知值控制 nodes 字段，未知值不会阻止默认地址族响应。
#[tokio::test]
async fn want_is_shared_by_both_lookup_methods() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    for method in [QueryMethod::GetPeers, QueryMethod::FindNode] {
        for (want, n4, n6) in [
            (vec![], true, false),
            (vec!["future"], true, false),
            (vec!["n6"], false, true),
            (vec!["n4", "n6", "future"], true, true),
        ] {
            let mut message = query(method.clone());
            let args = message.a.as_mut().unwrap();
            args.want = want.into_iter().map(str::to_owned).collect();
            if method == QueryMethod::FindNode {
                args.info_hash = None;
                args.target = Some(NodeId(HASH.0));
            }
            let r = dispatch(&mut dispatcher, &peer, message, Instant::now())
                .await
                .r
                .unwrap();
            assert_eq!(r.nodes.is_some(), n4);
            assert_eq!(r.nodes6.is_some(), n6);
            assert!(r.nodes6.is_none_or(|nodes| nodes.0.is_empty()));
        }
    }
}

/// 合法 token 仅授权宣布；路由表仍要另发 ping 验证发送方。
#[tokio::test]
async fn successful_announce_still_requires_routing_verification() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    let mut message = query(QueryMethod::AnnouncePeer);
    message.ro = None;
    let args = message.a.as_mut().unwrap();
    args.token = Some(
        dispatcher
            .tokens
            .issue(peer.local_addr().unwrap().ip(), now)
            .unwrap(),
    );
    args.implied_port = Some(1);
    assert_eq!(
        dispatch(&mut dispatcher, &peer, message, now).await.y,
        MessageType::Response
    );
    assert_eq!(receive(&peer).await.message.q, Some(QueryMethod::Ping));
    assert!(dispatcher.routing.closest_good(&HASH.0, 8, now).is_empty());
    assert_eq!(dispatcher.verifications.len(), 1);
    assert_eq!(
        dispatcher.peers.sample(HASH, 32, now, &mut rand::rng()),
        vec![peer.local_addr().unwrap()]
    );
}

/// 真实 UDP payload 受预算限制；先去掉 peers，再去掉较远节点，transaction ID 不变。
#[tokio::test]
async fn response_budget_preserves_required_fields() {
    for limit in [1024, 250] {
        let (mut dispatcher, _handle, peer) = fixture(false, limit).await.unwrap();
        let now = Instant::now();
        for port in 1000..1100 {
            dispatcher
                .peers
                .announce(
                    HASH,
                    SocketAddr::new(peer.local_addr().unwrap().ip(), port),
                    now,
                )
                .unwrap();
        }
        for value in 2..10 {
            dispatcher.routing.observe_response(
                NodeId([value; 20]),
                SocketAddr::new(peer.local_addr().unwrap().ip(), 2000 + u16::from(value)),
                now,
            );
        }
        let mut message = query(QueryMethod::GetPeers);
        message.t = ByteBuf::from(vec![42; if limit == 1024 { 600 } else { 80 }]);
        let response = dispatch(&mut dispatcher, &peer, copy_message(&message), now).await;
        assert!(bendy::serde::to_bytes(&response).unwrap().len() <= limit);
        assert_eq!(response.t, message.t);
        let r = response.r.unwrap();
        assert!(r.nodes.is_some());
        assert_eq!(r.token.unwrap().0.len(), 32);
        if limit == 250 {
            assert!(r.values.is_none());
            assert!(r.nodes.unwrap().0.len() < 8);
        }
    }
}

/// 即使确认报文放不下，合法宣布也已经保存；超长错误响应同样静默丢弃。
#[tokio::test]
async fn oversized_ack_does_not_undo_announce() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    let mut message = query(QueryMethod::AnnouncePeer);
    message.t = ByteBuf::from(vec![42; 1024]);
    let args = message.a.as_mut().unwrap();
    args.token = Some(
        dispatcher
            .tokens
            .issue(peer.local_addr().unwrap().ip(), now)
            .unwrap(),
    );
    args.implied_port = Some(1);
    dispatcher
        .handle_query(
            ReceivedMessage {
                source: peer.local_addr().unwrap(),
                message: copy_message(&message),
                encoded_len: 1200,
            },
            now,
        )
        .await;
    assert!(dispatcher.peers.next_deadline().is_some());
    message.a.as_mut().unwrap().token = None;
    dispatcher
        .handle_query(
            ReceivedMessage {
                source: peer.local_addr().unwrap(),
                message,
                encoded_len: 1200,
            },
            now,
        )
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.recv())
            .await
            .is_err()
    );
}

/// 到期记录即使还没清理，也不能返回；两个轮换周期后的 token 不能继续宣布。
#[tokio::test]
async fn expired_peers_fall_back_to_nodes_and_old_tokens_fail() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    dispatcher
        .peers
        .announce(HASH, peer.local_addr().unwrap(), now)
        .unwrap();
    let token = dispatcher
        .tokens
        .issue(peer.local_addr().unwrap().ip(), now)
        .unwrap();
    let response = dispatch(
        &mut dispatcher,
        &peer,
        query(QueryMethod::GetPeers),
        now + Duration::from_secs(30),
    )
    .await
    .r
    .unwrap();
    assert!(response.values.is_none());
    assert!(response.nodes.is_some());
    let mut message = query(QueryMethod::AnnouncePeer);
    let args = message.a.as_mut().unwrap();
    args.token = Some(token);
    args.implied_port = Some(1);
    assert_eq!(
        dispatch(
            &mut dispatcher,
            &peer,
            message,
            now + Duration::from_secs(600)
        )
        .await
        .e
        .unwrap()
        .0,
        203
    );
}

/// 运行时随机源故障返回 202；不能把内部故障误报成 token 无效，也不能写入数据。
#[tokio::test]
async fn entropy_failure_returns_server_error_without_writing() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    let token = dispatcher
        .tokens
        .issue(peer.local_addr().unwrap().ip(), now)
        .unwrap();
    dispatcher.tokens.fail_next_rotations();
    let later = now + Duration::from_secs(301);
    assert_eq!(
        dispatch(&mut dispatcher, &peer, query(QueryMethod::GetPeers), later)
            .await
            .e
            .unwrap()
            .0,
        202
    );
    let mut announce = query(QueryMethod::AnnouncePeer);
    let args = announce.a.as_mut().unwrap();
    args.token = Some(token);
    args.implied_port = Some(1);
    assert_eq!(
        dispatch(&mut dispatcher, &peer, announce, later)
            .await
            .e
            .unwrap()
            .0,
        202
    );
    assert!(dispatcher.peers.next_deadline().is_none());
    assert!(dispatcher.verifications.is_empty());
}

/// IPv6 的 32 个 peer 加 8 个节点会超限，裁剪后仍保留 token 和所有最近节点。
#[tokio::test]
async fn ipv6_full_response_is_trimmed_to_udp_budget() {
    let Some((mut dispatcher, _handle, peer)) = fixture(true, 4096).await else {
        return;
    };
    let now = Instant::now();
    for port in 1000..1032 {
        dispatcher
            .peers
            .announce(
                HASH,
                SocketAddr::new(peer.local_addr().unwrap().ip(), port),
                now,
            )
            .unwrap();
    }
    for value in 2..10 {
        dispatcher.routing.observe_response(
            NodeId([value; 20]),
            SocketAddr::new(peer.local_addr().unwrap().ip(), 2000 + u16::from(value)),
            now,
        );
    }
    let r = dispatch(&mut dispatcher, &peer, query(QueryMethod::GetPeers), now).await;
    assert!(bendy::serde::to_bytes(&r).unwrap().len() <= 1024);
    let r = r.r.unwrap();
    assert!(r.nodes.is_none());
    assert_eq!(r.nodes6.unwrap().0.len(), 8);
    let peers = r.values.unwrap();
    assert!(!peers.is_empty() && peers.len() < 32);
    assert!(
        peers
            .iter()
            .all(|peer| matches!(peer, CompactPeerAddress::V6(_)))
    );
}

/// 默认公网策略不能被合法 token 绕过：loopback 只有显式允许后才能存储。
#[tokio::test]
async fn public_policy_rejects_local_announce_even_with_valid_token() {
    let (mut dispatcher, _handle, peer) = fixture(false, 4096).await.unwrap();
    let now = Instant::now();
    dispatcher.peers = crate::dht::peer_store::PeerStore::new(
        PeerStoreConfig::default(),
        AddressFamily::Ipv4,
        now,
    )
    .unwrap();
    let mut message = query(QueryMethod::AnnouncePeer);
    message.ro = None;
    let args = message.a.as_mut().unwrap();
    args.token = Some(
        dispatcher
            .tokens
            .issue(peer.local_addr().unwrap().ip(), now)
            .unwrap(),
    );
    args.implied_port = Some(1);
    assert_eq!(
        dispatch(&mut dispatcher, &peer, message, now)
            .await
            .e
            .unwrap()
            .0,
        203
    );
    assert!(dispatcher.peers.next_deadline().is_none());
    assert!(dispatcher.verifications.is_empty());
}
