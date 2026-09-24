//! 驱动真实 loopback dispatcher，观察回复、transaction 和路由状态；不访问公共 DHT。
use super::*;
use bendy::serde::{Deserializer, to_bytes};
use serde_bytes::ByteBuf;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

use crate::dht::krpc::CompactNodeV4;
use crate::dht::krpc::CompactNodesV4;
use crate::dht::krpc::CompactNodesV6;
use crate::dht::krpc::KrpcErrorCode;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryArgs;
use crate::dht::krpc::QueryMethod;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::BUCKET_SIZE;
use crate::dht::routing::GOOD_FOR;
use crate::dht::routing::RoutingTable;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;
use crate::dht::udp::UdpTransportError;

/// 创建一个 IPv4 dispatcher，并返回它的查询入口、监听地址和后台任务。
async fn start_ipv4_dispatcher(
    local_id: NodeId,
    timeout: Duration,
    max_pending: usize,
    routing: Option<RoutingTable>,
) -> (
    DhtHandle,
    SocketAddr,
    JoinHandle<Result<(), DispatcherError>>,
) {
    let transport = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .expect("测试 dispatcher 应该能够绑定 IPv4 loopback");
    let address = transport.local_addr().unwrap();
    let now = Instant::now();
    let routing = routing.unwrap_or_else(|| RoutingTable::new(local_id, AddressFamily::Ipv4, now));
    let transactions = TransactionManager::new(timeout, max_pending);
    // 这些既有测试只验证查询分派；自动维护由 maintenance 专用测试单独覆盖。
    let config = DhtDispatcherConfig {
        maintenance: MaintenanceConfig {
            enabled: false,
            ..MaintenanceConfig::default()
        },
        ..DhtDispatcherConfig::default()
    };
    let (dispatcher, handle) =
        DhtDispatcher::with_config(transport, routing, transactions, config).unwrap();
    let task = tokio::spawn(dispatcher.run());
    (handle, address, task)
}

/// 创建启用 routing maintenance 的 dispatcher；测试可以缩短刷新周期。
async fn start_ipv4_maintenance_dispatcher(
    routing: RoutingTable,
    max_pending: usize,
    maintenance: MaintenanceConfig,
) -> (
    DhtHandle,
    SocketAddr,
    JoinHandle<Result<(), DispatcherError>>,
) {
    let transport = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let address = transport.local_addr().unwrap();
    let transactions = TransactionManager::new(Duration::from_secs(1), max_pending);
    let config = DhtDispatcherConfig {
        maintenance,
        // 维护测试明确使用本机单播，不依赖公网可达性。
        peer_store: crate::dht::peer_store::PeerStoreConfig {
            address_policy: crate::address::AddressPolicy::LocalUnicast,
            ..Default::default()
        },
        ..DhtDispatcherConfig::default()
    };
    let (dispatcher, handle) =
        DhtDispatcher::with_config(transport, routing, transactions, config).unwrap();
    let task = tokio::spawn(dispatcher.run());
    (handle, address, task)
}

fn query_message(
    transaction_id: &[u8],
    method: QueryMethod,
    sender_id: NodeId,
    target: Option<NodeId>,
    read_only: bool,
) -> KrpcMessage {
    KrpcMessage {
        t: ByteBuf::from(transaction_id.to_vec()),
        y: MessageType::Query,
        q: Some(method),
        a: Some(QueryArgs {
            id: sender_id,
            target,
            info_hash: None,
            port: None,
            token: None,
            implied_port: None,
            want: Vec::new(),
        }),
        r: None,
        e: None,
        ro: read_only.then_some(1),
        ip: None,
    }
}

fn response_message(transaction_id: ByteBuf, responder_id: NodeId) -> KrpcMessage {
    KrpcMessage {
        t: transaction_id,
        y: MessageType::Response,
        q: None,
        a: None,
        r: Some(empty_response(responder_id)),
        e: None,
        ip: None,
        ro: None,
    }
}

/// 构造带空 compact nodes 字段的合法 find_node 响应。
fn empty_find_node_response(transaction_id: ByteBuf, responder_id: NodeId) -> KrpcMessage {
    let mut message = response_message(transaction_id, responder_id);
    message.r.as_mut().unwrap().nodes = Some(CompactNodesV4(Vec::new()));
    message
}

fn empty_find_node_response_v6(transaction_id: ByteBuf, responder_id: NodeId) -> KrpcMessage {
    let mut message = response_message(transaction_id, responder_id);
    message.r.as_mut().unwrap().nodes6 = Some(CompactNodesV6(Vec::new()));
    message
}

async fn stop(handle: &DhtHandle, task: JoinHandle<Result<(), DispatcherError>>) {
    handle.shutdown().await.expect("dispatcher 应该正常关闭");
    task.await
        .expect("dispatcher task 不应该 panic")
        .expect("dispatcher 不应该因 transport 错误退出");
}

/// ping 响应必须返回本地 Node ID，同时原样回显对方自己选择的 transaction ID。
#[tokio::test]
async fn inbound_ping_echoes_transaction_id_and_local_node_id() {
    let local_id = NodeId([1; 20]);
    let (handle, address, task) =
        start_ipv4_dispatcher(local_id, Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let transaction_id = b"peer-chosen-id";
    let query = query_message(
        transaction_id,
        QueryMethod::Ping,
        NodeId([2; 20]),
        None,
        true,
    );

    peer.send_to(address, &query).await.unwrap();
    let received = peer.recv().await.unwrap().message;

    assert_eq!(received.y, MessageType::Response);
    assert_eq!(received.t.as_ref(), transaction_id);
    assert_eq!(received.r.expect("ping 应该包含 r 字典").id, local_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.recv())
            .await
            .is_err(),
        "ro=1 的发送者不应收到反向验证 ping"
    );
    stop(&handle, task).await;
}

/// find_node 只返回 good 节点，并按 XOR 距离从近到远排列，最多返回 K=8 条。
#[tokio::test]
async fn inbound_find_node_returns_closest_good_nodes() {
    let local_id = NodeId([0; 20]);
    let now = Instant::now();
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv4, now);
    for value in 1_u8..=8 {
        routing.observe_response(
            NodeId([value; 20]),
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 0, 2, value),
                6000 + value as u16,
            )),
            now,
        );
    }
    let expected: Vec<_> = routing
        .closest_good(&[0; 20], BUCKET_SIZE, now)
        .into_iter()
        .map(|contact| contact.id)
        .collect();
    let (handle, address, task) =
        start_ipv4_dispatcher(local_id, Duration::from_secs(1), 16, Some(routing)).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let query = query_message(
        b"fn",
        QueryMethod::FindNode,
        NodeId([99; 20]),
        Some(NodeId([0; 20])),
        true,
    );

    peer.send_to(address, &query).await.unwrap();
    let response = peer.recv().await.unwrap().message.r.unwrap();
    let nodes = response.nodes.expect("IPv4 响应应该包含 nodes").0;

    assert_eq!(nodes.len(), BUCKET_SIZE);
    assert_eq!(
        nodes.into_iter().map(|node| node.id).collect::<Vec<_>>(),
        expected
    );
    assert!(response.nodes6.is_none());
    stop(&handle, task).await;
}

/// routing table 为空时，find_node 仍应返回一个存在但内容为空的 compact nodes 字段。
#[tokio::test]
async fn empty_find_node_result_keeps_compact_field() {
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let query = query_message(
        b"empty",
        QueryMethod::FindNode,
        NodeId([2; 20]),
        Some(NodeId([3; 20])),
        true,
    );

    peer.send_to(address, &query).await.unwrap();
    let response = peer.recv().await.unwrap().message.r.unwrap();

    assert!(response.nodes.expect("nodes 字段不能省略").0.is_empty());
    stop(&handle, task).await;
}

/// IPv6 dispatcher 使用 nodes6，不会把 IPv6 联系人错误地写进 nodes。
#[tokio::test]
async fn ipv6_find_node_uses_nodes6() {
    let Ok(transport) = UdpTransport::bind("[::1]:0", Default::default()).await else {
        // 某些 CI 环境完全禁用了 IPv6；这种环境限制不应掩盖其他协议测试。
        return;
    };
    let address = transport.local_addr().unwrap();
    let routing = RoutingTable::new(NodeId([1; 20]), AddressFamily::Ipv6, Instant::now());
    let transactions = TransactionManager::new(Duration::from_secs(1), 8);
    let (dispatcher, handle) = DhtDispatcher::new(transport, routing, transactions).unwrap();
    let task = tokio::spawn(dispatcher.run());
    let peer = UdpTransport::bind("[::1]:0", Default::default())
        .await
        .unwrap();
    let query = query_message(
        b"v6",
        QueryMethod::FindNode,
        NodeId([2; 20]),
        Some(NodeId([3; 20])),
        true,
    );

    peer.send_to(address, &query).await.unwrap();
    let response = peer.recv().await.unwrap().message.r.unwrap();

    assert!(response.nodes.is_none());
    assert!(
        response
            .nodes6
            .expect("IPv6 响应应该包含 nodes6")
            .0
            .is_empty()
    );
    stop(&handle, task).await;
}

/// 缺少 find_node 必需的 target 时，应返回 203 Protocol Error。
#[tokio::test]
async fn missing_find_node_target_returns_protocol_error() {
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let query = query_message(b"bad", QueryMethod::FindNode, NodeId([2; 20]), None, true);

    peer.send_to(address, &query).await.unwrap();
    let error = peer.recv().await.unwrap().message;

    assert_eq!(error.y, MessageType::Error);
    assert_eq!(error.e.unwrap().0, KrpcErrorCode::Protocol.as_i64());
    stop(&handle, task).await;
}

/// 已经能解码出 transaction ID 时，缺少 q/a 或混入响应字段都应返回 203。
#[tokio::test]
async fn malformed_query_envelope_returns_protocol_error() {
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let malformed = [
        KrpcMessage {
            t: ByteBuf::from(b"no-q".to_vec()),
            y: MessageType::Query,
            q: None,
            a: Some(
                query_message(b"x", QueryMethod::Ping, NodeId([2; 20]), None, true)
                    .a
                    .unwrap(),
            ),
            r: None,
            e: None,
            ip: None,
            ro: Some(1),
        },
        KrpcMessage {
            t: ByteBuf::from(b"no-a".to_vec()),
            y: MessageType::Query,
            q: Some(QueryMethod::Ping),
            a: None,
            r: None,
            e: None,
            ip: None,
            ro: Some(1),
        },
        KrpcMessage {
            t: ByteBuf::from(b"conflict".to_vec()),
            y: MessageType::Query,
            q: Some(QueryMethod::Ping),
            a: query_message(b"x", QueryMethod::Ping, NodeId([2; 20]), None, true).a,
            r: Some(empty_response(NodeId([3; 20]))),
            e: None,
            ip: None,
            ro: Some(1),
        },
    ];

    for query in malformed {
        let expected_transaction = query.t.clone();
        peer.send_to(address, &query).await.unwrap();
        let response = peer.recv().await.unwrap().message;
        assert_eq!(response.t, expected_transaction);
        assert_eq!(response.e.unwrap().0, KrpcErrorCode::Protocol.as_i64());
    }
    stop(&handle, task).await;
}

/// 尚未实现的方法和私有扩展方法都应明确返回 204，而不是假装处理成功。
#[tokio::test]
async fn unsupported_methods_return_method_unknown() {
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let methods = [QueryMethod::Unknown("private_extension".to_owned())];

    for (index, method) in methods.into_iter().enumerate() {
        let query = query_message(&[index as u8], method, NodeId([2; 20]), None, true);
        peer.send_to(address, &query).await.unwrap();
        let error = peer.recv().await.unwrap().message.e.unwrap();
        assert_eq!(error.0, KrpcErrorCode::MethodUnknown.as_i64());
    }
    stop(&handle, task).await;
}

/// 无法解码和超大的数据报只丢弃当前消息，之后的正常查询仍应得到响应。
#[tokio::test]
async fn malformed_datagrams_do_not_stop_dispatcher() {
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    peer.send_to(b"not-bencode", address).await.unwrap();
    peer.send_to(&vec![0_u8; 4097], address).await.unwrap();
    let query = query_message(b"ok", QueryMethod::Ping, NodeId([2; 20]), None, true);
    peer.send_to(&to_bytes(&query).unwrap(), address)
        .await
        .unwrap();

    let mut buffer = vec![0_u8; 4096];
    let (size, _) = tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut buffer))
        .await
        .expect("正常 ping 不应因前面的垃圾数据报而超时")
        .unwrap();
    let response: KrpcMessage = Deserializer::from_bytes(&buffer[..size])
        .with_forbid_trailing_bytes(true)
        .deserialize()
        .unwrap();
    assert_eq!(response.y, MessageType::Response);
    stop(&handle, task).await;
}

/// 主动 ping 必须匹配 transaction、来源地址和预期 Node ID，成功后返回强类型结果。
#[tokio::test]
async fn outbound_ping_returns_typed_response() {
    let local_id = NodeId([1; 20]);
    let remote_id = NodeId([2; 20]);
    let (handle, _, task) = start_ipv4_dispatcher(local_id, Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: Some(remote_id),
    };
    let query_task = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });

    let query = peer.recv().await.unwrap();
    peer.send_to(query.source, &response_message(query.message.t, remote_id))
        .await
        .unwrap();

    assert_eq!(
        query_task.await.unwrap().unwrap(),
        PingResponse {
            responder_id: remote_id
        }
    );
    stop(&handle, task).await;
}

/// 主动 find_node 会把 compact nodes 转换成便于上层遍历的强类型节点列表。
#[tokio::test]
async fn outbound_find_node_decodes_discovered_nodes() {
    let remote_id = NodeId([2; 20]);
    let discovered_id = NodeId([3; 20]);
    let discovered_address = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 3), 6881);
    let (handle, _, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: Some(remote_id),
    };
    let query_task = tokio::spawn({
        let handle = handle.clone();
        async move { handle.find_node(remote, NodeId([9; 20])).await }
    });
    let query = peer.recv().await.unwrap();
    let mut response = empty_response(remote_id);
    response.nodes = Some(CompactNodesV4(vec![CompactNodeV4 {
        id: discovered_id,
        address: discovered_address,
    }]));
    peer.send_to(
        query.source,
        &KrpcMessage {
            t: query.message.t,
            y: MessageType::Response,
            q: None,
            a: None,
            r: Some(response),
            e: None,
            ip: None,
            ro: None,
        },
    )
    .await
    .unwrap();

    let result = query_task.await.unwrap().unwrap();
    assert_eq!(result.responder_id, remote_id);
    assert_eq!(
        result.nodes,
        vec![DiscoveredNode {
            id: discovered_id,
            address: SocketAddr::V4(discovered_address),
        }]
    );
    stop(&handle, task).await;
}

/// 来源和 transaction 都正确时，远端 KRPC 错误应原样交给查询调用方。
#[tokio::test]
async fn remote_error_is_returned_to_caller() {
    let (handle, _, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: None,
    };
    let query_task = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let query = peer.recv().await.unwrap();
    peer.send_to(
        query.source,
        &KrpcMessage {
            t: query.message.t,
            y: MessageType::Error,
            q: None,
            a: None,
            r: None,
            e: Some((202, ByteBuf::from(b"busy".to_vec()))),
            ip: None,
            ro: None,
        },
    )
    .await
    .unwrap();

    assert!(matches!(
        query_task.await.unwrap(),
        Err(QueryError::Remote { code: 202, message }) if message.as_ref() == b"busy"
    ));
    stop(&handle, task).await;
}

/// find_node 响应缺少当前地址族的 nodes 字段时，不能作为成功响应或可达性证明。
#[tokio::test]
async fn malformed_find_node_response_is_rejected() {
    let remote_id = NodeId([2; 20]);
    let (handle, _, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: Some(remote_id),
    };
    let query_task = tokio::spawn({
        let handle = handle.clone();
        async move { handle.find_node(remote, NodeId([9; 20])).await }
    });
    let query = peer.recv().await.unwrap();
    peer.send_to(query.source, &response_message(query.message.t, remote_id))
        .await
        .unwrap();

    assert!(matches!(
        query_task.await.unwrap(),
        Err(QueryError::InvalidResponse(_))
    ));
    stop(&handle, task).await;
}

/// 伪造者即使知道 transaction ID，只要来源地址不符也不能抢先完成查询。
#[tokio::test]
async fn response_source_mismatch_does_not_consume_transaction() {
    let remote_id = NodeId([2; 20]);
    let (handle, dispatcher_address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let attacker = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: Some(remote_id),
    };
    let query_task = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let query = peer.recv().await.unwrap().message;

    attacker
        .send_to(
            dispatcher_address,
            &response_message(query.t.clone(), remote_id),
        )
        .await
        .unwrap();
    peer.send_to(dispatcher_address, &response_message(query.t, remote_id))
        .await
        .unwrap();

    assert!(query_task.await.unwrap().is_ok());
    stop(&handle, task).await;
}

/// 地址正确但 Node ID 不符时，应结束查询并报告身份不匹配。
#[tokio::test]
async fn unexpected_node_id_is_rejected() {
    let expected = NodeId([2; 20]);
    let actual = NodeId([3; 20]);
    let (handle, _, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: Some(expected),
    };
    let query_task = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let query = peer.recv().await.unwrap();
    peer.send_to(query.source, &response_message(query.message.t, actual))
        .await
        .unwrap();

    assert!(matches!(
        query_task.await.unwrap(),
        Err(QueryError::UnexpectedNodeId {
            expected: found_expected,
            actual: found_actual,
        }) if found_expected == expected && found_actual == actual
    ));
    stop(&handle, task).await;
}

/// 陌生的非只读节点不会因一条入站查询直接入表，而会先收到反向 ping 验证。
#[tokio::test]
async fn unknown_query_sender_is_verified_before_being_returned() {
    let peer_id = NodeId([2; 20]);
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();

    let ping = query_message(b"in", QueryMethod::Ping, peer_id, None, false);
    peer.send_to(address, &ping).await.unwrap();
    assert_eq!(peer.recv().await.unwrap().message.y, MessageType::Response);

    // 第二条消息是 dispatcher 发出的验证 ping；只有正确响应后才允许入表。
    let verification = peer.recv().await.unwrap();
    assert_eq!(verification.message.q, Some(QueryMethod::Ping));
    peer.send_to(
        verification.source,
        &response_message(verification.message.t, peer_id),
    )
    .await
    .unwrap();

    // 同一 socket 随后发起只读 find_node，避免测试自身再制造验证请求。
    let find = query_message(b"find", QueryMethod::FindNode, peer_id, Some(peer_id), true);
    peer.send_to(address, &find).await.unwrap();
    let nodes = peer
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .nodes
        .unwrap()
        .0;
    assert!(nodes.iter().any(|node| node.id == peer_id));
    stop(&handle, task).await;
}

/// 查询超时应通知等待者；关闭时则使用独立错误取消仍在等待的查询。
#[tokio::test]
async fn timeout_and_shutdown_are_reported_separately() {
    let (handle, _, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_millis(20), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: None,
    };

    let timing_out = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    // 先读走第一次查询，确保后面收到的一定是 shutdown 场景中的第二条查询。
    let _ = peer.recv().await.unwrap();
    assert!(matches!(
        timing_out.await.unwrap(),
        Err(QueryError::Timeout)
    ));

    let pending = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    // 收到数据报说明第二条查询已经进入 transaction manager，此时再关闭。
    let _ = peer.recv().await.unwrap();
    handle.shutdown().await.unwrap();
    assert!(matches!(
        pending.await.unwrap(),
        Err(QueryError::ShuttingDown)
    ));
    task.await.unwrap().unwrap();
}

/// transaction 达到上限时要立即返回背压错误，不能悄悄丢弃新查询。
#[tokio::test]
async fn pending_limit_is_exposed_as_query_error() {
    let (handle, _, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 1, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: None,
    };
    let first = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let _ = peer.recv().await.unwrap();

    assert!(matches!(
        handle.ping(remote).await,
        Err(QueryError::AtCapacity { limit: 1 })
    ));
    handle.shutdown().await.unwrap();
    assert!(matches!(
        first.await.unwrap(),
        Err(QueryError::ShuttingDown)
    ));
    task.await.unwrap().unwrap();
}

/// 编码后的查询超过 transport 上限时，刚注册的 transaction 必须立即撤销。
#[tokio::test]
async fn send_failure_cancels_registered_transaction() {
    let transport = UdpTransport::bind(
        "127.0.0.1:0",
        crate::dht::udp::UdpTransportConfig {
            max_message_size: 1,
        },
    )
    .await
    .unwrap();
    let routing = RoutingTable::new(NodeId([1; 20]), AddressFamily::Ipv4, Instant::now());
    let transactions = TransactionManager::new(Duration::from_secs(1), 1);
    let (dispatcher, handle) = DhtDispatcher::new(transport, routing, transactions).unwrap();
    let task = tokio::spawn(dispatcher.run());
    let destination: SocketAddr = "127.0.0.1:6881".parse().unwrap();
    let remote = RemoteNode {
        address: destination,
        expected_id: None,
    };

    assert!(matches!(
        handle.ping(remote).await,
        Err(QueryError::Transport(
            UdpTransportError::MessageTooLarge { .. }
        ))
    ));
    // 第二次仍得到发送错误而不是 AtCapacity，证明第一次的 transaction 已撤销。
    assert!(matches!(
        handle.ping(remote).await,
        Err(QueryError::Transport(
            UdpTransportError::MessageTooLarge { .. }
        ))
    ));
    stop(&handle, task).await;
}

/// 同一陌生节点连续发来查询时，只能存在一条反向验证 ping，防止被放大利用。
#[tokio::test]
async fn verification_ping_is_deduplicated() {
    let peer_id = NodeId([2; 20]);
    let (handle, address, task) =
        start_ipv4_dispatcher(NodeId([1; 20]), Duration::from_secs(1), 8, None).await;
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();

    peer.send_to(
        address,
        &query_message(b"one", QueryMethod::Ping, peer_id, None, false),
    )
    .await
    .unwrap();
    peer.send_to(
        address,
        &query_message(b"two", QueryMethod::Ping, peer_id, None, false),
    )
    .await
    .unwrap();

    let mut responses = 0;
    let mut verification_queries = 0;
    for _ in 0..3 {
        match peer.recv().await.unwrap().message.y {
            MessageType::Response => responses += 1,
            MessageType::Query => verification_queries += 1,
            MessageType::Error => panic!("合法 ping 不应该收到错误"),
        }
    }
    assert_eq!(responses, 2);
    assert_eq!(verification_queries, 1);
    stop(&handle, task).await;
}

/// 同 IP 换 Node ID 和端口仍受接纳冷却约束，但合法查询必须得到回复。
#[tokio::test]
async fn verification_cooldown_does_not_suppress_replies_to_rotating_sources() {
    for (bind, family) in [
        ("127.0.0.1:0", AddressFamily::Ipv4),
        ("[::1]:0", AddressFamily::Ipv6),
    ] {
        let transport = UdpTransport::bind(bind, Default::default()).await.unwrap();
        let address = transport.local_addr().unwrap();
        let (dispatcher, handle) = DhtDispatcher::with_config(
            transport,
            RoutingTable::new(NodeId([1; 20]), family, Instant::now()),
            TransactionManager::new(Duration::from_secs(10), 32),
            DhtDispatcherConfig {
                maintenance: MaintenanceConfig {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        let budget = dispatcher.budget.clone();
        let task = tokio::spawn(dispatcher.run());
        for id in 2..=4 {
            let peer = UdpTransport::bind(bind, Default::default()).await.unwrap();
            peer.send_to(
                address,
                &query_message(&[id], QueryMethod::Ping, NodeId([id; 20]), None, false),
            )
            .await
            .unwrap();
            let received = tokio::time::timeout(Duration::from_secs(2), peer.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(received.message.y, MessageType::Response);
            assert_eq!(received.message.t.as_ref(), &[id]);
        }
        // stats 命令经过同一个事件循环，确保最后一次 observe_query 已执行。
        handle.status().await.unwrap();
        assert_eq!(budget.snapshot().verification.admitted, 1);
        assert_eq!(budget.snapshot().verification.cooldown, 2);
        assert_eq!(budget.snapshot().verification.failed, 0);
        stop(&handle, task).await;
    }
}

/// questionable incumbent 连续两次不响应后，应由已经验证过的候选节点替换。
#[tokio::test]
async fn bucket_probe_retries_then_replaces_bad_incumbent() {
    let incumbent_peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let candidate_peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([0; 20]);
    let incumbent_id = NodeId([0x80; 20]);
    let candidate_id = NodeId([0x88; 20]);
    let old = Instant::now() - GOOD_FOR - Duration::from_secs(1);
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv4, old);

    // 这些 ID 的最高不同位相同，会填满同一个 bucket。
    for offset in 0_u8..8 {
        let address = if offset == 0 {
            incumbent_peer.local_addr().unwrap()
        } else {
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 0, 2, offset),
                7000 + offset as u16,
            ))
        };
        routing.observe_response(NodeId([0x80 + offset; 20]), address, old);
    }
    let (handle, address, task) =
        start_ipv4_dispatcher(local_id, Duration::from_millis(20), 16, Some(routing)).await;
    let remote = RemoteNode {
        address: candidate_peer.local_addr().unwrap(),
        expected_id: Some(candidate_id),
    };
    let candidate_query = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let query = candidate_peer.recv().await.unwrap();
    candidate_peer
        .send_to(
            query.source,
            &response_message(query.message.t, candidate_id),
        )
        .await
        .unwrap();
    assert!(candidate_query.await.unwrap().is_ok());

    // incumbent 第一次超时后会再试一次；两次都不响应才允许替换。
    assert_eq!(
        incumbent_peer.recv().await.unwrap().message.q,
        Some(QueryMethod::Ping)
    );
    assert_eq!(
        incumbent_peer.recv().await.unwrap().message.q,
        Some(QueryMethod::Ping)
    );
    tokio::time::sleep(Duration::from_millis(30)).await;

    let lookup = query_message(
        b"lookup",
        QueryMethod::FindNode,
        candidate_id,
        Some(candidate_id),
        true,
    );
    candidate_peer.send_to(address, &lookup).await.unwrap();
    let nodes = candidate_peer
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .nodes
        .unwrap()
        .0;
    assert!(nodes.iter().any(|node| node.id == candidate_id));
    assert!(nodes.iter().all(|node| node.id != incumbent_id));
    stop(&handle, task).await;
}

/// questionable incumbent 如果正确响应第一次探测，就应继续保留并拒绝候选节点。
#[tokio::test]
async fn successful_bucket_probe_keeps_incumbent() {
    let incumbent_peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let candidate_peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([0; 20]);
    let incumbent_id = NodeId([0x80; 20]);
    let candidate_id = NodeId([0x88; 20]);
    let old = Instant::now() - GOOD_FOR - Duration::from_secs(1);
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv4, old);
    for offset in 0_u8..8 {
        let address = if offset == 0 {
            incumbent_peer.local_addr().unwrap()
        } else {
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(192, 0, 2, offset),
                7100 + offset as u16,
            ))
        };
        routing.observe_response(NodeId([0x80 + offset; 20]), address, old);
    }
    let (handle, address, task) =
        start_ipv4_dispatcher(local_id, Duration::from_secs(1), 16, Some(routing)).await;
    let remote = RemoteNode {
        address: candidate_peer.local_addr().unwrap(),
        expected_id: Some(candidate_id),
    };
    let candidate_query = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let query = candidate_peer.recv().await.unwrap();
    candidate_peer
        .send_to(
            query.source,
            &response_message(query.message.t, candidate_id),
        )
        .await
        .unwrap();
    assert!(candidate_query.await.unwrap().is_ok());

    let probe = incumbent_peer.recv().await.unwrap();
    incumbent_peer
        .send_to(
            probe.source,
            &response_message(probe.message.t, incumbent_id),
        )
        .await
        .unwrap();

    let lookup = query_message(
        b"lookup",
        QueryMethod::FindNode,
        candidate_id,
        Some(candidate_id),
        true,
    );
    candidate_peer.send_to(address, &lookup).await.unwrap();
    let nodes = candidate_peer
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .nodes
        .unwrap()
        .0;
    assert!(nodes.iter().any(|node| node.id == incumbent_id));
    assert!(nodes.iter().all(|node| node.id != candidate_id));
    stop(&handle, task).await;
}

/// dispatcher 构造时就拒绝 socket 与 routing table 地址族不一致的配置。
#[tokio::test]
async fn dispatcher_rejects_address_family_mismatch() {
    let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let transport = UdpTransport::from_socket(socket, Default::default()).unwrap();
    let routing = RoutingTable::new(NodeId([1; 20]), AddressFamily::Ipv6, Instant::now());
    let transactions = TransactionManager::new(Duration::from_secs(1), 8);

    assert!(matches!(
        DhtDispatcher::new(transport, routing, transactions),
        Err(DispatcherCreateError::AddressFamilyMismatch { .. })
    ));
}

// 保留一次显式构造，防止将来不小心移除 IPv6 compact 地址需要的标准库类型。
#[test]
fn ipv6_test_address_has_no_unrepresentable_metadata() {
    let address = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 0, 0);
    assert_eq!(address.flowinfo(), 0);
    assert_eq!(address.scope_id(), 0);
}

/// dispatcher 启动时如果已有节点，应立即用它查找离本地 Node ID 最近的节点。
#[tokio::test]
async fn maintenance_runs_startup_self_lookup() {
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([1; 20]);
    let peer_id = NodeId([2; 20]);
    let now = tokio::time::Instant::now().into_std();
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv4, now);
    routing.observe_response(peer_id, peer.local_addr().unwrap(), now);
    let (handle, _, task) =
        start_ipv4_maintenance_dispatcher(routing, 8, MaintenanceConfig::default()).await;

    let query = peer.recv().await.unwrap();
    assert_eq!(query.message.q, Some(QueryMethod::FindNode));
    assert_eq!(query.message.a.as_ref().unwrap().target, Some(local_id));
    peer.send_to(
        query.source,
        &empty_find_node_response(query.message.t, peer_id),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.recv())
            .await
            .is_err()
    );
    stop(&handle, task).await;
}

/// 空表不会主动发包；第一个节点通过响应验证后才会触发启动自查找。
#[tokio::test]
async fn first_verified_node_triggers_startup_lookup() {
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([1; 20]);
    let peer_id = NodeId([2; 20]);
    let now = tokio::time::Instant::now().into_std();
    let routing = RoutingTable::new(local_id, AddressFamily::Ipv4, now);
    let (handle, _, task) =
        start_ipv4_maintenance_dispatcher(routing, 8, MaintenanceConfig::default()).await;

    let remote = RemoteNode {
        address: peer.local_addr().unwrap(),
        expected_id: Some(peer_id),
    };
    let ping = tokio::spawn({
        let handle = handle.clone();
        async move { handle.ping(remote).await }
    });
    let query = peer.recv().await.unwrap();
    assert_eq!(query.message.q, Some(QueryMethod::Ping));
    peer.send_to(query.source, &response_message(query.message.t, peer_id))
        .await
        .unwrap();
    assert!(ping.await.unwrap().is_ok());

    let maintenance = peer.recv().await.unwrap();
    assert_eq!(maintenance.message.q, Some(QueryMethod::FindNode));
    assert_eq!(maintenance.message.a.unwrap().target, Some(local_id));
    stop(&handle, task).await;
}

/// 启动查找成功后，bucket 到期才会执行刷新，刷新完成后不会立刻重复。
#[tokio::test(start_paused = true)]
async fn stale_bucket_is_refreshed_once_after_deadline() {
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([1; 20]);
    let peer_id = NodeId([2; 20]);
    let now = tokio::time::Instant::now().into_std();
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv4, now);
    routing.observe_response(peer_id, peer.local_addr().unwrap(), now);
    let maintenance_config = MaintenanceConfig {
        refresh_after: Duration::from_secs(60),
        ..MaintenanceConfig::default()
    };
    let (handle, _, task) = start_ipv4_maintenance_dispatcher(routing, 8, maintenance_config).await;

    let startup = peer.recv().await.unwrap();
    peer.send_to(
        startup.source,
        &empty_find_node_response(startup.message.t, peer_id),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(60)).await;

    let refresh = peer.recv().await.unwrap();
    assert_eq!(refresh.message.q, Some(QueryMethod::FindNode));
    peer.send_to(
        refresh.source,
        &empty_find_node_response(refresh.message.t, peer_id),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), peer.recv())
            .await
            .is_err()
    );
    stop(&handle, task).await;
}

/// 只有一个 transaction 名额时，它应完整保留给显式用户查询。
#[tokio::test]
async fn maintenance_reserves_capacity_for_user_queries() {
    let peer = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([1; 20]);
    let peer_id = NodeId([2; 20]);
    let now = tokio::time::Instant::now().into_std();
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv4, now);
    routing.observe_response(peer_id, peer.local_addr().unwrap(), now);
    let (handle, _, task) =
        start_ipv4_maintenance_dispatcher(routing, 1, MaintenanceConfig::default()).await;

    let peer_address = peer.local_addr().unwrap();
    let ping = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .ping(RemoteNode {
                    address: peer_address,
                    expected_id: Some(peer_id),
                })
                .await
        }
    });
    let query = peer.recv().await.unwrap();
    assert_eq!(query.message.q, Some(QueryMethod::Ping));
    peer.send_to(query.source, &response_message(query.message.t, peer_id))
        .await
        .unwrap();
    assert!(ping.await.unwrap().is_ok());
    stop(&handle, task).await;
}

/// IPv6 dispatcher 的维护查询必须读取 nodes6；系统不支持 IPv6 时只跳过环境部分。
#[tokio::test]
async fn ipv6_maintenance_uses_nodes6() {
    let peer = match UdpTransport::bind("[::1]:0", Default::default()).await {
        Ok(peer) => peer,
        Err(crate::dht::udp::UdpTransportError::Io(error))
            if error.kind() == std::io::ErrorKind::AddrNotAvailable
                || matches!(error.raw_os_error(), Some(93 | 97)) =>
        {
            return;
        }
        Err(error) => panic!("IPv6 监听失败：{error}"),
    };
    let transport = UdpTransport::bind("[::1]:0", Default::default())
        .await
        .unwrap();
    let local_id = NodeId([1; 20]);
    let peer_id = NodeId([2; 20]);
    let now = tokio::time::Instant::now().into_std();
    let mut routing = RoutingTable::new(local_id, AddressFamily::Ipv6, now);
    routing.observe_response(peer_id, peer.local_addr().unwrap(), now);
    let transactions = TransactionManager::new(Duration::from_secs(1), 8);
    let mut config = DhtDispatcherConfig::default();
    config.peer_store.address_policy = crate::address::AddressPolicy::LocalUnicast;
    let (dispatcher, handle) =
        DhtDispatcher::with_config(transport, routing, transactions, config).unwrap();
    let task = tokio::spawn(dispatcher.run());

    let query = peer.recv().await.unwrap();
    assert_eq!(query.message.q, Some(QueryMethod::FindNode));
    peer.send_to(
        query.source,
        &empty_find_node_response_v6(query.message.t, peer_id),
    )
    .await
    .unwrap();
    tokio::task::yield_now().await;
    stop(&handle, task).await;
}

/// 重放抓包中的乱序响应布局：双栈均能完成引导，并清理在途 transaction。
#[tokio::test]
async fn unsorted_bootstrap_response_completes_on_both_families() {
    for (bind, family) in [
        ("127.0.0.1:0", AddressFamily::Ipv4),
        ("[::1]:0", AddressFamily::Ipv6),
    ] {
        let transport = UdpTransport::bind(bind, Default::default()).await.unwrap();
        let routing = RoutingTable::new(NodeId([1; 20]), family, Instant::now());
        let config = DhtDispatcherConfig {
            maintenance: MaintenanceConfig {
                enabled: false,
                ..Default::default()
            },
            peer_store: crate::dht::peer_store::PeerStoreConfig {
                address_policy: crate::address::AddressPolicy::LocalUnicast,
                ..Default::default()
            },
            ..Default::default()
        };
        let (dispatcher, handle) = DhtDispatcher::with_config(
            transport,
            routing,
            TransactionManager::new(Duration::from_secs(2), 8),
            config,
        )
        .unwrap();
        let task = tokio::spawn(dispatcher.run());
        let peer = UdpSocket::bind(bind).await.unwrap();
        let query_task = tokio::spawn({
            let handle = handle.clone();
            let address = peer.local_addr().unwrap();
            async move {
                handle
                    .bootstrap_ping(RemoteNode {
                        address,
                        expected_id: None,
                    })
                    .await
            }
        });
        let mut buffer = [0; 1024];
        let (size, source) =
            tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        let query: KrpcMessage = bendy::serde::from_bytes(&buffer[..size]).unwrap();
        let response = [
            b"d2:ip18:".as_slice(),
            &[0; 18],
            b"1:rd2:id20:",
            &[2; 20],
            b"e1:t4:",
            query.t.as_ref(),
            b"1:y1:r1:v4:LT\x01\x02e",
        ]
        .concat();
        peer.send_to(&response, source).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), query_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result.responder_id, NodeId([2; 20]));
        let status = handle.status().await.unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.good, 1);
        // 同一远端的乱序 ping 查询也必须得到回复；ro=1 避免测试触发反向验证。
        let inbound = b"d1:y1:q1:t4:echo1:q4:ping2:roi1e1:ad2:id20:abcdefghijklmnopqrstee";
        peer.send_to(inbound, source).await.unwrap();
        let (size, responder) =
            tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        let reply: KrpcMessage = bendy::serde::from_bytes(&buffer[..size]).unwrap();
        assert_eq!(responder, source);
        assert_eq!(reply.y, MessageType::Response);
        assert_eq!(reply.t, b"echo".as_slice());
        assert_eq!(reply.r.unwrap().id, NodeId([1; 20]));
        stop(&handle, task).await;
    }
}
