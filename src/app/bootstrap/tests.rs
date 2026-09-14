//! 注入 DNS 结果并使用 loopback 验证引导；真实发包与受控时钟共同检查退避和容量。
use super::*;
use crate::dht::dispatcher::DhtDispatcher;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::krpc::KrpcMessage;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::RoutingTable;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;

async fn node(
    capacity: usize,
) -> (
    DhtHandle,
    tokio::task::JoinHandle<Result<(), crate::dht::dispatcher::DispatcherError>>,
) {
    let transport = udp().await;
    let mut config = DhtDispatcherConfig::default();
    config.maintenance.enabled = false;
    let (dispatcher, handle) = DhtDispatcher::with_config(
        transport,
        RoutingTable::new(
            NodeId([1; 20]),
            AddressFamily::Ipv4,
            Instant::now().into_std(),
        ),
        TransactionManager::new(Duration::from_secs(5), capacity),
        config,
    )
    .unwrap();
    (handle, tokio::spawn(dispatcher.run()))
}
async fn udp() -> UdpTransport {
    UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap()
}
async fn answer(peer: &UdpTransport) {
    let request = timeout(Duration::from_secs(3), peer.recv())
        .await
        .unwrap()
        .unwrap();
    let args = bendy::serde::from_bytes(b"d2:id20:22222222222222222222e").unwrap();
    peer.send_to(
        request.source,
        &KrpcMessage {
            t: request.message.t,
            y: MessageType::Response,
            q: None,
            a: None,
            r: Some(args),
            e: None,
            ro: None,
        },
    )
    .await
    .unwrap();
}

/// 引导抖动不能缩短基础退避，也不能超过最大等待期限。
#[test]
fn jitter_has_floor_and_ceiling() {
    // 退避不会提前于基础期限，也不会超过十五分钟。
    assert_eq!(
        retry_delay(Duration::from_secs(60), 0),
        Duration::from_secs(60)
    );
    assert_eq!(
        retry_delay(Duration::from_secs(60), 200),
        Duration::from_secs(72)
    );
    assert_eq!(
        retry_delay(Duration::from_secs(900), 200),
        Duration::from_secs(900)
    );
}

/// 合法引导响应使邻居可用；本轮不再向未查询地址发送新请求。
#[tokio::test]
async fn valid_bootstrap_response_updates_status_and_stops_new_queries() {
    // 第一个节点响应后，不再访问余下地址；状态只统计本次已验证的邻居。
    let (handle, task) = node(8).await;
    let first = udp().await;
    let second = udp().await;
    let (result, ()) = tokio::join!(
        round(
            &handle,
            vec![first.local_addr().unwrap(), second.local_addr().unwrap()]
        ),
        answer(&first)
    );
    assert_eq!(result.unwrap(), Round::Connected);
    assert_eq!(handle.status().await.unwrap().good, 1);
    assert!(
        timeout(Duration::from_millis(50), second.recv())
            .await
            .is_err()
    );
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 引导不能占满 transaction，必须保留用户查询名额。
#[tokio::test]
async fn bootstrap_preserves_last_user_transaction() {
    // 总容量只有一条时，引导必须让步，而调用者显式 ping 仍能成功。
    let (handle, task) = node(1).await;
    let peer = udp().await;
    assert_eq!(
        round(&handle, vec![peer.local_addr().unwrap()])
            .await
            .unwrap(),
        Round::Busy
    );
    assert_eq!(handle.status().await.unwrap().pending, 0);
    let (result, ()) = tokio::join!(
        handle.ping(RemoteNode {
            address: peer.local_addr().unwrap(),
            expected_id: None
        }),
        answer(&peer)
    );
    result.unwrap();
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 已有已验证邻居时不再解析引导 DNS，避免无意义的重复引导。
#[tokio::test]
async fn verified_neighbor_skips_dns() {
    // 已经收到邻居的合法响应时，不再访问公共引导 DNS。
    let (handle, task) = node(8).await;
    let peer = udp().await;
    let (result, ()) = tokio::join!(
        handle.bootstrap_ping(RemoteNode {
            address: peer.local_addr().unwrap(),
            expected_id: None
        }),
        answer(&peer)
    );
    result.unwrap();
    let resolver = |_: String| -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        panic!("已有 good 邻居，不应进行 DNS 查询")
    };
    assert!(
        timeout(
            Duration::from_millis(30),
            run_with_resolver(
                handle.clone(),
                vec!["fake:1".into()],
                AddressPolicy::LocalUnicast,
                resolver
            )
        )
        .await
        .is_err()
    );
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 通过实际收包验证发送间隔和在途上限，而不是只检查配置数值。
#[tokio::test]
async fn bootstrap_spaces_sends_and_limits_concurrency() {
    // 前两条都不回答，第三条不能挤进来；前两次发送至少相隔一秒。
    let (handle, task) = node(8).await;
    let first = udp().await;
    let second = udp().await;
    let third = udp().await;
    let addresses = vec![
        first.local_addr().unwrap(),
        second.local_addr().unwrap(),
        third.local_addr().unwrap(),
    ];
    let round_handle = handle.clone();
    let round_task = tokio::spawn(async move { round(&round_handle, addresses).await });
    timeout(Duration::from_secs(2), first.recv())
        .await
        .unwrap()
        .unwrap();
    let first_received = Instant::now();
    timeout(Duration::from_secs(2), second.recv())
        .await
        .unwrap()
        .unwrap();
    // 留出本机调度误差；真正的发送调度使用严格的一秒期限。
    assert!(first_received.elapsed() >= Duration::from_millis(900));
    assert_eq!(handle.status().await.unwrap().pending, 2);
    assert!(
        timeout(Duration::from_millis(100), third.recv())
            .await
            .is_err()
    );
    round_task.abort();
    let _ = round_task.await;
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

/// 注入 DNS 结果后验证地址族、访问策略、去重和地址数量上限。
#[tokio::test(start_paused = true)]
async fn resolver_filters_deduplicates_and_limits_addresses() {
    // 注入 DNS 结果，不使用公共 DNS；同族公网地址最多取八个。
    let resolver = |_: String| -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        Box::pin(async {
            let mut addresses = vec![
                "127.0.0.1:1".parse().unwrap(),
                "[2606:4700::1111]:1".parse().unwrap(),
                "8.8.8.8:0".parse().unwrap(),
            ];
            for port in 1..=10 {
                addresses.push(SocketAddr::from(([8, 8, 8, 8], port)));
                addresses.push(SocketAddr::from(([8, 8, 8, 8], port)));
            }
            Ok(addresses)
        })
    };
    let addresses = resolve_addresses(
        &["fake:1".into()],
        AddressFamily::Ipv4,
        AddressPolicy::PublicOnly,
        &resolver,
    )
    .await;
    assert_eq!(addresses.len(), 8);
    assert_eq!(addresses.iter().copied().collect::<HashSet<_>>().len(), 8);
    let addresses = resolve_addresses(
        &["fake:1".into()],
        AddressFamily::Ipv6,
        AddressPolicy::PublicOnly,
        &resolver,
    )
    .await;
    assert_eq!(
        addresses,
        vec!["[2606:4700::1111]:1".parse::<SocketAddr>().unwrap()]
    );
}

/// DNS 无响应不能无限阻塞；没有可用邻居时按既定期限退避。
#[tokio::test(start_paused = true)]
async fn dns_timeout_is_bounded_and_empty_network_backs_off() {
    // DNS 永不返回也只能占用五秒；无人响应时不会忙循环解析域名。
    let resolver = |_: String| -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        Box::pin(std::future::pending())
    };
    let start = Instant::now();
    assert!(
        resolve_addresses(
            &["fake:1".into()],
            AddressFamily::Ipv4,
            AddressPolicy::PublicOnly,
            &resolver
        )
        .await
        .is_empty()
    );
    assert_eq!(Instant::now() - start, Duration::from_secs(5));
    let (handle, task) = node(8).await;
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls = count.clone();
    let resolver = move |_: String| -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async { Ok(Vec::new()) })
    };
    let bootstrap = tokio::spawn(run_with_resolver(
        handle.clone(),
        vec!["fake:1".into()],
        AddressPolicy::PublicOnly,
        resolver,
    ));
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
    tokio::time::advance(Duration::from_secs(59)).await;
    assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
    bootstrap.abort();
    let _ = bootstrap.await;
    handle.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}
