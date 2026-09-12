//! 暂停时间后逐轮驱动真实事件循环，直接检查物理清理而不只是查询过滤。
use super::*;
use crate::dht::dispatcher::MaintenanceConfig;
use crate::dht::peer_store::{PeerAddressPolicy, PeerStoreConfig};
use crate::krpc::InfoHashV1;
use std::time::Duration;

/// 维护关闭时仍会分批清理超过 256 条的到期记录；清空后不再产生过期唤醒。
#[tokio::test(start_paused = true)]
async fn peer_cleanup_is_independent_of_routing_maintenance() {
    let now = current_time();
    let transport = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let config = DhtDispatcherConfig {
        maintenance: MaintenanceConfig {
            enabled: false,
            ..Default::default()
        },
        peer_store: PeerStoreConfig {
            address_policy: PeerAddressPolicy::LocalUnicast,
            ..Default::default()
        },
        ..Default::default()
    };
    let (mut dispatcher, handle) = DhtDispatcher::with_config(
        transport,
        RoutingTable::new(NodeId([1; 20]), AddressFamily::Ipv4, now),
        TransactionManager::new(Duration::from_secs(1), 8),
        config,
    )
    .unwrap();
    assert!(dispatcher.peers.next_deadline().is_none());
    for hash in 0..3 {
        for port in 1..=100 {
            dispatcher
                .peers
                .announce(
                    InfoHashV1([hash; 20]),
                    SocketAddr::from(([127, 0, 0, 1], port)),
                    now,
                )
                .unwrap();
        }
    }
    tokio::time::advance(config.peer_store.ttl).await;
    // yield 分支使本测试始终可运行，避免暂停时钟自动跳到无关的未来期限。
    let mut running = Box::pin(dispatcher.run_loop());
    for _ in 0..8 {
        tokio::select! {
            biased;
            result = &mut running => panic!("事件循环意外退出：{result:?}"),
            _ = tokio::task::yield_now() => {}
        }
    }
    drop(running);
    assert!(
        dispatcher.peers.next_deadline().is_none(),
        "不能只过滤过期 peer 而不释放内存"
    );
    assert!(dispatcher.pending.is_empty());
    let (stopped, result) = tokio::join!(handle.shutdown(), dispatcher.run_loop());
    stopped.unwrap();
    result.unwrap();
}

async fn queued_fixture() -> (DhtDispatcher, DhtHandle) {
    let transport = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let mut config = DhtDispatcherConfig::default();
    config.maintenance.enabled = false;
    config.peer_store.address_policy = PeerAddressPolicy::LocalUnicast;
    DhtDispatcher::with_config(
        transport,
        RoutingTable::new(NodeId([1; 20]), AddressFamily::Ipv4, current_time()),
        TransactionManager::new(Duration::from_secs(1), 8),
        config,
    )
    .unwrap()
}
/// 未发包的排队超时只返回本地等待，取消与关闭同时回收待发意图。
#[tokio::test(start_paused = true)]
async fn queued_rpc_timeout_cancel_shutdown_and_head_of_line_isolation() {
    let (mut dispatcher, _handle) = queued_fixture().await;
    let budget = dispatcher.budget.clone();
    let blocked: SocketAddr = "127.0.0.1:6881".parse().unwrap();
    let other: SocketAddr = "127.0.0.2:6881".parse().unwrap();
    // 耗尽一个目的 IP，另一个目的地址仍能发出。
    for _ in 0..2 {
        assert!(
            budget
                .query(crate::dht::traffic::Class::Control, blocked.ip(), 100)
                .is_zero()
        );
    }
    let (reply, mut result) = oneshot::channel();
    dispatcher
        .start_query(
            RemoteNode {
                address: blocked,
                expected_id: None,
            },
            QueryMethod::Ping,
            None,
            PendingPurpose::UserPing {
                reply,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
            current_time(),
        )
        .await;
    assert_eq!(dispatcher.queued.len(), 1);
    assert_eq!(dispatcher.transactions.len(), 0);
    let (reply, other_result) = oneshot::channel();
    dispatcher
        .start_query(
            RemoteNode {
                address: other,
                expected_id: None,
            },
            QueryMethod::Ping,
            None,
            PendingPurpose::UserPing {
                reply,
                cancel: tokio_util::sync::CancellationToken::new(),
            },
            current_time(),
        )
        .await;
    assert_eq!(dispatcher.transactions.len(), 1);
    assert_eq!(dispatcher.queued.len(), 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    dispatcher.advance_outbound(current_time()).await;
    assert!(matches!(result.try_recv(), Ok(Err(QueryError::LocalWait))));
    dispatcher.close_pending();
    assert!(matches!(
        other_result.await.unwrap(),
        Err(QueryError::ShuttingDown)
    ));
    assert_eq!(dispatcher.occupied(), 0);
    // 取消尚未发出的采集查询，不等到 5 秒期限。
    for _ in 0..2 {
        assert!(
            budget
                .query(crate::dht::traffic::Class::Collector, blocked.ip(), 100)
                .is_zero()
        );
    }
    let cancel = tokio_util::sync::CancellationToken::new();
    let progress = std::sync::Arc::new(super::super::api::RpcProgress::default());
    let (reply, _result) = oneshot::channel();
    dispatcher
        .handle_command(
            Command::GetPeers {
                remote: RemoteNode {
                    address: blocked,
                    expected_id: None,
                },
                hash: InfoHashV1([0; 20]),
                progress: progress.clone(),
                cancel: cancel.clone(),
                reply,
            },
            current_time(),
        )
        .await;
    assert_eq!(dispatcher.queued.len(), 1);
    cancel.cancel();
    dispatcher.cancel_fetch_queries();
    assert_eq!(dispatcher.occupied(), 0);
    assert_eq!(progress.sent.load(std::sync::atomic::Ordering::Relaxed), 0);
}

/// 显式控制查询取消也唤醒事件循环，不等待远端超时。
#[tokio::test]
async fn explicit_rpc_cancellation_releases_in_flight_transaction() {
    let (dispatcher, handle) = queued_fixture().await;
    let server = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let address = server.local_addr().unwrap();
    let running = tokio::spawn(dispatcher.run());
    let h = handle.clone();
    let query = tokio::spawn(async move {
        h.ping(RemoteNode {
            address,
            expected_id: None,
        })
        .await
    });
    server.recv().await.unwrap();
    query.abort();
    let _ = query.await;
    tokio::time::timeout(Duration::from_millis(200), async {
        while handle.status().await.unwrap().pending != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    handle.shutdown().await.unwrap();
    running.await.unwrap().unwrap();
}
