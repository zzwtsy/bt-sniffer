//! 容量、查询节奏、连接许可与采样背压。
use super::*;

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
    let hashes: Vec<_> = (0..66).map(|n| SwarmKey([n; 20])).collect();
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
