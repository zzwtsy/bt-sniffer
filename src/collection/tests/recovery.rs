//! 事务、故障监督、取消及退出恢复。
use super::*;

/// 完成事务失败不能只保存 metadata；旧 generation 的成功结果同样不能写入。
#[tokio::test]
async fn completion_rollback_and_stale_success_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let observer = crate::observation::Observer::new("transactions".into());
    storage.handle.observer = observer.clone();
    let store = &storage.handle;
    store.enable_fetch(2);
    store.save_hashes(&[hash()], 100).await.unwrap();
    let old = store.claim_job(100).await.unwrap().unwrap();
    store.recover_jobs(200).await.unwrap();
    let current = store.claim_job(200).await.unwrap().unwrap();
    assert_eq!(
        store
            .complete_job(old, verified().await, 201)
            .await
            .unwrap(),
        UpdateResult::Stale
    );
    assert!(store.metadata(hash()).await.unwrap().is_none());
    store
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_completion
                 BEFORE UPDATE OF state ON fetch_jobs
                 WHEN new.state='succeeded'
                 BEGIN
                     SELECT RAISE(ABORT,'injected commit failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .complete_job(current.clone(), verified().await, 202)
            .await,
        Err(StorageError::Database(_))
    ));
    assert!(store.metadata(hash()).await.unwrap().is_none());
    assert_eq!(store.fetch_stats().await.unwrap().running, 1);
    store
        .call(|connection| {
            connection.execute_batch("DROP TRIGGER fail_completion;")?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .complete_job(current, verified().await, 203)
            .await
            .unwrap(),
        UpdateResult::Applied
    );
    assert_eq!(store.metadata(hash()).await.unwrap().unwrap(), INFO);
    assert_eq!(store.fetch_stats().await.unwrap().succeeded, 1);
    let events = observer
        .page(
            0,
            100,
            &crate::observation::Filter {
                kind: Some(crate::observation::Kind::Commit),
                ..Default::default()
            },
        )
        .events;
    for outcome in ["stale", "failed", "applied"] {
        assert!(
            events.iter().any(|e| e["result"] == outcome),
            "缺少提交结果 {outcome}"
        );
    }
    assert_eq!(
        events
            .iter()
            .filter(|e| e["step"] == "metadata" && e["result"] == "applied")
            .count(),
        1
    );
    storage.shutdown().await.unwrap();
}

/// 关闭会话会取消正在进行的 TCP 下载，未完成任务在重新打开后仍可领取。
#[tokio::test]
async fn graceful_shutdown_cancels_tcp_and_keeps_task_recoverable() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle: _,
        address,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    session.start_fetch(config(dir.path())).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender = udp(AddressFamily::Ipv4).await;
    sender
        .send_to(address, &announce_query(None, 0))
        .await
        .unwrap();
    let token = sender
        .recv()
        .await
        .unwrap()
        .message
        .r
        .unwrap()
        .token
        .unwrap();
    sender
        .send_to(
            address,
            &announce_query(Some(token), listener.local_addr().unwrap().port()),
        )
        .await
        .unwrap();
    sender.recv().await.unwrap();
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut hello = [0; 68];
    socket.read_exact(&mut hello).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), session.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(socket.read(&mut hello).await.unwrap(), 0);
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    assert_eq!(storage.handle.fetch_stats().await.unwrap().running, 0);
    assert_eq!(storage.handle.active_jobs().await.unwrap(), 1);
    assert!(
        storage
            .handle
            .claim_job(now().unwrap() + 61_000)
            .await
            .unwrap()
            .is_some()
    );
    storage.shutdown().await.unwrap();
}

/// 采集写库故障可被上层观察，同时保留 DHT 网络服务。
#[tokio::test]
async fn fetch_storage_failure_is_reported_while_dht_remains_alive() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let store = session.test_store();
    store.save_hashes(&[hash()], now().unwrap()).await.unwrap();
    store
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_job
                 BEFORE INSERT ON fetch_jobs
                 BEGIN
                     SELECT RAISE(ABORT,'injected storage failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    session.start_fetch(config(dir.path())).await.unwrap();
    let fault = tokio::time::timeout(Duration::from_secs(3), session.next_fault())
        .await
        .unwrap();
    assert!(matches!(fault, SessionFault::StorageWrite(_)));
    assert!(handle.status().await.is_ok());
    assert!(session.shutdown().await.is_err());
}

// 协调器报告 worker 异常后仍等待其他结果，成功事务不能随 JoinSet 一同丢弃。
#[tokio::test]
async fn worker_failure_reports_claim_and_drains_success() {
    for abort in [false, true] {
        let metadata = verified().await;
        let dir = tempfile::tempdir().unwrap();
        let storage =
            crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
                .await
                .unwrap();
        let (report, mut faults) = FaultReporter::new();
        let collector = Collector::new(
            storage.handle.clone(),
            vec![],
            config(dir.path()),
            Clock::default(),
            CancellationToken::new(),
            report.pause_notifications(),
            report.collector_callback(FaultLog::default()),
        )
        .await
        .unwrap();
        let bad_hash = InfoHashV1([1; 20]);
        storage
            .handle
            .save_hashes(&[bad_hash, hash()], collector.now().unwrap())
            .await
            .unwrap();
        let bad = storage
            .handle
            .claim_job(collector.now().unwrap())
            .await
            .unwrap()
            .unwrap();
        let good = storage
            .handle
            .claim_job(collector.now().unwrap())
            .await
            .unwrap()
            .unwrap();
        let (bad, good) = if bad.hash == bad_hash {
            (bad, good)
        } else {
            (good, bad)
        };
        let generation = bad.generation;
        let network = super::tcp_limits::TcpLimits::default();
        let connection_permit = network.acquire_for_ip("127.0.0.1".parse().unwrap()).await;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut workers = Workers::default();
        let task = workers.spawn(bad.clone(), async move {
            let (_connection_permit, _permit) = (connection_permit, permit);
            if abort {
                std::future::pending::<()>().await;
            }
            panic!("注入 metadata worker 崩溃");
        });
        if abort {
            task.abort();
        }
        let (release, ready) = tokio::sync::oneshot::channel();
        workers.spawn(good, async move {
            ready.await.unwrap();
            Outcome::Success(metadata)
        });
        let metrics = collector.metrics.clone();
        let run = tokio::spawn(collector.supervise(workers));
        faults.changed().await.unwrap();
        assert!(faults.borrow().as_ref().unwrap().fatal());
        assert!(!run.is_finished(), "报告故障后仍须回收其余 worker");
        release.send(()).unwrap();
        let errors = run.await.unwrap().unwrap_err();
        assert!(errors.iter().any(|error| matches!(error,
            CollectorError::Worker { hash, generation: actual, source }
            if *hash == bad_hash && *actual == generation && source.is_cancelled() == abort)));
        assert_eq!(network.tracked_tcp_ips(), 0);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(
            storage.handle.metadata(hash()).await.unwrap().unwrap(),
            INFO
        );
        let (state, attempts): (String, u32) = storage
            .handle
            .call(move |c| {
                Ok(c.query_row(
                    "SELECT state,attempts FROM fetch_jobs WHERE hash=?1",
                    [bad_hash.0.as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!((state.as_str(), attempts), ("retry_wait", 0));
        assert_eq!(
            metrics.diagnostics.report()["attempt_commits"][0]["committed"],
            1
        );
        storage.shutdown().await.unwrap();
    }
}

/// 控制通道关闭属于致命错误，另一地址族的节点仍须进入清理流程。
#[tokio::test]
async fn control_failure_is_fatal_and_other_node_is_cleaned_up() {
    let dir = tempfile::tempdir().unwrap();
    let Fixture {
        mut session,
        handle: closed,
        address: _,
    } = fixture(dir.path(), AddressFamily::Ipv4).await;
    let live = session
        .add_node(
            "other",
            udp(AddressFamily::Ipv4).await,
            TransactionManager::new(Duration::from_secs(2), 8),
            DhtDispatcherConfig::default(),
            AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap();
    let (report, faults) = FaultReporter::new();
    let collector = Collector::new(
        session.test_store(),
        vec![closed.clone(), live.clone()],
        config(dir.path()),
        Clock::default(),
        CancellationToken::new(),
        report.pause_notifications(),
        report.collector_callback(FaultLog::default()),
    )
    .await
    .unwrap();
    let ingress = collector.ingress.clone();
    closed.shutdown().await.unwrap();
    let errors = collector.run().await.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, CollectorError::Control { .. }))
    );
    assert!(faults.borrow().as_ref().unwrap().fatal());
    assert!(live.status().await.is_ok());
    assert_eq!(ingress.sender.strong_count(), 1, "存活节点也必须撤销入口");
    // 关闭节点本身是显式请求，session 仍能完成幂等清理。
    session.shutdown().await.unwrap();
}

/// 注入时钟决定重试期限，关闭重开后沿用磁盘期限而不是重新开始退避。
#[tokio::test(start_paused = true)]
async fn collector_retry_uses_injected_clock_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let settings = StorageConfig::new(dir.path());
    for (anchor, initial) in [(100, true), (101, false)] {
        let storage = crate::collection::test_storage::TestStorage::open(settings.clone())
            .await
            .unwrap();
        let (report, _) = FaultReporter::new();
        let clock = Clock::new(
            tokio::time::Instant::now().into_std(),
            std::time::UNIX_EPOCH + Duration::from_secs(anchor),
        );
        let collector = Collector::new(
            storage.handle.clone(),
            vec![],
            config(dir.path()),
            clock,
            CancellationToken::new(),
            report.pause_notifications(),
            report.collector_callback(FaultLog::default()),
        )
        .await
        .unwrap();
        if initial {
            storage
                .handle
                .save_hashes(&[hash()], collector.now().unwrap())
                .await
                .unwrap();
            let job = storage
                .handle
                .claim_job(collector.now().unwrap())
                .await
                .unwrap()
                .unwrap();
            collector
                .save_outcome(job, Outcome::Retry(RetryReason::Deferred))
                .await
                .unwrap();
        } else {
            assert!(
                storage
                    .handle
                    .claim_job(collector.now().unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            tokio::time::advance(Duration::from_millis(58_999)).await;
            assert!(
                storage
                    .handle
                    .claim_job(collector.now().unwrap())
                    .await
                    .unwrap()
                    .is_none()
            );
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(
                storage
                    .handle
                    .claim_job(collector.now().unwrap())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        drop(collector);
        storage.shutdown().await.unwrap();
    }
}

/// 清理写入失败仍要回收其他 worker，留下的领取记录可在重启时恢复。
#[tokio::test]
async fn cleanup_write_failure_still_drains_workers_and_preserves_recovery() {
    let metadata = verified().await;
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let (report, faults) = FaultReporter::new();
    let mut collector = Collector::new(
        storage.handle.clone(),
        vec![],
        config(dir.path()),
        Clock::default(),
        CancellationToken::new(),
        report.pause_notifications(),
        report.collector_callback(FaultLog::default()),
    )
    .await
    .unwrap();
    storage
        .handle
        .save_hashes(&[hash(), InfoHashV1([1; 20])], collector.now().unwrap())
        .await
        .unwrap();
    let first = storage
        .handle
        .claim_job(collector.now().unwrap())
        .await
        .unwrap()
        .unwrap();
    let second = storage
        .handle
        .claim_job(collector.now().unwrap())
        .await
        .unwrap()
        .unwrap();
    let (success, cancelled) = if first.hash == hash() {
        (first, second)
    } else {
        (second, first)
    };
    let network = super::tcp_limits::TcpLimits::default();
    let connection_permit = network.acquire_for_ip("127.0.0.1".parse().unwrap()).await;
    let mut workers = Workers::default();
    workers.spawn(success, async move { Outcome::Success(metadata) });
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    workers.spawn(cancelled, async move {
        let _connection_permit = connection_permit;
        token.cancelled().await;
        Outcome::Retry(RetryReason::Deferred)
    });
    // 所有状态更新失败，验证即使第一个结果无法保存，第二个任务仍被 join。
    storage
        .handle
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_cleanup
                 BEFORE UPDATE ON fetch_jobs
                 BEGIN
                     SELECT RAISE(ABORT,'injected cleanup failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let mut errors = Vec::new();
    collector.finish(&mut workers, &cancel, &mut errors).await;
    assert!(errors.iter().any(CollectorError::is_storage));
    assert!(matches!(
        *faults.borrow(),
        Some(SessionFault::StorageWrite(_))
    ));
    assert!(workers.is_empty());
    assert_eq!(network.tracked_tcp_ips(), 0);
    assert!(storage.handle.metadata(hash()).await.unwrap().is_none());
    assert_eq!(storage.handle.fetch_stats().await.unwrap().running, 2);
    storage
        .handle
        .call(|connection| {
            connection.execute_batch("DROP TRIGGER fail_cleanup;")?;
            Ok(())
        })
        .await
        .unwrap();
    storage
        .handle
        .recover_jobs(collector.now().unwrap())
        .await
        .unwrap();
    assert_eq!(storage.handle.fetch_stats().await.unwrap().pending, 2);
    drop(collector);
    storage.shutdown().await.unwrap();
}

/// 构造末尾订阅不补发旧暂停通知；唯一报告端退出仍按监督通道关闭报告错误。
#[tokio::test]
async fn late_fault_subscription_and_closed_notifications_preserve_contract() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let (pause, _) = watch::channel(());
    pause.send_replace(());
    let collector = Collector::new(
        storage.handle.clone(),
        vec![],
        config(dir.path()),
        Clock::default(),
        CancellationToken::new(),
        pause.clone(),
        Box::new(|_| {}),
    )
    .await
    .unwrap();
    assert!(!collector.faults.has_changed().unwrap());
    drop(pause);
    let errors = collector.run().await.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, CollectorError::SupervisorClosed))
    );
    storage.shutdown().await.unwrap();
}

/// 正常保存和退出保存均只统计 Applied；故障回滚、旧结果和重复完成不能增加提交数。
#[tokio::test]
async fn classified_commits_require_applied_in_running_and_cleanup_paths() {
    async fn save(collector: &Collector, job: Job, cleanup: bool) -> Result<(), CollectorError> {
        let (peer, task) = tcp_info_with_order(AddressFamily::Ipv4, INFO.to_vec(), true).await;
        let metadata = PeerClient::new(MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap()
        .with_metrics(collector.metrics.clone())
        .fetch_one(
            hash(),
            peer,
            &CancellationToken::new(),
            super::peer::PeerContext::default(),
        )
        .await
        .unwrap();
        task.await.unwrap();
        assert!(metadata.used_extension_compatibility());
        let outcome = Outcome::Success(metadata);
        if cleanup {
            collector.save_outcome(job, outcome).await
        } else {
            collector
                .apply_running_outcome(job, outcome, &mut CompletionTotals::default())
                .await
        }
    }
    for cleanup in [false, true] {
        for recover in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let storage =
                crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
                    .await
                    .unwrap();
            let (report, _) = FaultReporter::new();
            let collector = Collector::new(
                storage.handle.clone(),
                vec![],
                config(dir.path()),
                Clock::default(),
                CancellationToken::new(),
                report.pause_notifications(),
                report.collector_callback(FaultLog::default()),
            )
            .await
            .unwrap();
            let store = &storage.handle;
            store.save_hashes(&[hash()], 100).await.unwrap();
            let old = store.claim_job(100).await.unwrap().unwrap();
            let job = if recover {
                store.recover_jobs(200).await.unwrap();
                let current = store.claim_job(200).await.unwrap().unwrap();
                save(&collector, old, cleanup).await.unwrap();
                current
            } else {
                let mut stale = old.clone();
                stale.generation += 100;
                save(&collector, stale, cleanup).await.unwrap();
                old
            };
            assert!(
                collector.metrics.diagnostics.report()["attempt_commits"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                collector.metrics.diagnostics.report()["extension_compatibility"]["committed"],
                0
            );
            store
                .call(|connection| {
                    connection.execute_batch(
                        "CREATE TRIGGER fail_classified_commit
                     BEFORE UPDATE OF state ON fetch_jobs
                     WHEN new.state='succeeded'
                     BEGIN
                         SELECT RAISE(ABORT, 'injected write failure');
                     END;",
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            assert!(save(&collector, job.clone(), cleanup).await.is_err());
            assert!(store.metadata(hash()).await.unwrap().is_none());
            assert!(
                collector.metrics.diagnostics.report()["attempt_commits"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                collector.metrics.diagnostics.report()["extension_compatibility"]["committed"],
                0
            );
            store
                .call(|connection| {
                    connection.execute_batch("DROP TRIGGER fail_classified_commit")?;
                    Ok(())
                })
                .await
                .unwrap();
            save(&collector, job.clone(), cleanup).await.unwrap();
            save(&collector, job, cleanup).await.unwrap();
            let report = collector.metrics.diagnostics.report();
            let commits = report["attempt_commits"].as_array().unwrap();
            assert_eq!(commits.len(), 1);
            assert_eq!(
                commits[0]["attempt_kind"],
                if recover { "Repeat" } else { "First" }
            );
            assert_eq!(commits[0]["failed_attempts_before"], 0);
            assert_eq!(commits[0]["committed"], 1);
            assert_eq!(report["extension_compatibility"]["committed"], 1);
            assert_eq!(report["attempt_hints"][0]["committed"], 1);
            let class_commits = report["attempt_class_commits"].as_array().unwrap();
            assert_eq!(class_commits.len(), 1);
            assert_eq!(class_commits[0]["committed"], 1);
            assert_eq!(
                class_commits[0]["claim_class"],
                if recover { "Retry" } else { "Recent" }
            );
            assert_eq!(store.fetch_stats().await.unwrap().succeeded, 1);
            storage.shutdown().await.unwrap();
        }
    }
}

/// 实际协调器在完成或诊断 SQL 上阻塞时关闭；队列满也不能绕过共同期限。
#[tokio::test]
async fn slow_storage_shutdown_preserves_accepted_commands_and_completion_boundary() {
    use crate::collection::test_storage::{BlockedOperation, CommandBarrier};
    #[derive(Clone, Copy, Debug)]
    enum Waiting {
        Queue,
        Completion,
        Snapshot,
    }
    for waiting in [Waiting::Queue, Waiting::Completion, Waiting::Snapshot] {
        for expires in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut settings = StorageConfig::new(dir.path());
            settings.command_capacity = 1;
            let mut session = Session::open_observed(
                settings.clone(),
                Arc::default(),
                crate::observation::Observer::new("slow-shutdown".into()),
            )
            .await
            .unwrap();
            let closed = session.test_close_observer();
            let mut node_config = DhtDispatcherConfig::default();
            node_config.maintenance.enabled = false;
            node_config.peer_store.address_policy = AddressPolicy::LocalUnicast;
            let handle = session
                .add_node(
                    "test",
                    udp(AddressFamily::Ipv4).await,
                    TransactionManager::new(Duration::from_secs(1), 32),
                    node_config,
                    AddressPolicy::LocalUnicast,
                )
                .await
                .unwrap();
            let mut monitor_client = None;
            if matches!(waiting, Waiting::Completion) {
                let listener = crate::monitor::bind("127.0.0.1:0".parse().unwrap())
                    .await
                    .unwrap();
                let address = listener.local_addr().unwrap();
                session.start_monitor(listener);
                let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
                client
                    .write_all(b"GET /api/v1/stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .await
                    .unwrap();
                let mut bytes = [0; 8192];
                let count = tokio::time::timeout(Duration::from_secs(2), client.read(&mut bytes))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(String::from_utf8_lossy(&bytes[..count]).contains("200 OK"));
                monitor_client = Some(client);
            }
            let store = session.test_store();
            store.enable_fetch(16);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (peer, completed_peer) = if matches!(waiting, Waiting::Completion) {
                let (peer, task) = tcp(AddressFamily::Ipv4).await;
                (peer, Some(task))
            } else {
                (listener.local_addr().unwrap(), None)
            };
            let observed = now().unwrap();
            store.save_hashes(&[hash()], observed).await.unwrap();
            store.discover_peer(hash(), peer, observed).await.unwrap();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            *store.test_barrier.lock().unwrap() = Some((
                if matches!(waiting, Waiting::Completion) {
                    BlockedOperation::Completion
                } else {
                    BlockedOperation::Status
                },
                CommandBarrier {
                    entered,
                    release: blocked,
                },
            ));
            session.start_fetch(config(dir.path())).await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), ready)
                .await
                .unwrap()
                .unwrap();
            // 到达 SQL 屏障时，完成场景已通过真实 peer 校验；其他场景保留尚未完成的 worker。
            let mut socket = if let Some(task) = completed_peer {
                task.await.unwrap();
                None
            } else {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut hello = [0; 68];
                socket.read_exact(&mut hello).await.unwrap();
                Some(socket)
            };
            // 只有 Queue 场景填满命令槽；另外两项单独覆盖协调器等待结果。
            let mut queued = Box::pin(store.call(|_| Ok(())));
            if matches!(waiting, Waiting::Queue) {
                assert!(futures_util::poll!(&mut queued).is_pending());
            }
            tokio::time::pause();
            let mut shutdown = Box::pin(session.shutdown());
            assert!(futures_util::poll!(&mut shutdown).is_pending());
            if expires {
                tokio::time::advance(Duration::from_secs(31)).await;
                let errors = shutdown.await.unwrap_err();
                assert!(
                    errors
                        .iter()
                        .any(|error| error.contains("30 秒") && error.contains("回收采集协调器")),
                    "{errors:?}"
                );
                tokio::time::resume();
                assert!(matches!(
                    crate::storage::Storage::open(settings.clone()).await,
                    Err(StorageError::Locked)
                ));
                release.send(()).unwrap();
            } else {
                tokio::time::resume();
                release.send(()).unwrap();
                shutdown.await.unwrap();
            }
            if let Some(mut client) = monitor_client {
                let mut tail = Vec::new();
                let result =
                    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut tail))
                        .await
                        .unwrap();
                assert!(
                    result.is_ok()
                        || result.is_err_and(|e| e.kind() == std::io::ErrorKind::ConnectionReset)
                );
            }
            if matches!(waiting, Waiting::Queue) {
                queued.await.unwrap();
            } else {
                drop(queued);
            }
            if let Some(socket) = &mut socket {
                let mut byte = [0];
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(3), socket.read(&mut byte))
                        .await
                        .unwrap()
                        .unwrap(),
                    0
                );
            }
            drop(handle);
            drop(store);
            tokio::time::timeout(Duration::from_secs(5), closed)
                .await
                .unwrap()
                .unwrap();
            let reopened = crate::collection::test_storage::TestStorage::open(settings)
                .await
                .unwrap();
            let expected = if matches!(waiting, Waiting::Completion) {
                "succeeded"
            } else if expires {
                "running"
            } else {
                "retry_wait"
            };
            let state = reopened
                .handle
                .call(|c| {
                    Ok(c.query_row(
                        "SELECT state,generation,attempts FROM fetch_jobs WHERE hash=?1",
                        [hash().0.as_slice()],
                        |r| {
                            Ok((
                                r.get::<_, String>(0)?,
                                r.get::<_, i64>(1)?,
                                r.get::<_, i64>(2)?,
                            ))
                        },
                    )?)
                })
                .await
                .unwrap();
            assert_eq!(
                state,
                (expected.into(), 1, 0),
                "{waiting:?}, expires={expires}"
            );
            assert_eq!(
                reopened.handle.metadata(hash()).await.unwrap().is_some(),
                matches!(waiting, Waiting::Completion)
            );
            if matches!(waiting, Waiting::Completion) {
                let hints = reopened
                    .handle
                    .call(|c| {
                        Ok(c.query_row("SELECT count(*) FROM peer_hints", [], |r| {
                            r.get::<_, i64>(0)
                        })?)
                    })
                    .await
                    .unwrap();
                assert_eq!(hints, 0);
            }
            reopened.shutdown().await.unwrap();
        }
    }
}

/// 调用者已取消等待时，数据库事实和过程仍在真实提交后同时可见。
#[tokio::test]
async fn cancelled_completion_waiter_still_observes_committed_transaction() {
    use crate::collection::test_storage::{BlockedOperation, CommandBarrier};
    let dir = tempfile::tempdir().unwrap();
    let mut storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let observer = crate::observation::Observer::new("cancelled-commit".into());
    storage.handle.observer = observer.clone();
    let store = storage.handle.clone();
    store.enable_fetch(2);
    store.save_hashes(&[hash()], 100).await.unwrap();
    let job = store.claim_job(100).await.unwrap().unwrap();
    let metadata = verified().await;
    let (entered, arrival) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    *store.test_barrier.lock().unwrap() = Some((
        BlockedOperation::Completion,
        CommandBarrier {
            entered,
            release: blocked,
        },
    ));
    let writer = store.clone();
    let task = tokio::spawn(async move { writer.complete_job(job, metadata, 101).await });
    arrival.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        !observer
            .page(0, 100, &Default::default())
            .events
            .iter()
            .any(|e| e["step"] == "metadata" && e["result"] == "applied")
    );
    release.send(()).unwrap();
    assert_eq!(store.metadata(hash()).await.unwrap().unwrap(), INFO);
    let events = observer.page(0, 100, &Default::default()).events;
    assert_eq!(
        events
            .iter()
            .filter(|e| e["step"] == "metadata" && e["result"] == "applied")
            .count(),
        1
    );
    storage.shutdown().await.unwrap();
}
