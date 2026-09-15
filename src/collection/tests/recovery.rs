//! 事务、故障监督、取消及退出恢复。
use super::*;

/// 完成事务失败不能只保存 metadata；旧 generation 的成功结果同样不能写入。
#[tokio::test]
async fn completion_rollback_and_stale_success_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
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
