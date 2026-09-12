//! 临时目录隔离事务、预算、恢复和目录锁测试；进程崩溃场景使用专门子进程。
use super::*;
use crate::{
    dht::routing::AddressFamily,
    identity::load_or_create,
    krpc::{InfoHashV1, NodeId},
};

// 较新 schema 不允许旧程序修改；迁移中的冲突也必须回滚已创建的表。
#[tokio::test]
async fn schema_version_and_failed_migration_are_safe() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.sqlite3");
    let c = Connection::open(&db).unwrap();
    c.execute_batch("PRAGMA user_version=3;").unwrap();
    drop(c);
    assert!(matches!(
        Storage::open(StorageConfig::new(dir.path())).await,
        Err(StorageError::Invalid(_))
    ));
    let c = Connection::open(&db).unwrap();
    assert_eq!(
        c.pragma_query_value(None, "journal_mode", |r| r.get::<_, String>(0))
            .unwrap(),
        "delete"
    );
    c.execute_batch("PRAGMA user_version=0; CREATE TABLE routing_contacts(sentinel TEXT);")
        .unwrap();
    drop(c);
    assert!(Storage::open(StorageConfig::new(dir.path())).await.is_err());
    let c = Connection::open(&db).unwrap();
    assert_eq!(
        c.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='node_identities'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        c.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

// 清楚地区分容量不足、句柄已关闭与线程退出，不留下永远等不到的调用。
#[tokio::test]
async fn bounded_budget_and_closed_worker_return_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.byte_capacity = 20;
    let store = Storage::open(config).await.unwrap();
    assert_eq!(
        store.handle.save_hashes(&[InfoHashV1([1; 20]); 2], 1).await,
        Err(StorageError::Capacity)
    );
    let held = store.handle.budget(20).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), store.handle.budget(1))
            .await
            .is_err()
    );
    drop(held);
    store
        .handle
        .save_hashes(&[InfoHashV1([1; 20])], 1)
        .await
        .unwrap();
    let handle = store.handle.clone();
    store.shutdown().await.unwrap();
    assert_eq!(handle.save_hashes(&[], 1).await, Err(StorageError::Closed));
}

// 模拟磁盘写失败：失败事务没有留下新 hash，解除故障后同一分段可以安全重试。
#[tokio::test]
async fn write_failure_rolls_back_and_retry_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    store
        .handle
        .call(|connection| {
            connection.execute_batch(
                "CREATE TRIGGER fail_insert
                 BEFORE INSERT ON infohashes
                 BEGIN
                     SELECT RAISE(ABORT,'simulated disk failure');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        store
            .handle
            .save_hashes(&[InfoHashV1([3; 20])], 100)
            .await
            .is_err()
    );
    store
        .handle
        .call(|connection| {
            connection.execute_batch("DROP TRIGGER fail_insert;")?;
            Ok(())
        })
        .await
        .unwrap();
    for _ in 0..2 {
        store
            .handle
            .save_hashes(&[InfoHashV1([3; 20])], 100)
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .handle
            .call(
                |c| Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| r
                    .get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
        1
    );
    store.shutdown().await.unwrap();
}

// 备份包含已经提交在 WAL 中的数据；不能覆盖已有备份。
#[tokio::test]
async fn online_backup_restores_committed_data() {
    let dir = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    store
        .handle
        .save_hashes(&[InfoHashV1([4; 20])], 100)
        .await
        .unwrap();
    let destination = backup.path().join("state.sqlite3");
    store.handle.backup(destination.clone()).await.unwrap();
    assert!(store.handle.backup(destination).await.is_err());
    store.shutdown().await.unwrap();
    let restored = Storage::open(StorageConfig::new(backup.path()))
        .await
        .unwrap();
    assert_eq!(
        restored
            .handle
            .call(
                |c| Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| r
                    .get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
        1
    );
    restored.shutdown().await.unwrap();
}

// 旧预约的迟到结算不能覆盖新预约；到期边界允许复用，未到期时不能驱逐。
#[tokio::test]
async fn cooldown_capacity_expiry_and_stale_completion() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let id = load_or_create(&store.handle, "test", AddressFamily::Ipv4, 0)
        .await
        .unwrap();
    let ip = "8.8.8.8".parse().unwrap();
    let old = store
        .handle
        .reserve_sampling(id, NodeId([1; 20]), ip, 100, 21_600_000, 1)
        .await
        .unwrap();
    store
        .handle
        .settle_sampling(old.clone(), 100, 1000, 2)
        .await
        .unwrap();
    assert!(matches!(
        store
            .handle
            .reserve_sampling(
                id,
                NodeId([2; 20]),
                "9.9.9.9".parse().unwrap(),
                101,
                21_600_000,
                1
            )
            .await,
        Err(StorageError::Capacity)
    ));
    let next = store
        .handle
        .reserve_sampling(id, NodeId([1; 20]), ip, 1100, 21_600_000, 1)
        .await
        .unwrap();
    store.handle.settle_sampling(old, 1100, 1, 0).await.unwrap();
    assert!(matches!(
        store
            .handle
            .reserve_sampling(id, NodeId([1; 20]), ip, 1102, 21_600_000, 1)
            .await,
        Err(StorageError::Cooldown)
    ));
    store
        .handle
        .settle_sampling(next, 1200, 1000, 3)
        .await
        .unwrap();
    let restored = store.handle.restore_cooldowns(id, 1000).await.unwrap();
    assert!(restored.iter().any(|value| matches!(
        value,
        RestoredCooldown::Id {
            remaining_ms: 1000,
            failures: 3,
            ..
        }
    )));
    assert!(
        store
            .handle
            .restore_cooldowns(id, 5000)
            .await
            .unwrap()
            .is_empty()
    );
    store.shutdown().await.unwrap();
}

// UTC 锚点跟随单调时间，暂停时间测试无需真的等待六小时。
#[tokio::test(start_paused = true)]
async fn clock_is_injectable_and_rejects_invalid_time() {
    let now = tokio::time::Instant::now().into_std();
    let clock = Clock::new(now, std::time::UNIX_EPOCH + Duration::from_secs(100));
    tokio::time::advance(Duration::from_secs(21600)).await;
    assert_eq!(
        clock
            .millis_at(tokio::time::Instant::now().into_std())
            .unwrap(),
        21_700_000
    );
    assert!(clock.millis_at(now - Duration::from_secs(1)).is_err());
    assert!(unix_millis(std::time::UNIX_EPOCH - Duration::from_secs(1)).is_err());
}

// 子进程直接退出，不执行析构，用于验证 WAL 真正的崩溃恢复而不是正常 close。
#[test]
fn crash_child() {
    let Ok(directory) = std::env::var("BT_SNIFFER_CRASH_TEST_DIRECTORY") else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let store = Storage::open(StorageConfig::new(directory)).await.unwrap();
            store
                .handle
                .save_hashes(&[InfoHashV1([8; 20])], 100)
                .await
                .unwrap();
            let _: Result<(), StorageError> = store
                .handle
                .call(|connection| {
                    connection.execute_batch(
                        "BEGIN IMMEDIATE; INSERT INTO infohashes VALUES(zeroblob(20),200,200);",
                    )?;
                    std::process::exit(86);
                })
                .await;
        });
    panic!("子进程必须在未提交事务内退出");
}

/// 子进程异常退出后保留已提交事务，未提交写入不应出现在恢复结果中。
#[tokio::test]
async fn abrupt_process_exit_keeps_commits_and_discards_partial_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "storage::tests::crash_child", "--nocapture"])
        .env("BT_SNIFFER_CRASH_TEST_DIRECTORY", dir.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let hashes = store
        .handle
        .call(|c| {
            Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| {
                r.get::<_, i64>(0)
            })?)
        })
        .await
        .unwrap();
    assert_eq!(hashes, 1);
    assert_eq!(
        store
            .handle
            .call(|c| Ok(c.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?))
            .await
            .unwrap(),
        "ok"
    );
    store.shutdown().await.unwrap();
}

// 重启加载同一身份；目录锁防止两个进程同时冒用同一个本地节点。
#[tokio::test]
async fn identity_is_stable_and_directory_is_locked() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::new(dir.path());
    let store = Storage::open(config.clone()).await.unwrap();
    let first = load_or_create(&store.handle, "test", AddressFamily::Ipv4, 100)
        .await
        .unwrap();
    assert!(matches!(
        Storage::open(config.clone()).await,
        Err(StorageError::Locked)
    ));
    let v6 = load_or_create(&store.handle, "test", AddressFamily::Ipv6, 100)
        .await
        .unwrap();
    assert_ne!(first.node_id, v6.node_id);
    store.shutdown().await.unwrap();
    let store = Storage::open(config).await.unwrap();
    let again = load_or_create(&store.handle, "test", AddressFamily::Ipv4, 200)
        .await
        .unwrap();
    assert_eq!(first.node_id, again.node_id);
    store.shutdown().await.unwrap();
}

// 重复或乱序的采样分段只更新时间范围，不产生多份 hash。
#[tokio::test]
async fn hashes_are_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    for at in [200, 100, 300, 200] {
        store
            .handle
            .save_hashes(&[InfoHashV1([1; 20]); 2], at)
            .await
            .unwrap();
    }
    let result = store
        .handle
        .call(|c| {
            Ok(c.query_row(
                "SELECT count(*),min(first_seen),max(last_seen) FROM infohashes",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap();
    assert_eq!(result, (1, 100, 300));
    store.shutdown().await.unwrap();
}

// 用 SQLite 的页数上限触发真正的 SQLITE_FULL，事务内已经插入的前半段也要回滚。
#[tokio::test]
async fn sqlite_full_rolls_back_the_entire_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    store
        .handle
        .call(|c| {
            let pages: i64 = c.pragma_query_value(None, "page_count", |r| r.get(0))?;
            c.pragma_update(None, "max_page_count", pages)?;
            Ok(())
        })
        .await
        .unwrap();
    let hashes: Vec<_> = (0u32..1024)
        .map(|n| {
            let mut bytes = [0; 20];
            bytes[..4].copy_from_slice(&n.to_be_bytes());
            InfoHashV1(bytes)
        })
        .collect();
    let error = store.handle.save_hashes(&hashes, 100).await.unwrap_err();
    assert!(error.to_string().contains("full"));
    assert_eq!(
        store
            .handle
            .call(
                |c| Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| r
                    .get::<_, i64>(0))?)
            )
            .await
            .unwrap(),
        0
    );
    store.shutdown().await.unwrap();
}

// 线程 panic 时，正在等待与后续提交的调用都返回 Closed，不会永久挂起。
#[tokio::test]
async fn worker_panic_closes_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let result: Result<(), StorageError> = store
        .handle
        .call(|_| panic!("测试数据库线程意外退出"))
        .await;
    assert_eq!(result, Err(StorageError::Closed));
    assert_eq!(
        store.handle.save_hashes(&[], 0).await,
        Err(StorageError::Closed)
    );
    assert_eq!(store.shutdown().await, Err(StorageError::Closed));
}

// 快照中有一条坏记录时，整个替换回滚，上一份有效快照仍在。
#[tokio::test]
async fn snapshot_replacement_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let identity = load_or_create(&store.handle, "test", AddressFamily::Ipv4, 100)
        .await
        .unwrap();
    let contact = SavedContact {
        id: NodeId([1; 20]),
        address: "127.0.0.1:1234".parse().unwrap(),
        responded_at: 100,
    };
    store
        .handle
        .save_contacts(identity, std::slice::from_ref(&contact))
        .await
        .unwrap();
    let mut bad = contact.clone();
    bad.address.set_port(0);
    assert!(store.handle.save_contacts(identity, &[bad]).await.is_err());
    assert_eq!(
        store.handle.load_contacts(identity).await.unwrap(),
        [contact]
    );
    store.shutdown().await.unwrap();
}

// 发出请求后未结算便重启，冷却重新保守等待六小时；端口不参与冷却键。
#[tokio::test]
async fn unfinished_reservation_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::new(dir.path());
    let store = Storage::open(config.clone()).await.unwrap();
    let identity = load_or_create(&store.handle, "test", AddressFamily::Ipv4, 100)
        .await
        .unwrap();
    store
        .handle
        .reserve_sampling(
            identity,
            NodeId([1; 20]),
            "8.8.8.8".parse().unwrap(),
            100,
            21_600_000,
            10,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .handle
            .reserve_sampling(
                identity,
                NodeId([2; 20]),
                "8.8.8.8".parse().unwrap(),
                101,
                21_600_000,
                10
            )
            .await,
        Err(StorageError::Cooldown)
    ));
    store.shutdown().await.unwrap();
    let store = Storage::open(config).await.unwrap();
    let restored = store
        .handle
        .restore_cooldowns(identity, 50_000_000)
        .await
        .unwrap();
    assert_eq!(restored.len(), 2);
    assert!(matches!(
        restored[0],
        RestoredCooldown::Id {
            remaining_ms: 21_600_000,
            ..
        } | RestoredCooldown::Ip {
            remaining_ms: 21_600_000,
            ..
        }
    ));
    store.shutdown().await.unwrap();
}

// 关闭屏障之前的命令必须落盘；之后的命令丢弃，确认完成后目录可重新打开。
#[tokio::test]
async fn shutdown_drains_accepted_operations_and_releases_directory() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
        let handle = storage.handle.clone();
        let (started, ready) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        handle
            .sender
            .send(StorageCommand::Execute(Box::new(move |connection| {
                started.send(()).unwrap();
                blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                records::upsert_hash(connection, InfoHashV1([1; 20]), 100).unwrap();
            })))
            .await
            .unwrap();
        ready.await.unwrap();
        handle
            .sender
            .send(StorageCommand::Execute(Box::new(|connection| {
                records::upsert_hash(connection, InfoHashV1([2; 20]), 100).unwrap();
            })))
            .await
            .unwrap();

        let shutdown = storage.shutdown();
        tokio::pin!(shutdown);
        // 队列尚有空位；本次 poll 接纳关闭命令，然后等待数据库完成。
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        handle
            .sender
            .send(StorageCommand::Execute(Box::new(|connection| {
                records::upsert_hash(connection, InfoHashV1([3; 20]), 100).unwrap();
            })))
            .await
            .unwrap();
        release.send(()).unwrap();
        shutdown.await.unwrap();
        assert_eq!(
            handle.save_hashes(&[], 100).await,
            Err(StorageError::Closed)
        );

        let reopened = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
        let count: i64 =
            reopened
                .handle
                .call(|connection| {
                    Ok(connection
                        .query_row("SELECT count(*) FROM infohashes", [], |row| row.get(0))?)
                })
                .await
                .unwrap();
        assert_eq!(count, 2);
        reopened.shutdown().await.unwrap();
    })
    .await
    .expect("关闭必须完成，不能遗留等待者或目录锁");
}

// 取消 oneshot 等待者不会撤销已入队的命令，预算必须留给仍在工作的数据库线程。
#[tokio::test]
async fn cancelled_waiter_keeps_operation_and_byte_budget_until_completion() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let dir = tempfile::tempdir().unwrap();
        let mut config = StorageConfig::new(dir.path());
        config.byte_capacity = 20;
        let storage = Storage::open(config).await.unwrap();
        let handle = storage.handle.clone();
        let permit = handle.budget(20).await.unwrap();
        let (started, ready) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let caller = tokio::spawn(async move {
            handle
                .submit(permit, move |connection| {
                    started.send(()).unwrap();
                    blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                    records::upsert_hash(connection, InfoHashV1([4; 20]), 100)
                })
                .await
        });
        ready.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(storage.handle.bytes.available_permits(), 0);

        release.send(()).unwrap();
        // 后续查询在原命令之后执行，响应确认同时证明原命令已完成。
        let count: i64 =
            storage
                .handle
                .call(|connection| {
                    Ok(connection
                        .query_row("SELECT count(*) FROM infohashes", [], |row| row.get(0))?)
                })
                .await
                .unwrap();
        assert_eq!(count, 1);
        assert_eq!(storage.handle.bytes.available_permits(), 20);
        storage.shutdown().await.unwrap();
    })
    .await
    .expect("取消后数据库操作必须能够收尾");
}

/// 已确认未发包时撤销租约，旧撤销不得删除新预约。
#[tokio::test]
async fn unsent_sampling_lease_revocation_is_generation_safe() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let id = load_or_create(&store.handle, "unsent", AddressFamily::Ipv4, 100)
        .await
        .unwrap();
    let ip = "8.8.8.8".parse().unwrap();
    let old = store
        .handle
        .reserve_sampling(id, NodeId([1; 20]), ip, 100, 21_600_000, 2)
        .await
        .unwrap();
    store.handle.abandon_sampling(old.clone()).await.unwrap();
    let current = store
        .handle
        .reserve_sampling(id, NodeId([1; 20]), ip, 101, 21_600_000, 2)
        .await
        .unwrap();
    store.handle.abandon_sampling(old).await.unwrap();
    assert!(
        store
            .handle
            .reserve_sampling(id, NodeId([1; 20]), ip, 102, 21_600_000, 2)
            .await
            .is_err()
    );
    store.handle.abandon_sampling(current).await.unwrap();
    assert!(
        store
            .handle
            .reserve_sampling(id, NodeId([1; 20]), ip, 103, 21_600_000, 2)
            .await
            .is_ok()
    );
    store.shutdown().await.unwrap();
}
