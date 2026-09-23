//! 临时目录隔离事务、预算、恢复和目录锁测试；进程崩溃场景使用专门子进程。
use super::*;

#[tokio::test]
async fn directory_lock_is_exclusive_and_released_before_shutdown_returns() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig::new(directory.path());
    let first = Storage::open(config.clone()).await.unwrap();
    assert!(matches!(
        Storage::open(config.clone()).await,
        Err(StorageError::Locked)
    ));
    first.shutdown().await.unwrap();
    // 不重试、不等待；关闭确认之后必须可以立即重新取得锁。
    Storage::open(config)
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[test]
fn non_contention_lock_errors_preserve_system_reason() {
    let error = std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "test permission denied",
    );
    let result = lock_error(std::fs::TryLockError::Error(error));
    assert!(
        matches!(result, StorageError::Io(message) if message.contains("test permission denied"))
    );
}

// 较新 schema 不允许旧程序修改；迁移中的冲突也必须回滚已创建的表。
#[tokio::test]
async fn schema_version_and_failed_migration_are_safe() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.sqlite3");
    let c = Connection::open(&db).unwrap();
    c.execute_batch("PRAGMA user_version=4;").unwrap();
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

#[test]
fn schema_v2_upgrades_without_losing_metadata_and_starts_incomplete_catalog() {
    let mut connection = Connection::open_in_memory().unwrap();
    schema::migrate(&mut connection).unwrap();
    connection
        .execute_batch(
            "INSERT INTO infohashes VALUES(zeroblob(20),1,1);
             INSERT INTO metadata VALUES(zeroblob(20),X'6465',2);
             DROP TRIGGER metadata_catalog_total_ai;
             DROP TRIGGER metadata_catalog_total_ad;
             DROP TABLE torrent_catalog_fts;
             DROP TABLE torrent_catalog;
             DROP TABLE torrent_catalog_state;
             PRAGMA user_version=2;",
        )
        .unwrap();

    schema::migrate(&mut connection).unwrap();

    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        3
    );
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM metadata", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT indexed,total FROM torrent_catalog_state WHERE singleton=1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
        (0, 1)
    );
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
                .call(|connection| test_insert_hash(connection, 8))
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
        store.handle.call(|_| Ok(())).await,
        Err(StorageError::Closed)
    );
    assert_eq!(store.shutdown().await, Err(StorageError::Closed));
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
                test_insert_hash(connection, 1).unwrap();
            })))
            .await
            .unwrap();
        ready.await.unwrap();
        handle
            .sender
            .send(StorageCommand::Execute(Box::new(|connection| {
                test_insert_hash(connection, 2).unwrap();
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
                test_insert_hash(connection, 3).unwrap();
            })))
            .await
            .unwrap();
        release.send(()).unwrap();
        shutdown.await.unwrap();
        assert_eq!(handle.call(|_| Ok(())).await, Err(StorageError::Closed));

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
                    test_insert_hash(connection, 4)
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

fn test_insert_hash(connection: &Connection, byte: u8) -> Result<(), StorageError> {
    connection.execute(
        "INSERT INTO infohashes(hash, first_seen, last_seen) VALUES (?1, 100, 100)",
        rusqlite::params![[byte; 20].as_slice()],
    )?;
    Ok(())
}
