//! 功能存储测试使用临时数据库；事务与恢复观察沿用真实生产入口。
use super::store::CollectionStore;
use crate::info_hash::SwarmKey;
use crate::storage::{Storage, StorageConfig, StorageError};
use std::time::Duration;
// 清楚地区分容量不足、句柄已关闭与线程退出，不留下永远等不到的调用。
#[tokio::test]
async fn bounded_budget_and_closed_worker_return_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = StorageConfig::new(dir.path());
    config.byte_capacity = 20;
    let store = Storage::open(config).await.unwrap();
    assert_eq!(
        CollectionStore::new(store.handle.clone())
            .save_hashes(&[SwarmKey([1; 20]); 2], 1)
            .await,
        Err(StorageError::Capacity)
    );
    let held = store.handle.budget(20).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), store.handle.budget(1))
            .await
            .is_err()
    );
    drop(held);
    CollectionStore::new(store.handle.clone())
        .save_hashes(&[SwarmKey([1; 20])], 1)
        .await
        .unwrap();
    let handle = store.handle.clone();
    store.shutdown().await.unwrap();
    assert_eq!(
        CollectionStore::new(handle.clone())
            .save_hashes(&[], 1)
            .await,
        Err(StorageError::Closed)
    );
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
        CollectionStore::new(store.handle.clone())
            .save_hashes(&[SwarmKey([3; 20])], 100)
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
        CollectionStore::new(store.handle.clone())
            .save_hashes(&[SwarmKey([3; 20])], 100)
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

// 重复或乱序的采样分段只更新时间范围，不产生多份 hash。
#[tokio::test]
async fn hashes_are_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    for at in [200, 100, 300, 200] {
        CollectionStore::new(store.handle.clone())
            .save_hashes(&[SwarmKey([1; 20]); 2], at)
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
            SwarmKey(bytes)
        })
        .collect();
    let error = CollectionStore::new(store.handle.clone())
        .save_hashes(&hashes, 100)
        .await
        .unwrap_err();
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
