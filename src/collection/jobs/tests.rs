//! 通过真实 SQLite 观察任务状态转换；整数时间均为 UTC 毫秒，不等待真实退避时间。
use super::*;
use crate::collection::test_storage::TestStorage as Storage;
use crate::storage::StorageConfig;

/// 重复 hash 去重且受容量限制；恢复生成新领取，旧结果不能覆盖当前任务。
#[tokio::test]
async fn dedup_capacity_recovery_and_stale_generation() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let store = &storage.handle;
    // 只允许一个活跃任务；重复发现同一 hash 不会额外占用容量。
    store.enable_fetch(1);
    let first_hash = InfoHashV1([1; 20]);
    let second_hash = InfoHashV1([2; 20]);
    store
        .save_hashes(&[first_hash, first_hash, second_hash], 100)
        .await
        .unwrap();
    assert_eq!(store.active_jobs().await.unwrap(), 1);
    let first = store.claim_job(100).await.unwrap().unwrap();
    assert_eq!(first.attempt_kind(), AttemptKind::First);
    assert_eq!(first.failed_attempts_before, 0);
    assert!(store.claim_job(100).await.unwrap().is_none());
    // 模拟重启恢复：新领取的版本更高，旧领取的迟到结果必须被忽略。
    store.recover_jobs(200).await.unwrap();
    let second = store.claim_job(200).await.unwrap().unwrap();
    assert!(second.generation > first.generation);
    assert_eq!(second.attempt_kind(), AttemptKind::Repeat);
    assert_eq!(second.failed_attempts_before, 0);
    assert_eq!(
        store
            .retry_job(
                first,
                201,
                RetryReason::Failed(crate::collection::failure::AttemptFailure::Other)
            )
            .await
            .unwrap(),
        UpdateResult::Stale
    );
    assert_eq!(store.fetch_stats().await.unwrap().running, 1);
    assert_eq!(
        store
            .retry_job(
                second,
                201,
                RetryReason::Failed(crate::collection::failure::AttemptFailure::Other)
            )
            .await
            .unwrap(),
        UpdateResult::Applied
    );
    store.save_hashes(&[first_hash], 202).await.unwrap();
    assert!(store.claim_job(202).await.unwrap().is_none());
    // 连续失败最终进入休眠，释放活跃任务容量给另一个 hash。
    for n in 1..6 {
        let now_ms = 2_000_000 * n;
        let job = store.claim_job(now_ms).await.unwrap().unwrap();
        assert_eq!(job.attempt_kind(), AttemptKind::Repeat);
        assert_eq!(job.failed_attempts_before, n as u32);
        assert_eq!(
            store
                .retry_job(
                    job,
                    now_ms,
                    RetryReason::Failed(crate::collection::failure::AttemptFailure::Other)
                )
                .await
                .unwrap(),
            UpdateResult::Applied
        );
    }
    assert_eq!(store.fetch_stats().await.unwrap().dormant, 1);
    store.backfill_jobs(12_000_000).await.unwrap();
    assert_eq!(
        store.claim_job(12_000_000).await.unwrap().unwrap().hash,
        second_hash
    );
    storage.shutdown().await.unwrap();
}

/// peer 提示有容量和期限；休眠任务需要新观察才重新进入调度。
#[tokio::test]
async fn hints_are_bounded_expire_and_dormancy_needs_new_observation() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(2);
    let hash = InfoHashV1([3; 20]);
    for port in 1..=12 {
        s.discover_peer(hash, format!("127.0.0.1:{port}").parse().unwrap(), 100)
            .await
            .unwrap();
    }
    let job = s.claim_job(100).await.unwrap().unwrap();
    assert_eq!(job.peers.len(), 8);
    assert_eq!(job.attempt_kind(), AttemptKind::First);
    assert_eq!(job.failed_attempts_before, 0);
    s.retry_job(job, 100, RetryReason::Deferred).await.unwrap();
    let hinted = s
        .claim_class(
            60_100,
            Some(ClaimClass::Hint),
            crate::address::AddressPolicy::LocalUnicast,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hinted.job.class, ClaimClass::Retry);
    assert!(hinted.job.had_valid_hint);
    assert_eq!(hinted.job.attempt_kind(), AttemptKind::Repeat);
    assert_eq!(hinted.job.failed_attempts_before, 0);
    s.retry_job(
        hinted.job,
        60_100,
        RetryReason::Local(LocalReason::ResourceWait),
    )
    .await
    .unwrap();
    let job = s.claim_job(2_000_000).await.unwrap().unwrap();
    assert_eq!(job.failed_attempts_before, 0);
    assert!(job.peers.is_empty());
    s.call(move |connection| {
        connection.execute(
            "UPDATE fetch_jobs SET state='dormant',attempts=6,updated_at=100",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    s.backfill_jobs(90_000_000).await.unwrap();
    assert!(s.claim_job(90_000_000).await.unwrap().is_none());
    s.save_hashes(&[hash], 200).await.unwrap();
    assert!(s.claim_job(90_000_000).await.unwrap().is_none());
    s.save_hashes(&[hash], 90_000_000).await.unwrap();
    let reactivated = s.claim_job(90_000_000).await.unwrap().unwrap();
    assert_eq!(reactivated.attempt_kind(), AttemptKind::Repeat);
    assert_eq!(reactivated.failed_attempts_before, 0);
    storage.shutdown().await.unwrap();
}

/// 旧库升级保留历史 hash，只有启用采集时才回填下载任务。
#[tokio::test]
async fn v1_upgrade_preserves_hashes_and_backfills_only_on_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let hash = InfoHashV1([4; 20]);
    storage.handle.save_hashes(&[hash], 1).await.unwrap();
    storage.shutdown().await.unwrap();
    let c = Connection::open(dir.path().join("state.sqlite3")).unwrap();
    c.execute_batch(
        "DROP TRIGGER metadata_catalog_total_ai;
         DROP TRIGGER metadata_catalog_total_ad;
         DROP TABLE torrent_catalog_fts;
         DROP TABLE torrent_catalog;
         DROP TABLE torrent_catalog_state;
         DROP TABLE peer_hints;
         DROP TABLE fetch_jobs;
         PRAGMA user_version=1;",
    )
    .unwrap();
    drop(c);
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    assert_eq!(storage.handle.active_jobs().await.unwrap(), 0);
    storage.handle.enable_fetch(10);
    storage.handle.backfill_jobs(2).await.unwrap();
    assert_eq!(
        storage.handle.claim_job(2).await.unwrap().unwrap().hash,
        hash
    );
    storage.shutdown().await.unwrap();
}

/// 历史回填按页限制扫描，容量耗尽时保留游标，恢复后不遗漏待处理 hash。
#[tokio::test]
async fn historical_backfill_bounds_scans_and_preserves_cursor_at_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    let hashes: Vec<_> = (0u16..600)
        .map(|i| {
            let mut bytes = [0; 20];
            bytes[..2].copy_from_slice(&i.to_be_bytes());
            InfoHashV1(bytes)
        })
        .collect();
    s.save_hashes(&hashes, 1).await.unwrap();
    let last = hashes[599];
    s.call(move |connection| {
        connection.execute(
            "INSERT INTO fetch_jobs(hash,state,due_at,updated_at)
             SELECT hash,'succeeded',1,1
             FROM infohashes
             WHERE hash<?1",
            [last.0.as_slice()],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    s.enable_fetch(1);
    let first = s.backfill_page(2, None).await.unwrap();
    assert_eq!(first, Some(hashes[255]));
    assert_eq!(s.active_jobs().await.unwrap(), 0);
    let second = s.backfill_page(2, first).await.unwrap();
    assert_eq!(second, Some(hashes[511]));
    assert_eq!(s.active_jobs().await.unwrap(), 0);
    let third = s.backfill_page(2, second).await.unwrap();
    assert_eq!(third, Some(last));
    assert_eq!(s.active_jobs().await.unwrap(), 1);
    assert_eq!(s.backfill_page(2, third).await.unwrap(), third);
    assert_eq!(s.claim_job(2).await.unwrap().unwrap().hash, last);
    storage.shutdown().await.unwrap();
}

/// 3:1 轮转只领取到期任务，空类借用；失效或非法 hint 不能取得优先级。
#[tokio::test]
async fn preferred_claims_rotate_borrow_and_respect_hint_policy_and_due_time() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let store = &storage.handle;
    store.enable_fetch(16);
    let policy = crate::address::AddressPolicy::PublicOnly;
    for id in 1..=12 {
        let hash = InfoHashV1([id; 20]);
        store.save_hashes(&[hash], 100).await.unwrap();
        if id >= 4 {
            store
                .discover_peer(hash, "8.8.8.8:6881".parse().unwrap(), 100)
                .await
                .unwrap();
        }
    }
    let mut order = Vec::new();
    for turn in 0..12 {
        let Claim { job, .. } = store
            .claim_class(
                100,
                Some(if turn % 4 < 3 {
                    ClaimClass::Hint
                } else {
                    ClaimClass::Recent
                }),
                policy,
            )
            .await
            .unwrap()
            .unwrap();
        order.push(job.hash.0[0]);
        assert_eq!(job.had_valid_hint, turn % 4 < 3);
        store
            .retry_job(job, 100, RetryReason::Local(LocalReason::ResourceWait))
            .await
            .unwrap();
    }
    assert_eq!(order, vec![4, 5, 6, 1, 7, 8, 9, 2, 10, 11, 12, 3]);
    assert!(
        store
            .claim_class(101, Some(ClaimClass::Hint), policy)
            .await
            .unwrap()
            .is_none()
    );
    let Claim { job, .. } = store
        .claim_class(2_000_000, Some(ClaimClass::Hint), policy)
        .await
        .unwrap()
        .unwrap();
    assert!(!job.had_valid_hint, "全部提示过期后借用普通类");
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn capacity_still_refreshes_accepted_hints_and_local_deferral_keeps_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(1);
    let hash = InfoHashV1([1; 20]);
    s.save_hashes(&[hash], 100).await.unwrap();
    assert!(
        s.discover_peer(hash, "127.0.0.1:6881".parse().unwrap(), 200)
            .await
            .unwrap()
    );
    assert!(
        !s.discover_peer(InfoHashV1([2; 20]), "127.0.0.1:6881".parse().unwrap(), 200)
            .await
            .unwrap()
    );
    let Claim { job, .. } = s
        .claim_class(
            200,
            Some(ClaimClass::Hint),
            crate::address::AddressPolicy::PublicOnly,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(!job.had_valid_hint);
    assert!(job.peers.is_empty());
    s.retry_job(job, 200, RetryReason::Local(LocalReason::ResourceWait))
        .await
        .unwrap();
    s.call(|c| {
        let actual: (i64, i64, String) =
            c.query_row("SELECT attempts,due_at,error FROM fetch_jobs", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
        assert_eq!(actual, (0, 60200, "local_wait".into()));
        Ok(())
    })
    .await
    .unwrap();
    storage.shutdown().await.unwrap();
}
