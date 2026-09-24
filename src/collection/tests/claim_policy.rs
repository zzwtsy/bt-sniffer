//! 通过真实补位入口核对策略状态；空节点列表与空地址族避免网络流量。
use super::*;
use jobs::{ClaimClass, ClaimPolicy};

#[tokio::test]
async fn refills_share_cursor_and_only_successful_claims_advance() {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        crate::collection::test_storage::TestStorage::open(StorageConfig::new(dir.path()))
            .await
            .unwrap();
    let mut cfg = config(dir.path());
    cfg.concurrency = 1;
    let (notifications, _) = watch::channel(());
    let collector = Collector::new(
        storage.handle.clone(),
        vec![],
        cfg,
        Clock::default(),
        CancellationToken::new(),
        notifications,
        Box::new(|_| {}),
    )
    .await
    .unwrap();
    let resources = Arc::new(WorkerResources::new(
        collector.peer.clone(),
        collector.metrics.clone(),
    ));
    let cancel = CancellationToken::new();
    let mut workers = Workers::default();
    let mut policy = ClaimPolicy::new();
    let mut totals = CompletionTotals::default();
    use ClaimClass::{Hint, History, Recent, Retry};
    for (n, expected) in [Hint, Recent, Retry, Hint, History, Recent, Retry, Hint]
        .repeat(2)
        .into_iter()
        .enumerate()
    {
        assert_eq!(policy.order()[0], expected);
        // 多次空补位不能消耗预留机会。
        for _ in 0..2 {
            collector
                .claim_workers(&mut workers, &cancel, &resources, &[], &mut policy)
                .await
                .unwrap();
            assert!(workers.is_empty());
            assert_eq!(policy.order()[0], expected);
        }
        let h = SwarmKey([n as u8; 20]);
        collector
            .store
            .save_hashes(&[h], collector.now().unwrap())
            .await
            .unwrap();
        collector
            .store
            .call(|c| {
                c.execute_batch(
                    "CREATE TRIGGER fail_refill AFTER UPDATE ON fetch_jobs
                WHEN NEW.state='running' BEGIN SELECT RAISE(FAIL, 'refill fixture'); END;",
                )?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(
            collector
                .claim_workers(&mut workers, &cancel, &resources, &[], &mut policy)
                .await
                .is_err()
        );
        assert!(workers.is_empty());
        assert_eq!(policy.order()[0], expected);
        collector
            .store
            .call(|c| {
                c.execute_batch("DROP TRIGGER fail_refill")?;
                Ok(())
            })
            .await
            .unwrap();
        collector
            .claim_workers(&mut workers, &cancel, &resources, &[], &mut policy)
            .await
            .unwrap();
        assert_eq!(workers.len(), 1);
        let order_after_claim = policy.order();
        // 满并发补位不会额外领取或推进；与完成后补位共用同一个实例。
        collector
            .claim_workers(&mut workers, &cancel, &resources, &[], &mut policy)
            .await
            .unwrap();
        assert_eq!(policy.order(), order_after_claim);
        let (job, outcome) = workers.next().await.unwrap();
        assert_eq!(job.hash, h);
        assert_eq!(job.class, Recent, "其他首选通过借用命中 Recent");
        assert_eq!(job.generation, 1, "失败领取没有留下版本变更");
        collector
            .apply_running_outcome(job, outcome.unwrap(), &mut totals)
            .await
            .unwrap();
    }
    assert_eq!(policy.order()[0], Hint);
    drop(collector);
    storage.shutdown().await.unwrap();
}
