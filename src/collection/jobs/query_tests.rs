//! 领取与近期分页的扫描成本回归；用 SQLite VM 指令数避免机器负载影响耗时断言。
use super::admission::enqueue;
use super::*;
use crate::address::AddressPolicy;
use crate::collection::records::upsert_hash;
use crate::collection::test_storage::TestStorage as Storage;
use crate::storage::StorageConfig;
use rusqlite::StatementStatus;

const NOW: i64 = 3_600_000;

fn hash(n: u32) -> InfoHashV1 {
    let mut bytes = [0; 20];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    InfoHashV1(bytes)
}

/// 真实生产领取 SQL 的选择结果与执行成本，包含语句完整执行。
fn claim_steps(connection: &Connection, class: ClaimClass) -> (InfoHashV1, i32) {
    let mut query = connection.prepare(&schedule_sql(Some(class))).unwrap();
    let bytes = query
        .query_row(params![NOW, PEER_HINT_TTL_MS, RECENT_MS], |r| {
            r.get::<_, Vec<u8>>(0)
        })
        .unwrap();
    (
        decode_hash(bytes).unwrap(),
        query.get_status(StatementStatus::VmStep),
    )
}

#[test]
fn claims_do_not_scan_unadmitted_recent_hashes() {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::storage::schema::migrate(&mut connection).unwrap();
    install_peer_policy(&connection, AddressPolicy::PublicOnly).unwrap();
    let tx = connection.transaction().unwrap();
    let mut room = 16;
    for n in 0..16 {
        upsert_hash(&tx, hash(n), if n % 2 == 0 { 0 } else { NOW }).unwrap();
        enqueue(&tx, hash(n), NOW, &mut room).unwrap();
    }
    tx.execute(
        "INSERT INTO peer_hints VALUES (?1, ?2, 6881, ?3)",
        params![hash(2).0.as_slice(), [8u8, 8, 8, 8].as_slice(), NOW],
    )
    .unwrap();
    tx.execute(
        "UPDATE fetch_jobs SET generation=1 WHERE hash=?1",
        [hash(3).0.as_slice()],
    )
    .unwrap();
    tx.commit().unwrap();
    let classes = [
        ClaimClass::Recent,
        ClaimClass::History,
        ClaimClass::Hint,
        ClaimClass::Retry,
    ];
    let before = classes.map(|class| claim_steps(&connection, class));
    assert_eq!(before[0].0, hash(1));
    assert_eq!(before[1].0, hash(0));
    assert_eq!(before[2].0, hash(2));
    assert_eq!(before[3].0, hash(3));

    // 保存更多采样观察，但不扩大活跃任务集；领取不应因此处理这些无关记录。
    let tx = connection.transaction().unwrap();
    for n in 16..20_016 {
        upsert_hash(&tx, hash(n), NOW).unwrap();
    }
    tx.commit().unwrap();
    for (class, (expected, steps)) in classes.into_iter().zip(before) {
        let (actual, expanded_steps) = claim_steps(&connection, class);
        assert_eq!(actual, expected);
        assert!(
            expanded_steps <= steps * 2 + 100,
            "{class:?}: 无关观察增加后指令数从 {steps} 增至 {expanded_steps}"
        );
    }
}

#[test]
fn recent_pages_seek_past_large_prefixes() {
    let mut connection = Connection::open_in_memory().unwrap();
    crate::storage::schema::migrate(&mut connection).unwrap();
    let tx = connection.transaction().unwrap();
    // 同时覆盖相同毫秒内多个 hash，以及跨观察时间的分页。
    for n in 0..20_000 {
        upsert_hash(&tx, hash(n), NOW + i64::from(n / 100)).unwrap();
    }
    tx.commit().unwrap();
    let mut costs = Vec::new();
    for after in [0, 15_000] {
        let mut query = connection.prepare(admission::RECENT_PAGE_SQL).unwrap();
        let page = query
            .query_map(
                params![
                    NOW + 1000,
                    NOW + i64::from(after / 100),
                    hash(after).0.as_slice()
                ],
                |r| r.get::<_, Vec<u8>>(1),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let expected: Vec<_> = (after + 1..=after + 128)
            .map(|n| hash(n).0.to_vec())
            .collect();
        assert_eq!(page, expected);
        costs.push(query.get_status(StatementStatus::VmStep));
    }
    assert!(
        costs[1] <= costs[0] * 2 + 100,
        "相同页大小不应重扫游标前缀：指令数 {costs:?}"
    );
}

/// 窗口前移后重定位过期游标；窗口下界包含全部 hash，上界之外仍不接纳。
#[tokio::test]
async fn recent_backfill_clamps_expired_cursor_and_keeps_window_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(directory.path()))
        .await
        .unwrap();
    let store = &storage.handle;
    store.enable_fetch(20);
    store.enable_recent_admission(20, AddressPolicy::PublicOnly);
    let start = NOW - RECENT_MS;
    for (n, at) in [
        (0, start - 1),
        (1, start),
        (2, start),
        (3, NOW),
        (4, NOW + 1),
    ] {
        store.save_hashes_at(&[hash(n)], at, NOW).await.unwrap();
    }
    let cursor = store
        .backfill_recent_page(NOW, Some((start - 1, hash(u32::MAX))))
        .await
        .unwrap();
    assert_eq!(cursor, Some((NOW, hash(3))));
    let jobs = store
        .call(|c| {
            let mut query = c.prepare("SELECT hash FROM fetch_jobs ORDER BY hash")?;
            Ok(query
                .query_map([], |r| r.get::<_, Vec<u8>>(0))?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .unwrap();
    assert_eq!(
        jobs,
        vec![hash(1).0.to_vec(), hash(2).0.to_vec(), hash(3).0.to_vec()]
    );

    // 到达页尾后回绕，空游标仍保留闭区间下界；未来记录到时才进入下一页。
    assert_eq!(store.backfill_recent_page(NOW, cursor).await.unwrap(), None);
    store.backfill_recent_page(NOW, None).await.unwrap();
    assert_eq!(store.active_jobs().await.unwrap(), 3);
    store.backfill_recent_page(NOW + 1, cursor).await.unwrap();
    assert_eq!(store.active_jobs().await.unwrap(), 4);
    storage.shutdown().await.unwrap();
}

/// 全部已接纳首试积压包含提示、未到期及超龄任务；无关观察不能放大查询成本。
#[tokio::test]
async fn first_attempt_backlog_covers_boundaries_without_scanning_unadmitted_hashes() {
    use super::queries::FirstAttemptBacklog;
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(directory.path()))
        .await
        .unwrap();
    let store = &storage.handle;
    store.enable_fetch(10);
    assert_eq!(
        store.first_attempt_backlog(NOW).await.unwrap(),
        FirstAttemptBacklog {
            waiting: 0,
            older_than_30m: 0,
            oldest_discovery_age_ms: None,
            oldest_due_wait_ms: None,
            not_due: 0,
        }
    );
    // 边界恰好 30 分钟不算超龄；更老一毫秒才算。
    for (n, age) in [
        (0, 0),
        (1, RECENT_MS),
        (2, RECENT_MS + 1),
        (3, RECENT_MS * 2),
        (4, RECENT_MS * 2),
    ] {
        store.save_hashes(&[hash(n)], NOW - age).await.unwrap();
    }
    store
        .discover_peer(hash(1), "8.8.8.8:6881".parse().unwrap(), NOW)
        .await
        .unwrap();
    store
        .call(|connection| {
            connection.execute(
                "UPDATE fetch_jobs SET state='retry_wait', due_at=?1 WHERE hash=?2",
                params![NOW + 99999, hash(2).0.as_slice()],
            )?;
            connection.execute(
                "UPDATE fetch_jobs SET generation=1 WHERE hash=?1",
                [hash(3).0.as_slice()],
            )?;
            connection.execute(
                "UPDATE fetch_jobs SET state='running', generation=1 WHERE hash=?1",
                [hash(4).0.as_slice()],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let expected = FirstAttemptBacklog {
        waiting: 3,
        older_than_30m: 1,
        oldest_discovery_age_ms: Some(RECENT_MS + 1),
        oldest_due_wait_ms: Some(RECENT_MS),
        not_due: 1,
    };
    assert_eq!(store.first_attempt_backlog(NOW).await.unwrap(), expected);
    store
        .call(|connection| {
            let steps = |connection: &Connection| {
                let mut query = connection
                    .prepare(queries::FIRST_ATTEMPT_BACKLOG_SQL)
                    .unwrap();
                let count: i64 = query
                    .query_row(params![NOW, RECENT_MS], |row| row.get(0))
                    .unwrap();
                assert_eq!(count, 3);
                query.get_status(StatementStatus::VmStep)
            };
            let before = steps(connection);
            let tx = connection.transaction()?;
            for n in 10..20010 {
                upsert_hash(&tx, hash(n), NOW)?;
            }
            tx.commit()?;
            let after = steps(connection);
            assert!(
                after <= before * 2 + 100,
                "未接纳观察使指令数从 {before} 增至 {after}"
            );
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(store.first_attempt_backlog(NOW).await.unwrap(), expected);
    // 全部未到期时最大到期等待为 0，而不是缺失值；发现年龄保持原值。
    store
        .call(|connection| {
            connection.execute(
                "UPDATE fetch_jobs SET due_at=?1 WHERE generation=0",
                [NOW + 1],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let future = store.first_attempt_backlog(NOW).await.unwrap();
    assert_eq!(future.oldest_due_wait_ms, Some(0));
    assert_eq!(future.not_due, 3);
    assert_eq!(
        future.oldest_discovery_age_ms,
        expected.oldest_discovery_age_ms
    );
    storage.shutdown().await.unwrap();
}

/// 同一读取时间覆盖空库、混合状态和提示边界；快照与单项查询保持同一口径。
#[tokio::test]
async fn status_snapshot_matches_individual_queries_and_propagates_failure() {
    let directory = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(directory.path()))
        .await
        .unwrap();
    let store = &storage.handle;
    store.enable_fetch(16);
    let empty = store
        .status_snapshot(NOW, AddressPolicy::PublicOnly)
        .await
        .unwrap();
    assert_eq!(empty.stats.active(), 0);
    assert_eq!(empty.due.count, 0);
    assert_eq!(empty.backlog.oldest_discovery_age_ms, None);
    for (n, at) in [
        (0, NOW),
        (1, NOW - RECENT_MS - 1),
        (2, NOW + 1),
        (3, NOW),
        (4, NOW),
        (5, NOW),
        (6, NOW),
        (7, NOW),
    ] {
        store.save_hashes(&[hash(n)], at).await.unwrap();
    }
    store
        .call(|c| {
            for (n, state) in [
                (3, "running"),
                (4, "retry_wait"),
                (5, "dormant"),
                (6, "succeeded"),
            ] {
                c.execute(
                    "UPDATE fetch_jobs SET state=?2,generation=1 WHERE hash=?1",
                    params![hash(n).0.as_slice(), state],
                )?;
            }
            c.execute(
                "UPDATE fetch_jobs SET due_at=?2 WHERE hash=?1",
                params![hash(7).0.as_slice(), NOW + 100],
            )?;
            for (n, ip, observed) in [
                (0, [8u8, 8, 8, 8], NOW),
                (1, [9, 9, 9, 9], NOW - PEER_HINT_TTL_MS - 1),
                (4, [127, 0, 0, 1], NOW),
            ] {
                c.execute(
                    "INSERT INTO peer_hints VALUES (?1,?2,6881,?3)",
                    params![hash(n).0.as_slice(), ip.as_slice(), observed],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();
    for at in [NOW - 1, NOW, NOW + 200, NOW + RECENT_MS + 1] {
        for policy in [AddressPolicy::PublicOnly, AddressPolicy::LocalUnicast] {
            let snapshot = store.status_snapshot(at, policy).await.unwrap();
            let due = store.due_stats(at, policy).await.unwrap();
            assert_eq!(
                (
                    snapshot.due.count,
                    snapshot.due.oldest_wait_ms,
                    snapshot.due.fresh
                ),
                (due.count, due.oldest_wait_ms, due.fresh)
            );
            assert_eq!(
                serde_json::to_value(&snapshot.stats).unwrap(),
                serde_json::to_value(store.fetch_stats().await.unwrap()).unwrap()
            );
            assert_eq!(
                snapshot.recent_active,
                store.recent_active_jobs(at).await.unwrap()
            );
            assert_eq!(
                snapshot.first_attempt_waiting,
                store.first_attempt_waiting(at, policy).await.unwrap()
            );
            assert_eq!(
                snapshot.backlog,
                store.first_attempt_backlog(at).await.unwrap()
            );
        }
    }
    let current = store
        .status_snapshot(NOW, AddressPolicy::PublicOnly)
        .await
        .unwrap();
    assert_eq!(current.due.fresh, 1);
    assert_eq!(current.stats.running, 1);
    assert_eq!(current.stats.dormant, 1);
    assert_eq!(current.backlog.not_due, 2);
    // 最后一个查询依赖该索引；前几个查询成功也不能返回部分快照。
    store
        .call(|c| {
            c.execute_batch("DROP INDEX fetch_claim_due")?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(matches!(
        store.status_snapshot(NOW, AddressPolicy::PublicOnly).await,
        Err(StorageError::Database(_))
    ));
    // 失败事务已经结束，连接仍可执行下一条操作。
    store
        .call(|c| {
            assert!(c.is_autocommit());
            Ok(())
        })
        .await
        .unwrap();
    storage.shutdown().await.unwrap();
}
