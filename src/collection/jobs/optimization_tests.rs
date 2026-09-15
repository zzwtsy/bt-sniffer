//! 新调度和接纳的数据库回归；所有状态位于临时目录，使用受控 UTC 时间。
use super::*;
use crate::address::AddressPolicy;
use crate::collection::test_storage::TestStorage as Storage;
use crate::storage::StorageConfig;
const NOW: i64 = 3_600_000;
fn hash(n: u32) -> InfoHashV1 {
    let mut bytes = [0; 20];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    InfoHashV1(bytes)
}

#[tokio::test]
async fn delayed_sample_uses_current_admission_window() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(5);
    s.enable_recent_admission(4, crate::address::AddressPolicy::PublicOnly);
    s.save_hashes_at(&[hash(1), hash(2)], 0, NOW).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 0);
    s.backfill_page(NOW, None).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 1);
    assert_eq!(s.recent_active_jobs(NOW).await.unwrap(), 0);
    s.save_hashes_at(&[hash(3), hash(4), hash(5), hash(6)], NOW, NOW)
        .await
        .unwrap();
    s.backfill_recent_page(NOW, None).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 5);
    assert_eq!(s.recent_active_jobs(NOW).await.unwrap(), 4);
    s.call(|c| {
        let (count, first): (i64, i64) = c.query_row(
            "SELECT count(*), min(first_seen) FROM infohashes",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!((count, first), (6, 0));
        Ok(())
    })
    .await
    .unwrap();
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn four_classes_rotate_and_recovery_remains_retry() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(100);
    for (n, class) in ClaimClass::ROTATION.into_iter().enumerate() {
        let h = hash(n as u32);
        s.save_hashes(&[h], if class == ClaimClass::History { 0 } else { NOW })
            .await
            .unwrap();
        if class == ClaimClass::Hint {
            s.discover_peer(h, "8.8.8.8:6881".parse().unwrap(), NOW)
                .await
                .unwrap();
        }
        if class == ClaimClass::Retry {
            s.call(move |c| {
                c.execute(
                    "UPDATE fetch_jobs SET generation=1,state='retry_wait' WHERE hash=?1",
                    [h.0.as_slice()],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        }
    }
    assert_eq!(
        ClaimClass::ROTATION,
        [
            ClaimClass::Hint,
            ClaimClass::Recent,
            ClaimClass::Retry,
            ClaimClass::Hint,
            ClaimClass::History,
            ClaimClass::Recent,
            ClaimClass::Retry,
            ClaimClass::Hint,
        ]
    );
    let mut first = 0;
    let mut repeat = 0;
    for class in ClaimClass::ROTATION {
        let claim = s
            .claim_class(NOW, Some(class), AddressPolicy::PublicOnly)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.job.class, class);
        assert!(claim.due_at <= NOW);
        match claim.job.attempt_kind() {
            AttemptKind::First => first += 1,
            AttemptKind::Repeat => repeat += 1,
        }
        assert_eq!(claim.job.had_valid_hint, class == ClaimClass::Hint);
    }
    assert_eq!((first, repeat), (6, 2));
    assert!(
        s.claim_class(NOW, None, AddressPolicy::PublicOnly)
            .await
            .unwrap()
            .is_none()
    );
    s.recover_jobs(NOW + 1).await.unwrap();
    let claim = s
        .claim_class(
            NOW + RECENT_MS + 1,
            Some(ClaimClass::Recent),
            AddressPolicy::PublicOnly,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        claim.job.class,
        ClaimClass::Retry,
        "提示过期和恢复不会把已领取任务变成首试"
    );
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn batches_keep_all_hashes_and_reserve_recent_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    let history: Vec<_> = (0..100).map(hash).collect();
    s.save_hashes(&history, 0).await.unwrap();
    s.enable_fetch(20);
    s.enable_recent_admission(4, crate::address::AddressPolicy::PublicOnly);
    s.backfill_page(NOW, None).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 16);
    let recent: Vec<_> = (100..200).map(hash).collect();
    s.save_hashes(&recent, NOW).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 16);
    s.backfill_recent_page(NOW, None).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 20);
    assert_eq!(s.recent_active_jobs(NOW).await.unwrap(), 4);
    let count = s
        .call(|c| {
            Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| {
                r.get::<_, i64>(0)
            })?)
        })
        .await
        .unwrap();
    assert_eq!(count, 200);
    s.save_hashes(&recent, NOW + 1).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 20);
    let job = s
        .claim_class(NOW, Some(ClaimClass::Recent), AddressPolicy::LocalUnicast)
        .await
        .unwrap()
        .unwrap()
        .job;
    s.retry_job(job, NOW, RetryReason::Deferred).await.unwrap();
    assert_eq!(
        s.recent_active_jobs(NOW + 1).await.unwrap(),
        4,
        "旧 recent_active 指标仍含未到期重试，已不作为额度依据"
    );
    s.backfill_recent_page(NOW + 1, None).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 20);
    assert_eq!(
        s.recent_active_jobs(NOW + RECENT_MS + 1).await.unwrap(),
        0,
        "重复观察不延长窗口"
    );
    storage.shutdown().await.unwrap();
}

#[tokio::test]
async fn small_limits_overfull_existing_jobs_and_rollback_preserve_data() {
    for limit in 1..=4 {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
        let s = &storage.handle;
        s.enable_fetch(limit);
        s.enable_recent_admission(limit, crate::address::AddressPolicy::PublicOnly);
        s.save_hashes(&(0..10).map(hash).collect::<Vec<_>>(), NOW)
            .await
            .unwrap();
        s.backfill_recent_page(NOW, None).await.unwrap();
        assert_eq!(s.active_jobs().await.unwrap(), limit as i64);
        s.enable_recent_admission(1, crate::address::AddressPolicy::PublicOnly);
        s.save_hashes(&[hash(20)], NOW).await.unwrap();
        assert_eq!(s.active_jobs().await.unwrap(), limit as i64);
        s.call(|c| {
            c.execute_batch(
                "CREATE TRIGGER reject_sample
                 BEFORE INSERT ON infohashes
                 WHEN NEW.hash = x'0000001f00000000000000000000000000000000'
                 BEGIN
                     SELECT RAISE(ABORT, 'test rollback');
                 END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(s.save_hashes(&[hash(30), hash(31)], NOW).await.is_err());
        let exists = s
            .call(|c| {
                Ok(c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM infohashes WHERE hash=?1)",
                    [hash(30).0.as_slice()],
                    |r| r.get::<_, bool>(0),
                )?)
            })
            .await
            .unwrap();
        assert!(!exists);
        storage.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn recent_backfill_and_announce_refresh_are_transactional() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.save_hashes(&[hash(0), hash(1), hash(2)], NOW)
        .await
        .unwrap();
    s.enable_fetch(10);
    s.enable_recent_admission(2, crate::address::AddressPolicy::PublicOnly);
    s.backfill_recent_page(NOW, None).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 2);
    let claim = s
        .claim_class(NOW, Some(ClaimClass::Recent), AddressPolicy::PublicOnly)
        .await
        .unwrap()
        .unwrap();
    s.retry_job(claim.job, NOW, RetryReason::Deferred)
        .await
        .unwrap();
    s.discover_peer(hash(0), "8.8.8.8:6881".parse().unwrap(), NOW + 1)
        .await
        .unwrap();
    let claim = s
        .claim_class(NOW + 1, Some(ClaimClass::Hint), AddressPolicy::PublicOnly)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.job.hash, hash(1), "提示不能提前领取尚未到期的重试");
    storage.shutdown().await.unwrap();
}

#[test]
fn production_queries_use_due_and_recent_indexes() {
    let mut c = Connection::open_in_memory().unwrap();
    crate::storage::schema::migrate(&mut c).unwrap();
    install_peer_policy(&c, AddressPolicy::PublicOnly).unwrap();
    for class in ClaimClass::ROTATION {
        let mut q = c
            .prepare(&format!("EXPLAIN QUERY PLAN {}", schedule_sql(Some(class))))
            .unwrap();
        let plan = q
            .query_map(params![NOW, PEER_HINT_TTL_MS, RECENT_MS], |r| {
                r.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        let index = if class == ClaimClass::Retry {
            "fetch_retry_due"
        } else {
            "fetch_claim_due"
        };
        assert!(plan.contains(index), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
    }
    let mut q = c
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {}",
            admission::RECENT_PAGE_SQL
        ))
        .unwrap();
    let plan = q
        .query_map(params![NOW, NOW - RECENT_MS, Vec::<u8>::new()], |r| {
            r.get::<_, String>(3)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");
    assert!(plan.contains("infohash_first_seen"), "{plan}");
}

/// 保留旧 3:1 查询的测试基线；不提供生产回退开关。
async fn baseline_claim(s: &CollectionStore, now: i64, turn: usize) -> Option<Claim> {
    s.call(move |c| {
        let tx = c.transaction()?;
        install_peer_policy(&tx, AddressPolicy::PublicOnly)?;
        let preferred = turn % 4 < 3;
        let mut selected = None;
        for preference in [preferred, !preferred] {
            selected = tx
                .query_row(
                    CLAIM_SQL,
                    params![now, PEER_HINT_TTL_MS, preference],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, bool>(3)?,
                        ))
                    },
                )
                .optional()?;
            if selected.is_some() {
                break;
            }
        }
        let Some((bytes, generation, due_at, hint)) = selected else {
            return Ok(None);
        };
        let hash = decode_hash(bytes)?;
        let first_seen = tx.query_row(
            "SELECT first_seen FROM infohashes WHERE hash = ?1",
            [hash.0.as_slice()],
            |row| row.get::<_, i64>(0),
        )?;
        let class = if hint {
            ClaimClass::Hint
        } else if generation > 0 {
            ClaimClass::Retry
        } else if first_seen >= now - RECENT_MS {
            ClaimClass::Recent
        } else {
            ClaimClass::History
        };
        tx.execute(
            "UPDATE fetch_jobs
             SET state = 'running',
                 generation = generation + 1,
                 updated_at = ?2
             WHERE hash = ?1",
            params![hash.0.as_slice(), now],
        )?;
        let failed_attempts_before = tx.query_row(
            "SELECT attempts FROM fetch_jobs WHERE hash = ?1",
            [hash.0.as_slice()],
            |row| row.get(0),
        )?;
        tx.commit()?;
        Ok(Some(Claim {
            job: Job {
                had_valid_hint: hint,
                class,
                hash,
                generation: generation + 1,
                failed_attempts_before,
                peers: vec![],
            },
            due_at,
            first_seen,
        }))
    })
    .await
    .unwrap()
}
fn sample(n: usize) -> VerifiedMetadata {
    VerifiedMetadata::fixture(format!("d4:name8:{n:08}6:pieces0:e").into_bytes())
}

#[tokio::test]
#[ignore = "独立固定 SQLite 输入与模拟服务时间，非公网吞吐"]
async fn backlog_release_comparison() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "使用 Release"
    );
    let mut results = Vec::new();
    for optimized in [false, true] {
        let dir = tempfile::Builder::new()
            .prefix("bt-backlog-comparison-")
            .tempdir()
            .unwrap()
            .keep();
        let mut report = crate::acceptance::Report::new(
            "backlog-simulation",
            &dir,
            serde_json::json!({
                "optimized": optimized,
                "history": 9000,
                "workers": 4,
                "horizon_seconds": 600,
                "arrival_per_second": 4,
                "service_ms": 5000,
            }),
        );
        report.running();
        let storage = Storage::open(StorageConfig::new(&dir)).await.unwrap();
        let s = &storage.handle;
        let mut payloads = std::collections::HashMap::new();
        for n in 0..9000 {
            let value = sample(n);
            payloads.insert(value.info_hash(), n);
        }
        let initial: Vec<_> = (0..9000).map(|n| sample(n).info_hash()).collect();
        for batch in initial.chunks(1000) {
            s.save_hashes(batch, 0).await.unwrap();
        }
        s.enable_fetch(10000);
        if optimized {
            s.enable_recent_admission(16, crate::address::AddressPolicy::PublicOnly);
        }
        let mut cursor = None;
        for _ in 0..80 {
            cursor = s.backfill_page(NOW, cursor).await.unwrap();
        }
        let mut active: Vec<(Claim, i64)> = Vec::new();
        let mut turn = 0usize;
        let mut counts = [0u64; 4];
        let mut waits = Vec::new();
        let mut successes = 0;
        let mut peak = 0;
        let mut recent_cursor = None;
        let mut query_us = Vec::new();
        for second in 0..600 {
            let now = NOW + second * 1000;
            let mut pending = Vec::new();
            for (claim, until) in active.drain(..) {
                if until > now {
                    pending.push((claim, until));
                    continue;
                }
                let n = payloads[&claim.job.hash];
                if n % 3 == 0 {
                    s.complete_job(claim.job, sample(n), now).await.unwrap();
                    successes += 1;
                } else {
                    let hash = claim.job.hash;
                    s.retry_job(
                        claim.job,
                        now,
                        RetryReason::Failed(
                            crate::collection::failure::AttemptFailure::PeerTimeout,
                        ),
                    )
                    .await
                    .unwrap();
                    // 比较固定输入：仅在此测试消除随机 jitter，保留指数退避基数。
                    s.call(move |c| {
                        c.execute(
                            "UPDATE fetch_jobs
                             SET due_at = ?2 + 60000 * (1 << (attempts - 1))
                             WHERE hash = ?1",
                            params![hash.0.as_slice(), now],
                        )?;
                        Ok(())
                    })
                    .await
                    .unwrap();
                }
            }
            active = pending;
            let mut batch = Vec::new();
            for offset in 0..4 {
                let n = 9000 + second as usize * 4 + offset;
                let h = sample(n).info_hash();
                payloads.insert(h, n);
                batch.push(h);
            }
            s.save_hashes(&batch, now).await.unwrap();
            if second % 10 == 0 {
                s.discover_peer(batch[0], "8.8.8.8:6881".parse().unwrap(), now)
                    .await
                    .unwrap();
            }
            if optimized {
                recent_cursor = s.backfill_recent_page(now, recent_cursor).await.unwrap();
            }
            cursor = s.backfill_page(now, cursor).await.unwrap();
            while active.len() < 4 {
                let at = std::time::Instant::now();
                let claim = if optimized {
                    s.claim_class(
                        now,
                        Some(ClaimClass::ROTATION[turn % 8]),
                        AddressPolicy::PublicOnly,
                    )
                    .await
                    .unwrap()
                } else {
                    baseline_claim(s, now, turn).await
                };
                query_us.push(at.elapsed().as_micros() as u64);
                let Some(claim) = claim else {
                    break;
                };
                turn += 1;
                counts[claim.job.class as usize] += 1;
                if claim.job.class == ClaimClass::Recent {
                    waits.push(now - claim.first_seen);
                }
                active.push((claim, now + 5000));
            }
            let count = s.active_jobs().await.unwrap();
            assert!(count <= 10000);
            peak = peak.max(count);
        }
        for (claim, _) in active {
            s.retry_job(claim.job, NOW + 600000, RetryReason::Deferred)
                .await
                .unwrap();
        }
        let rows = s
            .call(|c| {
                Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| {
                    r.get::<_, i64>(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(rows, 11400);
        waits.sort_unstable();
        query_us.sort_unstable();
        if optimized {
            assert!(counts.iter().all(|&n| n > 0), "{counts:?}");
            assert!(!waits.is_empty());
        }
        let recent_age_p95_ms = if waits.is_empty() {
            None
        } else {
            Some(waits[(waits.len() * 95).div_ceil(100) - 1])
        };
        let value = serde_json::json!({
            "optimized": optimized,
            "claims_by_class": counts,
            "recent_age_p95_ms": recent_age_p95_ms,
            "claim_call_p95_us": query_us[(query_us.len() * 95).div_ceil(100) - 1],
            "metadata_committed": successes,
            "peak_active": peak,
            "hashes": rows,
        });
        storage.shutdown().await.unwrap();
        assert!(!dir.join("state.sqlite3-wal").exists());
        report.value["statistics"] = value.clone();
        report.finish(true, true).expect("验收报告必须成功保存");
        results.push(value);
    }
    assert!(
        results[1]["claims_by_class"][1].as_u64().unwrap()
            > results[0]["claims_by_class"][1].as_u64().unwrap()
    );
    println!(
        "BACKLOG_COMPARISON={}",
        serde_json::json!({"kind":"simulation","results":results})
    );
}

/// Q 与领取使用同一地址策略；到期时间不影响 Q，提示失效允许已有队列超过 B。
#[tokio::test]
async fn first_attempt_buffer_counts_policy_expiry_and_generation() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(20);
    s.enable_recent_admission(2, AddressPolicy::LocalUnicast);
    for n in 0..4 {
        s.discover_peer(hash(n), "127.0.0.1:6881".parse().unwrap(), NOW)
            .await
            .unwrap();
    }
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::PublicOnly)
            .await
            .unwrap(),
        4
    );
    s.save_hashes(&[hash(4), hash(5), hash(6)], NOW)
        .await
        .unwrap();
    s.backfill_recent_page(NOW, None).await.unwrap();
    assert_eq!(
        s.active_jobs().await.unwrap(),
        6,
        "补建不能沿用上一次查询安装的 PublicOnly 策略"
    );
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        2
    );
    s.call(|c| {
        c.execute(
            "UPDATE fetch_jobs SET due_at=?1+60000,state='retry_wait' WHERE hash=?2",
            params![NOW, hash(4).0.as_slice()],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        2,
        "未到期首试仍占 Q"
    );
    let claim = s
        .claim_class(NOW, Some(ClaimClass::Recent), AddressPolicy::LocalUnicast)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.job.hash, hash(5));
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        1,
        "领取事务立即释放 Q"
    );
    s.retry_job(claim.job, NOW, RetryReason::Deferred)
        .await
        .unwrap();
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        1
    );
    // 提前到期的旧 hint 让仍在 30 分钟窗口内的 4 个 hash 同时进入 Q。
    s.call(|c| {
        c.execute(
            "UPDATE peer_hints SET observed_at=?1-?2",
            params![NOW, PEER_HINT_TTL_MS],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        s.first_attempt_waiting(NOW, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        s.first_attempt_waiting(NOW + 1, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        5
    );
    s.backfill_recent_page(NOW + 1, None).await.unwrap();
    assert_eq!(
        s.active_jobs().await.unwrap(),
        6,
        "已有 Q 超限不驱逐，不继续补建"
    );
    assert_eq!(
        s.first_attempt_waiting(NOW + RECENT_MS, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        s.first_attempt_waiting(NOW + RECENT_MS + 1, AddressPolicy::LocalUnicast)
            .await
            .unwrap(),
        0
    );
    storage.shutdown().await.unwrap();
}

/// 新批次不得抢占等待者；游标越过已完成前缀，绕回后才能看到插入在游标前的旧观察。
#[tokio::test]
async fn unified_backfill_preserves_cursor_budget_and_dormant_contract() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(1000);
    s.enable_recent_admission(2, AddressPolicy::PublicOnly);
    s.save_hashes(&(10..310).map(hash).collect::<Vec<_>>(), NOW)
        .await
        .unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 0);
    let cursor = s.backfill_recent_page(NOW, None).await.unwrap();
    assert_eq!(cursor, Some((NOW, hash(11))));
    s.save_hashes(&[hash(400)], NOW + 1).await.unwrap();
    assert_eq!(s.active_jobs().await.unwrap(), 2);
    s.call(|c| {
        c.execute(
            "UPDATE fetch_jobs SET generation=1,state='retry_wait',due_at=?1+300000",
            [NOW],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    let cursor = s.backfill_recent_page(NOW + 1, cursor).await.unwrap();
    assert_eq!(cursor, Some((NOW, hash(13))));
    // 留下大量已领取的前缀，验证每次最多检查 128 行而不重复从头扫。
    s.call(|c| {
        c.execute(
            "UPDATE fetch_jobs SET generation=1,state='dormant',updated_at=0",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    s.enable_recent_admission(400, AddressPolicy::PublicOnly);
    let before = s.backfill_counters.lock().unwrap().total.recent_scanned;
    let cursor = s.backfill_recent_page(NOW + 1, cursor).await.unwrap();
    assert_eq!(
        s.backfill_counters.lock().unwrap().total.recent_scanned - before,
        128
    );
    s.save_hashes(&[hash(1)], NOW - 1).await.unwrap();
    let mut cursor = cursor;
    while cursor.is_some() {
        cursor = s.backfill_recent_page(NOW + 1, cursor).await.unwrap();
    }
    s.backfill_recent_page(NOW + 1, cursor).await.unwrap();
    assert!(
        s.call(|c| Ok(c.query_row(
            "SELECT EXISTS(SELECT 1 FROM fetch_jobs WHERE hash=?1)",
            [hash(1).0.as_slice()],
            |r| r.get::<_, bool>(0)
        )?))
        .await
        .unwrap()
    );
    assert_eq!(
        s.fetch_stats().await.unwrap().dormant,
        4,
        "补建不能复活休眠任务"
    );
    s.save_hashes(&[hash(10)], DORMANT_REACTIVATION_DELAY_MS)
        .await
        .unwrap();
    let generation = s
        .call(|c| {
            Ok(c.query_row(
                "SELECT generation FROM fetch_jobs WHERE hash=?1 AND state='pending'",
                [hash(10).0.as_slice()],
                |r| r.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(generation, 1, "观察按冷却复活，保留 generation");
    storage.shutdown().await.unwrap();
}

#[test]
fn first_attempt_count_query_has_bounded_indexed_inputs() {
    let mut c = Connection::open_in_memory().unwrap();
    crate::storage::schema::migrate(&mut c).unwrap();
    install_peer_policy(&c, AddressPolicy::LocalUnicast).unwrap();
    let mut q = c
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {}",
            admission::FIRST_ATTEMPT_SQL
        ))
        .unwrap();
    let plan = q
        .query_map(params![NOW, RECENT_MS, PEER_HINT_TTL_MS], |r| {
            r.get::<_, String>(3)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join("\n");
    println!("FIRST_ATTEMPT_QUERY_PLAN={plan}");
    assert!(!plan.contains("SCAN i"), "{plan}");
    assert!(!plan.contains("SCAN h"), "{plan}");
    assert!(!plan.contains("TEMP B-TREE"), "{plan}");
}

/// 新提示提供连接地址，但不能绕过未到期任务的退避；已返回的领取分类保持不变。
#[tokio::test]
async fn repeat_hints_preserve_due_time_and_frozen_claim() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(StorageConfig::new(dir.path())).await.unwrap();
    let s = &storage.handle;
    s.enable_fetch(10);
    let h = hash(991);
    s.discover_peer(h, "8.8.8.8:6881".parse().unwrap(), NOW)
        .await
        .unwrap();
    let first = s
        .claim_class(NOW, None, AddressPolicy::PublicOnly)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.job.class, ClaimClass::Hint);
    assert!(first.job.had_valid_hint);
    let frozen = first.job.clone();
    s.retry_job(
        first.job,
        NOW,
        RetryReason::Failed(crate::collection::failure::AttemptFailure::PeerTimeout),
    )
    .await
    .unwrap();
    let before = s
        .call(move |c| {
            Ok(c.query_row(
                "SELECT due_at, attempts FROM fetch_jobs WHERE hash=?1",
                [h.0.as_slice()],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )?)
        })
        .await
        .unwrap();
    for address in ["8.8.8.8:6881", "1.1.1.1:6882"] {
        s.discover_peer(h, address.parse().unwrap(), NOW + 1)
            .await
            .unwrap();
    }
    let after = s
        .call(move |c| {
            Ok(c.query_row(
                "SELECT due_at, attempts FROM fetch_jobs WHERE hash=?1",
                [h.0.as_slice()],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(before, after);
    assert!(
        s.claim_class(NOW + 1, Some(ClaimClass::Hint), AddressPolicy::PublicOnly)
            .await
            .unwrap()
            .is_none()
    );
    // 在到期前刷新提示，领取时仍归 Retry，并携带连接地址。
    s.discover_peer(h, "1.1.1.1:6882".parse().unwrap(), before.0)
        .await
        .unwrap();
    let repeat = s
        .claim_class(before.0, Some(ClaimClass::Hint), AddressPolicy::PublicOnly)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeat.job.class, ClaimClass::Retry);
    assert!(repeat.job.had_valid_hint);
    assert!(!repeat.job.peers.is_empty());
    assert_eq!(repeat.job.failed_attempts_before, 1);
    assert_eq!(frozen.class, ClaimClass::Hint);
    assert_eq!(frozen.attempt_kind(), AttemptKind::First);
    storage.shutdown().await.unwrap();
}
