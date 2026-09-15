//! 固定逻辑时间的真实 SQLite 接纳/领取比较；服务时间是模型，不代表公网下载速度。
use super::*;
use crate::address::AddressPolicy;
use crate::collection::backpressure::Backpressure;
use crate::collection::backpressure::Mode;
use crate::collection::test_storage::TestStorage as Storage;
use crate::storage::StorageConfig;
use std::collections::HashMap;
const START: i64 = 3_600_000;
fn payload(n: usize) -> VerifiedMetadata {
    VerifiedMetadata::fixture(format!("d4:name8:{n:08}6:pieces0:e").into_bytes())
}
fn p95(values: &mut [u64]) -> Option<u64> {
    values.sort_unstable();
    (!values.is_empty()).then(|| values[(values.len() * 95).div_ceil(100) - 1])
}
/// 以当前接纳策略分别运行退避存量与持续竞争场景，比较首试覆盖、等待和提交。
#[tokio::test]
#[ignore = "显式运行首试缓冲固定输入比较；无网络"]
async fn first_attempt_buffer_comparison() {
    let mut reports = Vec::new();
    for competition in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::open(StorageConfig::new(directory.path()))
            .await
            .unwrap();
        let s = &storage.handle;
        s.enable_fetch(10000);
        let mut catalog = HashMap::new();
        let history = if competition { 9000 } else { 0 };
        for start in (0..history).step_by(1000) {
            let batch: Vec<_> = (start..(start + 1000).min(history))
                .map(|n| {
                    let p = payload(n);
                    catalog.insert(p.info_hash(), n);
                    p.info_hash()
                })
                .collect();
            s.save_hashes(&batch, 0).await.unwrap();
        }
        let retry: Vec<_> = (10000..10050)
            .map(|n| {
                let p = payload(n);
                catalog.insert(p.info_hash(), n);
                p.info_hash()
            })
            .collect();
        s.save_hashes(&retry, START).await.unwrap();
        s.call(move |c| {
            c.execute(
                "UPDATE fetch_jobs
                 SET generation = 1,
                     state = 'retry_wait',
                     due_at = ?1
                 WHERE hash IN (
                     SELECT hash FROM infohashes WHERE first_seen = ?2
                 )",
                params![if competition { START } else { START + 300000 }, START],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        if competition {
            for n in 11000..11064 {
                let p = payload(n);
                catalog.insert(p.info_hash(), n);
                s.discover_peer(p.info_hash(), "8.8.8.8:6881".parse().unwrap(), START)
                    .await
                    .unwrap();
            }
        }
        s.enable_recent_admission(16, crate::address::AddressPolicy::PublicOnly);
        let initial: Vec<_> = (12000..12034)
            .map(|n| {
                let p = payload(n);
                catalog.insert(p.info_hash(), n);
                p.info_hash()
            })
            .collect();
        s.save_hashes(&initial, START).await.unwrap();
        let mut first_claims = HashMap::new();
        let mut workers: Vec<(Claim, i64)> = Vec::new();
        let mut turn = 0;
        let mut classes = [0u64; 4];
        let mut last = [0i64; 4];
        let mut gap = [0i64; 4];
        let mut recent_cursor = None;
        let mut history_cursor = None;
        let mut policy = Backpressure::new(Mode::Freshness, 10000, 4);
        let mut paused_at = None;
        let mut resumed_at = None;
        let mut calls = Vec::new();
        let (mut peak, mut peak_q, mut committed) = (0, 0, 0);
        let horizon = if competition { 600 } else { 60 };
        for second in 0..=horizon {
            let now = START + second * 1000;
            let mut pending = Vec::new();
            for (claim, until) in workers.drain(..) {
                if until > now {
                    pending.push((claim, until));
                    continue;
                }
                let n = catalog[&claim.job.hash];
                if !competition || n % 3 == 0 {
                    assert_eq!(
                        s.complete_job(claim.job, payload(n), now).await.unwrap(),
                        UpdateResult::Applied
                    );
                    committed += 1;
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
            workers = pending;
            // 固定已观察输入，不把暂停后仍注入的批次解释为真实主动采样量。
            if competition && second > 0 && second % 10 == 0 {
                let n = 13000 + second as usize;
                let p = payload(n);
                catalog.insert(p.info_hash(), n);
                s.save_hashes(&[p.info_hash()], now).await.unwrap();
            }
            if competition && second > 0 && second % 5 == 0 {
                let n = 14000 + second as usize;
                let p = payload(n);
                catalog.insert(p.info_hash(), n);
                s.discover_peer(p.info_hash(), "8.8.8.8:6881".parse().unwrap(), now)
                    .await
                    .unwrap();
            }
            let scan_before = s.backfill_counters.lock().unwrap().total;
            let start = std::time::Instant::now();
            recent_cursor = s.backfill_recent_page(now, recent_cursor).await.unwrap();
            calls.push(start.elapsed().as_micros() as u64);
            let start = std::time::Instant::now();
            history_cursor = s.backfill_page(now, history_cursor).await.unwrap();
            calls.push(start.elapsed().as_micros() as u64);
            let scan_after = s.backfill_counters.lock().unwrap().total;
            assert!(scan_after.recent_scanned - scan_before.recent_scanned <= 128);
            assert!(scan_after.history_scanned - scan_before.history_scanned <= 128);
            peak_q = peak_q.max(
                s.first_attempt_waiting(now, AddressPolicy::PublicOnly)
                    .await
                    .unwrap(),
            );
            while workers.len() < 4 {
                let start = std::time::Instant::now();
                let claim = s
                    .claim_class(
                        now,
                        Some(ClaimClass::ROTATION[turn % 8]),
                        AddressPolicy::PublicOnly,
                    )
                    .await
                    .unwrap();
                calls.push(start.elapsed().as_micros() as u64);
                let Some(claim) = claim else {
                    break;
                };
                turn += 1;
                let class = claim.job.class as usize;
                classes[class] += 1;
                gap[class] = gap[class].max(second - last[class]);
                last[class] = second;
                first_claims
                    .entry(claim.job.hash)
                    .or_insert((now - claim.first_seen) as u64);
                workers.push((claim, now + if competition { 5000 } else { 1000 }));
            }
            let active = s.active_jobs().await.unwrap();
            peak = peak.max(active);
            assert!(active < 10000);
            if second % 5 == 0 {
                let quantity = if admission::POLICY_VERSION == 1 {
                    s.recent_active_jobs(now).await.unwrap()
                } else {
                    s.first_attempt_waiting(now, AddressPolicy::PublicOnly)
                        .await
                        .unwrap()
                };
                policy.update(now, active, Some(quantity), false);
                if policy.paused() {
                    paused_at.get_or_insert(second);
                } else if paused_at.is_some() {
                    resumed_at.get_or_insert(second);
                }
            }
        }
        let missing = initial
            .iter()
            .filter(|h| !first_claims.contains_key(h))
            .count();
        let mut ages: Vec<_> = initial
            .iter()
            .filter_map(|h| first_claims.get(h).copied())
            .collect();
        let age_p95 = if missing == 0 { p95(&mut ages) } else { None };
        if admission::POLICY_VERSION >= 2 {
            assert_eq!(missing, 0);
            assert!(
                age_p95.unwrap() <= if competition { 180000 } else { 15000 },
                "{ages:?}"
            );
            if competition {
                assert!(classes.iter().all(|n| *n > 0));
            } else {
                assert!(ages.iter().all(|age| *age <= 15000));
                assert!(paused_at.is_none() || resumed_at.is_some_and(|s| s <= 45));
            }
        }
        for (claim, _) in workers {
            s.retry_job(claim.job, START + horizon * 1000, RetryReason::Deferred)
                .await
                .unwrap();
        }
        let stats = s.fetch_stats().await.unwrap();
        assert_eq!(stats.running, 0);
        let rows = s
            .call(|c| {
                Ok(c.query_row("SELECT count(*) FROM infohashes", [], |r| {
                    r.get::<_, i64>(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(rows, if competition { 9328 } else { 84 });
        for class in 0..4 {
            gap[class] = gap[class].max(horizon - last[class]);
        }
        if competition && admission::POLICY_VERSION >= 2 {
            assert!(gap[2] <= 10 && gap[3] <= 10);
        }
        let scan = s.backfill_counters.lock().unwrap().total;
        // 回填比较依次记录近期扫描/插入、历史扫描/插入，以及四类延期计数。
        let scan = [
            scan.recent_scanned,
            scan.recent_inserted,
            scan.history_scanned,
            scan.history_inserted,
            scan.deferrals.total_capacity,
            scan.deferrals.first_attempt_buffer,
            scan.deferrals.history_reserve,
            scan.deferrals.sample_waiting_backfill,
        ];
        reports.push(serde_json::json!({
            "competition": competition,
            "policy_version": admission::POLICY_VERSION,
            "hashes": rows,
            "initial_missing_first_claims": missing,
            "initial_claim_age_p95_ms": age_p95,
            "initial_claimed": ages.len(),
            "initial_claim_age_max_ms": ages.iter().max(),
            "claims_by_class": classes,
            "max_service_gap_seconds": gap,
            "metadata_committed": committed,
            "peak_active": peak,
            "peak_first_attempt_waiting": peak_q,
            "paused_at_seconds": paused_at,
            "resumed_at_seconds": resumed_at,
            "database_call_p95_us": p95(&mut calls),
            "backfill_scanned_inserted": scan,
            "running_after_cleanup": stats.running,
        }));
        storage.shutdown().await.unwrap();
        assert!(!directory.path().join("state.sqlite3-wal").exists());
    }
    println!(
        "FIRST_ATTEMPT_COMPARISON={}",
        serde_json::json!({"kind":"fixed_input_sqlite_simulation","workers":4,"results":reports})
    );
}
