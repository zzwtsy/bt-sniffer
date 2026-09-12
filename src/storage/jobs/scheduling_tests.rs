//! 固定 SQLite 输入：10,000 活跃任务与 40,000 条历史记录。
use super::*;
use crate::net::address::AddressPolicy;
use serde_json::json;
fn fixture(path: &std::path::Path) -> Connection {
    let mut c = Connection::open(path).unwrap();
    crate::storage::schema::migrate(&mut c).unwrap();
    let tx = c.transaction().unwrap();
    {
        let mut hashes = tx
            .prepare("INSERT INTO infohashes VALUES(?1,100,100)")
            .unwrap();
        let mut jobs = tx
            .prepare(
                "INSERT INTO fetch_jobs(hash,state,due_at,updated_at) VALUES(?1,'pending',?2,100)",
            )
            .unwrap();
        let mut hints = tx
            .prepare("INSERT INTO peer_hints VALUES(?1,x'08080808',6881,1000)")
            .unwrap();
        for n in 0..50000u32 {
            let mut hash = [0; 20];
            hash[..4].copy_from_slice(&n.to_be_bytes());
            hashes.execute([hash.as_slice()]).unwrap();
            if n < 10000 {
                jobs.execute(params![hash.as_slice(), if n < 2500 { 100 } else { 200 }])
                    .unwrap();
            }
            if (2500..10000).contains(&n) {
                hints.execute([hash.as_slice()]).unwrap();
            }
        }
    }
    tx.commit().unwrap();
    install_peer_policy(&c, AddressPolicy::PublicOnly).unwrap();
    c
}
#[test]
fn claim_query_plan_bounds_history_scan_and_uses_hint_key() {
    let dir = tempfile::tempdir().unwrap();
    let c = fixture(&dir.path().join("plan.sqlite3"));
    for preference in [true, false] {
        let mut q = c
            .prepare(&format!("EXPLAIN QUERY PLAN {CLAIM_SQL}"))
            .unwrap();
        let plan = q
            .query_map(params![1000, PEER_HINT_TTL_MS, preference], |r| {
                r.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        assert!(plan.contains("fetch_claim_due"), "{plan}");
        assert!(plan.contains("sqlite_autoindex_peer_hints_1"), "{plan}");
        assert!(!plan.contains("TEMP B-TREE"), "{plan}");
        assert!(!plan.contains("SCAN infohashes"), "{plan}");
    }
}
/// 短的 Release 调度模拟；不是公网吞吐或真实 TCP 下载性能证据。
#[test]
#[ignore = "独立短基准：cargo test --release scheduling_release_comparison -- --ignored --nocapture"]
fn scheduling_release_comparison() {
    assert!(
        !std::hint::black_box(cfg!(debug_assertions)),
        "固定使用 Release 配置"
    );
    let directory = tempfile::Builder::new()
        .prefix("bt-sniffer-scheduling-")
        .tempdir()
        .unwrap()
        .keep();
    let mut report = crate::acceptance::Report::new(
        "scheduler-comparison",
        &directory,
        json!({"active":10000,"history":40000,"workers":4,"simulated_horizon_ms":2000,"fresh_service_ms":10,"ordinary_service_ms":100,"input":"2500 older ordinary then 7500 fresh; fixed big-endian hashes"}),
    );
    report.running();
    let mut results = Vec::new();
    for preferred in [false, true] {
        let c = fixture(&directory.join(if preferred {
            "new.sqlite3"
        } else {
            "old.sqlite3"
        }));
        if !preferred {
            c.execute("DROP INDEX fetch_claim_due", []).unwrap();
        }
        let sql = if preferred {
            CLAIM_SQL.to_owned()
        } else {
            CLAIM_SQL.replace(" INDEXED BY fetch_claim_due", "")
        };
        let start = std::time::Instant::now();
        let mut running = Vec::new();
        let mut waits = Vec::new();
        let mut class_claims = [0u64; 2];
        let mut completed = 0;
        let mut turn = 0;
        for now in 1000..3000i64 {
            running.retain(|due| {
                if *due <= now {
                    completed += 1;
                    false
                } else {
                    true
                }
            });
            while running.len() < 4 {
                let preference = if preferred { Some(turn % 4 < 3) } else { None };
                let (hash, _, due, fresh): ClaimRow = c
                    .query_row(&sql, params![now, PEER_HINT_TTL_MS, preference], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                    })
                    .unwrap();
                c.execute(
                    "UPDATE fetch_jobs SET state='running' WHERE hash=?1",
                    [hash],
                )
                .unwrap();
                waits.push(now - due);
                class_claims[usize::from(fresh)] += 1;
                running.push(now + if fresh { 10 } else { 100 });
                turn += 1;
            }
        }
        waits.sort_unstable();
        results.push(json!({"scheduler":if preferred {"3:1"} else {"old_due_order"},"completed":completed,"claimed_ordinary":class_claims[0],"claimed_fresh":class_claims[1],"wait_p50_ms":waits[waits.len()/2],"wait_p95_ms":waits[waits.len()*95/100],"wall_seconds":start.elapsed().as_secs_f64()}));
    }
    report.value["statistics"] = json!(results);
    report.value["verification"] = json!([
        "same release binary and fixed input",
        "simulated completion and claim wait only; no network throughput claim"
    ]);
    report.finish(true, true);
    eprintln!("{}", report.value);
}
