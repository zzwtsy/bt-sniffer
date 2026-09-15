//! 显式本机长时间验收，不属于默认测试。
use super::*;

/// 显式运行的真实时间耐久测试；混合合法宣布、重复 hash、无效 token 和 DHT 服务查询。
#[tokio::test]
#[ignore = "需要 30 分钟真实时间和本机 socket"]
async fn sustained_mixed_loopback_30_minutes() {
    let signal = crate::app::shutdown_signal().expect("注册验收退出信号");
    let dir = tempfile::Builder::new()
        .prefix("bt-sniffer-local-acceptance-")
        .tempdir()
        .unwrap()
        .keep();
    let mut report = crate::acceptance::Report::new(
        "local-thirty-minutes",
        &dir,
        serde_json::json!({"planned_seconds":1800,"workers":2,"max_active":16,"rss_growth_limit_bytes":33554432,"dht_defaults":true}),
    );
    let Fixture {
        mut session,
        handle,
        address,
    } = fixture(&dir, AddressFamily::Ipv4).await;
    report.running();
    let budget = session.test_budget();
    let store = session.test_store();
    session.start_fetch(config(&dir)).await.unwrap();
    let sender = UdpTransport::bind("127.0.0.1:0", Default::default())
        .await
        .unwrap();
    let flood = UdpTransport::bind("127.0.0.3:0", Default::default())
        .await
        .unwrap();
    let pressure = tokio::spawn(async move {
        loop {
            for _ in 0..20 {
                flood
                    .send_to(address, &announce_query(None, 0))
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    let start = tokio::time::Instant::now();
    let mut rounds = 0u64;
    let mut memory_floor = None;
    let mut peak_rss = 0;
    let mut servers = Vec::new();
    let mut completed = false;
    let work = async {
        let mut previous = InfoHashV1([0; 20]);
        while start.elapsed() < Duration::from_secs(1800) {
            // 新鲜合法任务、实际失败 peer、重复宣布和无效 token 并存。
            sender
                .send_to(address, &announce_query(None, 0))
                .await
                .unwrap();
            let token = tokio::time::timeout(Duration::from_secs(2), sender.recv())
                .await
                .unwrap()
                .unwrap()
                .message
                .r
                .unwrap()
                .token
                .unwrap();
            let mut target = InfoHashV1([0; 20]);
            target.0[..8].copy_from_slice(&rounds.to_be_bytes());
            let mut port = 1;
            if rounds.is_multiple_of(10) {
                let name = format!("soak-{rounds}");
                let info = format!("d4:name{}:{}6:pieces0:e", name.len(), name).into_bytes();
                target = InfoHashV1(Sha1::digest(&info).into());
                let (peer, task) = tcp_info(AddressFamily::Ipv4, info).await;
                port = peer.port();
                servers.push(task);
            }
            if rounds % 5 == 1 {
                target = previous;
            } else {
                previous = target;
            }
            let mut query = announce_query(Some(token), port);
            query.a.as_mut().unwrap().info_hash = Some(target);
            // 宣布地址由真实 UDP 来源决定，成功服务绑定同一 loopback 地址。
            if rounds % 3 == 2 {
                query.a.as_mut().unwrap().token = Some(Token(ByteBuf::from(b"invalid".to_vec())));
            }
            sender.send_to(address, &query).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2), sender.recv())
                .await
                .unwrap()
                .unwrap();
            if rounds.is_multiple_of(10) {
                // 饱和时可以拒绝新任务，不为被拒绝的测试 peer 留监听器。
                while servers.last().is_some_and(|task| task.is_finished()) {
                    servers.pop().unwrap().await.unwrap();
                }
                if servers.len() > 2 {
                    let task = servers.remove(0);
                    task.abort();
                    let _ = task.await;
                }
            }
            if rounds.is_multiple_of(20) {
                let stats = store.fetch_stats().await.unwrap();
                assert!(stats.active() <= 16 && stats.running <= 2);
                assert!(
                    tokio::time::timeout(Duration::from_secs(2), handle.status())
                        .await
                        .unwrap()
                        .unwrap()
                        .pending
                        <= 32
                );
                #[cfg(target_os = "linux")]
                {
                    let status = std::fs::read_to_string("/proc/self/status").unwrap();
                    let rss = status
                        .lines()
                        .find(|s| s.starts_with("VmRSS:"))
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .parse::<u64>()
                        .unwrap();
                    peak_rss = peak_rss.max(rss);
                    if start.elapsed() >= Duration::from_secs(60) {
                        let floor = memory_floor.get_or_insert(rss);
                        *floor = (*floor).min(rss);
                        assert!(rss <= *floor + 32 * 1024, "预热后 RSS 增量超过 32 MiB");
                    }
                }
                eprintln!(
                    "local soak rounds={rounds} elapsed={}s stats={stats:?}",
                    start.elapsed().as_secs()
                );
            }
            rounds += 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    let result = std::panic::AssertUnwindSafe(async {
        tokio::select! { biased; _=signal=>{}, _=work=>completed=true }
    })
    .catch_unwind()
    .await;
    pressure.abort();
    let _ = pressure.await;
    for task in servers {
        task.abort();
        let _ = task.await;
    }
    let stats = store.fetch_stats().await.unwrap();
    let shutdown = session.shutdown().await;
    report.value["statistics"] = serde_json::json!({"rounds":rounds,"peak_rss_kib":peak_rss,"rss_floor_kib":memory_floor,"storage":stats,"dht":budget.snapshot()});
    report.value["families"] =
        serde_json::json!({"ipv4":"loopback exercised","ipv6":"not exercised by this scenario"});
    report.value["verification"] = serde_json::json!([
        "active and running bounds",
        "healthy control within 2 seconds",
        "rss growth after 60 second warmup",
        "normal shutdown"
    ]);
    report
        .finish(
            completed || result.is_err(),
            result.is_ok() && shutdown.is_ok() && stats.metadata_count > 0,
        )
        .expect("验收报告必须成功保存");
    shutdown.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert!(completed, "本机验收已提前停止，未完成 30 分钟");
    assert!(stats.metadata_count > 0, "本机验收需要成功 metadata");
}
