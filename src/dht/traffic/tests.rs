use super::*;
#[tokio::test(start_paused = true)]
async fn shared_burst_classes_and_ip_limits_do_not_consume_unrelated_budget() {
    let budget = Budget::default();
    let ip = "127.0.0.1".parse().unwrap();
    assert!(budget.query(Class::Control, ip, 100).is_zero());
    assert!(budget.query(Class::Control, ip, 100).is_zero());
    for _ in 0..20 {
        assert!(!budget.query(Class::Control, ip, 100).is_zero());
    }
    for n in 2..=5 {
        assert!(
            budget
                .query(Class::Control, format!("127.0.0.{n}").parse().unwrap(), 100)
                .is_zero()
        );
    }
    assert!(
        !budget
            .query(Class::Control, "::1".parse().unwrap(), 100)
            .is_zero()
    );
    assert!(
        budget
            .query(Class::Collector, "::1".parse().unwrap(), 100)
            .is_zero()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(budget.query(Class::Control, ip, 100).is_zero());
}
#[tokio::test(start_paused = true)]
async fn byte_pools_response_reserve_and_tracking_capacity_are_isolated() {
    let budget = Budget::default();
    assert!(budget.reply(229376));
    assert!(!budget.reply(1));
    let ip = "127.0.0.1".parse().unwrap();
    assert!(budget.query(Class::Collector, ip, 100).is_zero());
    for _ in 0..5 {
        assert!(budget.inbound(ip, 10, false));
    }
    assert!(!budget.inbound(ip, 10, false));
    for _ in 0..128 {
        assert!(budget.inbound(ip, 10, true));
    }
    assert!(!budget.inbound(ip, 10, true));
    for n in 0..10000 {
        budget
            .0
            .lock()
            .unwrap()
            .track(IpAddr::V6(std::net::Ipv6Addr::from(n + 1)));
    }
    assert_eq!(budget.0.lock().unwrap().ips.len(), 10000);
    assert!(!budget.0.lock().unwrap().track("8.8.8.8".parse().unwrap()));
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(budget.0.lock().unwrap().track("8.8.8.8".parse().unwrap()));
    assert_eq!(budget.0.lock().unwrap().ips.len(), 1);
}

/// 多 IP 与双栈共用一份预算，任意前缀发送量不超过 burst + rate * t。
#[tokio::test(start_paused = true)]
async fn all_class_prefixes_obey_shared_rate_envelope() {
    let budget = Budget::default();
    let classes = [
        Class::Collector,
        Class::Control,
        Class::Sampling,
        Class::Verification,
    ];
    let rates = Config::default().classes();
    let mut sent = [0u64; 4];
    for tick in 0..=100 {
        for n in 1..=40u8 {
            let ip = if n % 2 == 0 {
                IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, n))
            } else {
                IpAddr::V6(std::net::Ipv6Addr::from(n as u128))
            };
            for class in classes {
                if budget.query(class, ip, 100).is_zero() {
                    sent[class as usize] += 1;
                }
            }
        }
        for i in 0..4 {
            assert!(
                sent[i] * 1000 <= rates[i] as u64 * (1000 + tick * 10),
                "{i}: {}",
                sent[i]
            );
        }
        assert!(sent.iter().sum::<u64>() * 1000 <= 20 * (1000 + tick * 10));
        tokio::time::advance(Duration::from_millis(10)).await;
    }
}

#[tokio::test(start_paused = true)]
// 同一意图重复观察两个等待原因，确认按原因去重，并在 Drop 后归还当前占用。
async fn queue_statistics_count_intents_not_budget_polls() {
    let budget = Arc::new(Budget::default());
    let mut queued = budget.queue_record(Class::Collector);
    for _ in 0..100 {
        queued.blocked(3);
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    queued.finish(2);
    drop(queued);
    let stats = budget.snapshot();
    assert_eq!(stats.queued[0], 1);
    assert_eq!(stats.blocked[0], [1, 1, 0, 0]);
    assert_eq!(stats.queue_finished[0], [0, 0, 1, 0]);
    assert_eq!(stats.queue_timeouts, 1);
    assert_eq!(stats.queue_wait[0].count(), 1);
    assert_eq!(budget.0.lock().unwrap().queued_current, [0; 4]);
    drop(budget.queue_record(Class::Control));
    assert_eq!(budget.snapshot().queue_finished[1][1], 1);
}

#[tokio::test(start_paused = true)]
async fn verification_cooldown_and_waiting_slots_are_shared_and_bounded() {
    let budget = Arc::new(Budget::default());
    let mut permits = Vec::new();
    for n in 1..=10u128 {
        let ip = if n % 2 == 0 {
            IpAddr::V6(std::net::Ipv6Addr::from(n))
        } else {
            IpAddr::V4(std::net::Ipv4Addr::from(n as u32))
        };
        permits.push(budget.admit_verification(ip).unwrap());
    }
    let extra = "1.2.3.4".parse().unwrap();
    assert!(budget.admit_verification(extra).is_none());
    assert_eq!(budget.snapshot().verification.capacity, 1);
    drop(permits);
    let ip = "::2".parse().unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(budget.admit_verification(ip).is_none());
    tokio::time::advance(Duration::from_secs(30)).await;
    // 重复事件没有把 60 秒截止时间延后。
    drop(budget.admit_verification(ip).unwrap());
    assert_eq!(budget.0.lock().unwrap().verification_queued, 0);
    assert_eq!(budget.snapshot().verification.failed, 0);
    // 表满不能靠淘汰尚在冷却的键来接纳新 IP。
    for n in 100..10100u128 {
        let _ = budget.admit_verification(IpAddr::V6(std::net::Ipv6Addr::from(n)));
    }
    assert_eq!(budget.0.lock().unwrap().ips.len(), 10000);
    assert!(budget.admit_verification(extra).is_none());
    assert!(budget.snapshot().verification.ip_table > 0);
}

/// 回调非阻塞探测锁，并在首条输出期间产生新计数；下一快照必须包含该计数。
#[tokio::test]
async fn log_releases_budget_lock_and_consumes_interval_once() {
    // tracing 的 callsite 缓存跨测试共享；独立进程避免探针影响其他 subscriber。
    const CHILD: &str = "BT_SNIFFER_TRAFFIC_LOCK_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "dht::traffic::tests::log_releases_budget_lock_and_consumes_interval_once",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing_subscriber::{Layer, prelude::*};
    struct Probe {
        budget: Arc<Budget>,
        updated: Arc<AtomicBool>,
    }
    impl<S: tracing::Subscriber> Layer<S> for Probe {
        fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            assert!(
                self.budget.0.try_lock().is_ok(),
                "subscriber 不得在流量锁内调用"
            );
            if !self.updated.swap(true, Ordering::SeqCst) {
                self.budget.update(|stats| stats.inbound_packets += 7);
            }
        }
    }
    let budget = Arc::new(Budget::default());
    budget.update(|stats| stats.inbound_packets += 3);
    let updated = Arc::new(AtomicBool::new(false));
    let subscriber = tracing_subscriber::registry().with(Probe {
        budget: budget.clone(),
        updated: updated.clone(),
    });
    tracing::subscriber::with_default(subscriber, || budget.log());
    assert!(updated.load(Ordering::SeqCst));
    let second = budget.take_log_snapshot();
    assert_eq!(second.total.inbound_packets, 10);
    assert_eq!(second.interval.inbound_packets, 7);
    let third = budget.take_log_snapshot();
    assert_eq!(third.total.inbound_packets, 10);
    assert_eq!(third.interval.inbound_packets, 0);
}
