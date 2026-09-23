//! 固定大小日志快照、流量事件与 histogram 输出。

use super::{Budget, Stats, VerificationStats};

/// 只保留统计与占用，不复制远端 IP 明细或限流器。
pub(super) struct TrafficLogSnapshot {
    pub(super) total: Stats,
    pub(super) interval: Stats,
    tracked_ips: usize,
    verification_queued: u64,
    queued_current: [u64; 4],
}

impl Budget {
    /// 锁内只清理和取得固定大小快照；区间取走后不会因过滤或投递丢弃还原。
    pub(super) fn take_log_snapshot(&self) -> TrafficLogSnapshot {
        let mut state = self.0.lock().expect("流量锁");
        state.clean();
        TrafficLogSnapshot {
            total: state.total.clone(),
            interval: std::mem::take(&mut state.interval),
            tracked_ips: state.ips.len(),
            verification_queued: state.verification_queued,
            queued_current: state.queued_current,
        }
    }

    /// 格式化和 subscriber 回调均在流量锁释放后执行，仍占用当前调用线程。
    pub(crate) fn log(&self) {
        log_snapshot(self.take_log_snapshot());
    }
}

fn log_snapshot(snapshot: TrafficLogSnapshot) {
    tracing::info!(
        target: "bt_sniffer::dht::traffic",
        event = "dht_occupancy",
        schema_version = 1u64,
        tracked_ips = snapshot.tracked_ips,
        verification_queued = snapshot.verification_queued,
        "DHT 当前占用"
    );
    for (scope, stats) in [("total", &snapshot.total), ("interval", &snapshot.interval)] {
        tracing::info!(
            target: "bt_sniffer::dht::traffic",
            event = "dht_traffic",
            schema_version = 1u64,
            scope,
            inbound_packets = stats.inbound_packets,
            inbound_bytes = stats.inbound_bytes,
            reply_packets = stats.reply_packets,
            reply_bytes = stats.reply_bytes,
            limited_drops = stats.limited_drops,
            queue_timeouts = stats.queue_timeouts,
            validated_v4 = stats.validated_v4,
            validated_v6 = stats.validated_v6,
            "DHT 流量"
        );
        let verification: &VerificationStats = &stats.verification;
        tracing::info!(
            target: "bt_sniffer::dht::traffic",
            event = "verification_admission",
            schema_version = 1u64,
            scope,
            admitted = verification.admitted,
            duplicate = verification.duplicate,
            cooldown = verification.cooldown,
            capacity = verification.capacity,
            ip_table = verification.ip_table,
            succeeded = verification.succeeded,
            failed = verification.failed,
            "反向验证接纳"
        );
        for (index, class) in ["collector", "control", "sampling", "verification"]
            .iter()
            .enumerate()
        {
            tracing::info!(
                target: "bt_sniffer::dht::traffic",
                event = "dht_class",
                schema_version = 1u64,
                scope,
                class,
                packets = stats.packets[index],
                bytes = stats.bytes[index],
                queued = stats.queued[index],
                dequeued_to_send = stats.queue_finished[index][0],
                queued_cancelled = stats.queue_finished[index][1],
                queue_timeouts = stats.queue_finished[index][2],
                local_rejected = stats.queue_finished[index][3],
                inflight_cancelled = stats.inflight_cancelled[index],
                "DHT 类别计数"
            );
            for (reason, count) in [
                "class_quota",
                "destination_ip_quota",
                "upload_bytes",
                "ip_table_capacity",
            ]
            .iter()
            .zip(stats.blocked[index])
            {
                tracing::info!(
                    target: "bt_sniffer::dht::traffic",
                    event = "dht_wait_reason",
                    schema_version = 1u64,
                    scope,
                    class,
                    reason,
                    count,
                    "DHT 待发限制"
                );
            }
            log_histogram(&stats.queue_wait[index], scope, "dht_queue", class);
        }
    }
    for (class, current) in ["collector", "control", "sampling", "verification"]
        .iter()
        .zip(snapshot.queued_current)
    {
        tracing::info!(
            target: "bt_sniffer::dht::traffic",
            event = "dht_queue_occupancy",
            schema_version = 1u64,
            class,
            current,
            "DHT 待发占用"
        );
    }
}

/// 本切片拥有日志契约；固定桶算法只返回数值，不引用任何业务模块。
fn log_histogram(histogram: &crate::histogram::Histogram, scope: &str, timing: &str, class: &str) {
    let p50 = histogram.quantile(50);
    let p95 = histogram.quantile(95);
    let p99 = histogram.quantile(99);
    tracing::info!(
        target: "bt_sniffer::dht::traffic",
        event = "duration_histogram",
        schema_version = 1u64,
        scope,
        timing,
        class,
        count = histogram.count(),
        overflow = histogram.overflow(),
        p50_upper_bound_ms = p50.upper_bound_ms,
        p50_exceeds_ms = p50.exceeds_ms,
        p95_upper_bound_ms = p95.upper_bound_ms,
        p95_exceeds_ms = p95.exceeds_ms,
        p99_upper_bound_ms = p99.upper_bound_ms,
        p99_exceeds_ms = p99.exceeds_ms,
        "固定桶耗时"
    );
}
