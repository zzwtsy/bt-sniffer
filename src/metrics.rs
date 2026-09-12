//! 固定大小的累计与区间聚合；不保留 hash、IP 或单次任务记录。
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub(crate) enum Counter {
    ClaimsFresh,
    ClaimsOther,
    Lookups,
    RpcSent,
    LookupWithPeers,
    Connections,
    MetadataCount,
    MetadataBytes,
    RemoteFailures,
    LocalDeferrals,
    LookupsFinished,
    LookupFirstPeer,
    LookupCancelledSuccess,
    PeerFailures,
    SamplingResumes,
    AnnouncesAccepted,
}
const COUNTERS: [Counter; 16] = [
    Counter::ClaimsFresh,
    Counter::ClaimsOther,
    Counter::Lookups,
    Counter::RpcSent,
    Counter::LookupWithPeers,
    Counter::Connections,
    Counter::MetadataCount,
    Counter::MetadataBytes,
    Counter::RemoteFailures,
    Counter::LocalDeferrals,
    Counter::LookupsFinished,
    Counter::LookupFirstPeer,
    Counter::LookupCancelledSuccess,
    Counter::PeerFailures,
    Counter::SamplingResumes,
    Counter::AnnouncesAccepted,
];
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub(crate) enum Timing {
    ClaimWait,
    Lookup,
    TcpWait,
    Task,
    FirstPeer,
    PeerConnect,
    PeerHandshake,
    PeerTransfer,
    PeerVerify,
}
const TIMINGS: [Timing; 9] = [
    Timing::ClaimWait,
    Timing::Lookup,
    Timing::TcpWait,
    Timing::Task,
    Timing::FirstPeer,
    Timing::PeerConnect,
    Timing::PeerHandshake,
    Timing::PeerTransfer,
    Timing::PeerVerify,
];
const NETWORK_BOUNDS: &[u64] = &[
    1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000, 180000,
];
const CLAIM_BOUNDS: &[u64] = &[
    1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000, 180000, 300000, 600000, 1800000,
    3600000, 7200000, 21600000, 86400000,
];

/// 分位数是桶上界；溢出用 exceeds_ms 表达，绝不把哨兵当成实际毫秒数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Quantile {
    pub(crate) upper_bound_ms: Option<u64>,
    pub(crate) exceeds_ms: Option<u64>,
}
#[derive(Debug, Clone, Default, serde::Serialize)]
pub(crate) struct Histogram {
    buckets: [u64; 21],
}
impl Histogram {
    pub(crate) fn log(&self, scope: &str, timing: &str, class: &str, long: bool) {
        let p50 = self.quantile(50, long);
        let p95 = self.quantile(95, long);
        let p99 = self.quantile(99, long);
        tracing::info!(
            event = "duration_histogram",
            schema_version = 1u64,
            scope,
            timing,
            class,
            count = self.count(),
            overflow = self.overflow(long),
            p50_upper_bound_ms = p50.upper_bound_ms,
            p50_exceeds_ms = p50.exceeds_ms,
            p95_upper_bound_ms = p95.upper_bound_ms,
            p95_exceeds_ms = p95.exceeds_ms,
            p99_upper_bound_ms = p99.upper_bound_ms,
            p99_exceeds_ms = p99.exceeds_ms,
            "固定桶耗时"
        );
    }
    pub(crate) fn record(&mut self, duration: Duration, long: bool) {
        let bounds = if long { CLAIM_BOUNDS } else { NETWORK_BOUNDS };
        let ms = duration.as_millis();
        let index = bounds
            .iter()
            .position(|bound| ms <= u128::from(*bound))
            .unwrap_or(bounds.len());
        self.buckets[index] = self.buckets[index].saturating_add(1);
    }
    pub(crate) fn quantile(&self, percent: u64, long: bool) -> Quantile {
        let bounds = if long { CLAIM_BOUNDS } else { NETWORK_BOUNDS };
        let count = self.count();
        if count == 0 {
            return Quantile {
                upper_bound_ms: None,
                exceeds_ms: None,
            };
        }
        let rank = (u128::from(count) * u128::from(percent)).div_ceil(100);
        let mut seen = 0u128;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += u128::from(*count);
            if seen >= rank {
                return Quantile {
                    upper_bound_ms: bounds.get(index).copied(),
                    exceeds_ms: (index >= bounds.len()).then(|| *bounds.last().unwrap()),
                };
            }
        }
        unreachable!("非空直方图的分位落在某个桶中")
    }
    pub(crate) fn count(&self) -> u64 {
        self.buckets.iter().fold(0u64, |a, b| a.saturating_add(*b))
    }
    pub(crate) fn overflow(&self, long: bool) -> u64 {
        self.buckets[if long {
            CLAIM_BOUNDS.len()
        } else {
            NETWORK_BOUNDS.len()
        }]
    }
}
#[derive(Debug, Clone, Default)]
struct Snapshot {
    counts: [u64; 16],
    timings: [Histogram; 9],
    phases: [[u64; 3]; 4],
}
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    snapshots: Mutex<(Snapshot, Snapshot)>,
}
impl Metrics {
    pub(crate) fn add(&self, counter: Counter, value: u64) {
        let mut snapshots = self.snapshots.lock().expect("指标锁");
        let (total, interval) = &mut *snapshots;
        for snapshot in [total, interval] {
            snapshot.counts[counter as usize] =
                snapshot.counts[counter as usize].saturating_add(value);
        }
    }
    pub(crate) fn observe(&self, timing: Timing, duration: Duration) {
        let mut snapshots = self.snapshots.lock().expect("指标锁");
        snapshots.0.timings[timing as usize].record(duration, matches!(timing, Timing::ClaimWait));
        snapshots.1.timings[timing as usize].record(duration, matches!(timing, Timing::ClaimWait));
    }
    pub(crate) fn timer(self: &Arc<Self>, timing: Timing) -> Timer {
        Timer {
            metrics: self.clone(),
            timing,
            start: tokio::time::Instant::now(),
        }
    }
    pub(crate) fn log(&self) {
        let (total, interval) = {
            let mut snapshots = self.snapshots.lock().expect("指标锁");
            (snapshots.0.clone(), std::mem::take(&mut snapshots.1))
        };
        for (scope, snapshot) in [("total", total), ("interval", interval)] {
            for (counter, value) in COUNTERS.iter().zip(snapshot.counts) {
                tracing::info!(
                    event = "collector_counter",
                    schema_version = 1u64,
                    scope,
                    ?counter,
                    value,
                    "采集运行计数"
                );
            }
            for (phase, counts) in ["connect", "handshake", "transfer", "verify"]
                .iter()
                .zip(snapshot.phases)
            {
                tracing::info!(
                    event = "peer_phase",
                    schema_version = 1u64,
                    scope,
                    phase,
                    succeeded = counts[0],
                    failed = counts[1],
                    cancelled = counts[2],
                    "peer 阶段结果"
                );
            }
            for (timing, histogram) in TIMINGS.iter().zip(snapshot.timings) {
                histogram.log(
                    scope,
                    &format!("{timing:?}"),
                    "collector",
                    matches!(timing, Timing::ClaimWait),
                );
            }
        }
    }
}
pub(crate) struct Timer {
    metrics: Arc<Metrics>,
    timing: Timing,
    start: tokio::time::Instant,
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.metrics.observe(self.timing, self.start.elapsed());
    }
}
/// 生命周期内只保留当前阶段，错误和取消都会记录已花费时间。
pub(crate) struct PhaseReport {
    metrics: Arc<Metrics>,
    phase: usize,
    start: tokio::time::Instant,
    outcome: usize,
}
impl PhaseReport {
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            phase: 0,
            start: tokio::time::Instant::now(),
            outcome: 2,
        }
    }
    fn record(&self) {
        let timing = [
            Timing::PeerConnect,
            Timing::PeerHandshake,
            Timing::PeerTransfer,
            Timing::PeerVerify,
        ][self.phase];
        self.metrics.observe(timing, self.start.elapsed());
        let mut snapshots = self.metrics.snapshots.lock().expect("指标锁");
        snapshots.0.phases[self.phase][self.outcome] += 1;
        snapshots.1.phases[self.phase][self.outcome] += 1;
    }
    pub(crate) fn advance(&mut self, phase: usize) {
        if phase == self.phase {
            return;
        }
        self.outcome = 0;
        self.record();
        self.phase = phase;
        self.outcome = 2;
        self.start = tokio::time::Instant::now();
    }
    pub(crate) fn finish(&mut self, success: bool) {
        self.outcome = if success { 0 } else { 1 };
    }
}
impl Drop for PhaseReport {
    fn drop(&mut self) {
        self.record();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_histogram_reports_bucket_upper_bound_and_overflow() {
        let mut h = Histogram::default();
        for ms in 1..=100 {
            h.record(Duration::from_millis(ms), false);
        }
        assert_eq!(h.quantile(50, false).upper_bound_ms, Some(50));
        assert_eq!(h.quantile(95, false).upper_bound_ms, Some(100));
        let mut h = Histogram::default();
        h.record(Duration::from_secs(1801), true);
        assert_eq!(h.quantile(95, true).upper_bound_ms, Some(3600000));
        h.record(Duration::from_secs(86401), true);
        assert_eq!(
            h.quantile(99, true),
            Quantile {
                upper_bound_ms: None,
                exceeds_ms: Some(86400000)
            }
        );
        assert_eq!(h.overflow(true), 1);
        assert_eq!(Histogram::default().quantile(95, false).exceeds_ms, None);
    }
    #[tokio::test(start_paused = true)]
    async fn phase_reports_success_failure_and_cancellation_once() {
        let metrics = Arc::new(Metrics::default());
        let mut report = PhaseReport::new(metrics.clone());
        tokio::time::advance(Duration::from_secs(1)).await;
        report.advance(1);
        report.finish(false);
        drop(report);
        drop(PhaseReport::new(metrics.clone()));
        let data = metrics.snapshots.lock().unwrap();
        assert_eq!(data.0.phases[0], [1, 0, 1]);
        assert_eq!(data.0.phases[1], [0, 1, 0]);
        assert_eq!(data.0.timings[Timing::PeerConnect as usize].count(), 2);
    }
}
