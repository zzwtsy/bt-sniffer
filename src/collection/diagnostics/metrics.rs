//! 固定大小的累计与区间聚合；不保留 hash、IP 或单次任务记录。
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 采集事件计数；枚举序号对应 Snapshot.counts，COUNTERS 只负责按同序输出。
/// 累计的是已观察事件，不可把开始数、结束数或区间数直接当成功率。
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub(crate) enum Counter {
    /// 实际领取到具有有效 peer 提示的任务数，不等于提示连接成功。
    ClaimsWithHint,
    /// 领取时没有有效 peer 提示的任务数，包含 First 和 Repeat。
    ClaimsWithoutHint,
    /// 开始执行流式 DHT 查找的次数，双栈合计为一次。
    Lookups,
    /// 查找结束或被丢弃时汇总的实际 RPC 发送数，可能跨统计区间。
    RpcSent,
    /// 查找退出时已发现至少一个 peer 的次数，不保证该 peer 可下载。
    LookupWithPeers,
    /// 通过地址筛选后开始尝试 peer 的次数，不是 TCP 连接成功数。
    PeerAttempts,
    /// complete_job 返回 Applied 后确认的结果数，不包含 Stale。
    MetadataCommitted,
    /// 上述成功提交的原始 info 字节数，不是网络总接收字节。
    MetadataBytes,
    /// 失败重试已提交的任务轮数，不是失败 peer 数。
    RemoteFailures,
    /// 本地延期已提交的任务轮数；取消、无路由和本地等待不计远端失败。
    LocalDeferrals,
    /// 查找 future 退出的次数，包含超时和被丢弃，不表示成功。
    LookupsFinished,
    /// 一次双栈查找首次发现 peer 的次数，每次查找最多一次。
    LookupFirstPeer,
    /// metadata 成功后主动结束仍未完成查找的次数。
    LookupCancelledSuccess,
    /// 单 peer 尝试返回错误的次数；外层取消而直接丢弃 future 不在此加一。
    PeerFailures,
    /// 主动采样从总体暂停状态恢复的次数，不是单个暂停原因消失的次数。
    SamplingResumes,
    /// discover_peer 确认接纳的宣布数，包含对已有记录的刷新。
    AnnouncesAccepted,
}
const COUNTERS: [Counter; 16] = [
    Counter::ClaimsWithHint,
    Counter::ClaimsWithoutHint,
    Counter::Lookups,
    Counter::RpcSent,
    Counter::LookupWithPeers,
    Counter::PeerAttempts,
    Counter::MetadataCommitted,
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
/// 耗时观察维度；枚举序号对应 Snapshot.timings，取消也可形成一次样本。
#[derive(Debug, Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub(crate) enum Timing {
    /// 一次双栈查找从开始到返回或被丢弃的耗时。
    Lookup,
    /// 同 IP 连接许可的等待；不包含 TCP 建连。
    TcpWait,
    /// 查找开始至首次发现 peer；没有结果时不产生此样本。
    FirstPeer,
}
const TIMINGS: [Timing; 3] = [Timing::Lookup, Timing::TcpWait, Timing::FirstPeer];
use crate::histogram::Histogram;
#[cfg(test)]
use crate::histogram::Quantile;
#[derive(Debug, Clone)]
/// 数组索引分别对应 Counter、Timing；peer 阶段由 diagnostics 统一记录。
struct Snapshot {
    counts: [u64; 16],
    timings: [Histogram; 3],
}
impl Default for Snapshot {
    fn default() -> Self {
        Self {
            counts: [0; 16],
            timings: std::array::from_fn(|_| Histogram::default()),
        }
    }
}
/// 可共享的进程内统计，Mutex 保护累计与区间同时更新，不跨 await 持锁。
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    pub(crate) diagnostics: crate::collection::diagnostics::Diagnostics,
    /// 第一个快照累计保留，第二个快照在 log 时取走并清零。
    snapshots: Mutex<(Snapshot, Snapshot)>,
}
impl Metrics {
    /// 固定累计快照；不消费日志区间，分位保持桶边界语义。
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        let snapshot = self.snapshots.lock().expect("指标锁").0.clone();
        serde_json::json!({"diagnostics":self.diagnostics.snapshot(),"counters":COUNTERS.iter().zip(snapshot.counts).map(|(counter,value)|serde_json::json!({"counter":counter,"value":value})).collect::<Vec<_>>(),"durations":TIMINGS.iter().zip(snapshot.timings).map(|(timing,h)|serde_json::json!({"timing":timing,"count":h.count(),"overflow":h.overflow(),"p50":h.quantile(50),"p95":h.quantile(95),"p99":h.quantile(99)})).collect::<Vec<_>>()})
    }

    #[cfg(test)]
    pub(crate) fn report(&self) -> serde_json::Value {
        let pair = self.snapshots.lock().expect("指标锁");
        let total = &pair.0;
        serde_json::json!({
            "diagnostics": self.diagnostics.report(),
            "metadata": total.counts[Counter::MetadataCommitted as usize],
            "claims": total.counts[Counter::ClaimsWithHint as usize]
                + total.counts[Counter::ClaimsWithoutHint as usize],
            "peer_attempts": total.counts[Counter::PeerAttempts as usize],
        })
    }
    /// 不重置区间，供同一次日志输出的摘要读取；log 仍是唯一清零入口。
    pub(crate) fn interval_counts(&self) -> [u64; 3] {
        let pair = self.snapshots.lock().expect("指标锁");
        let c = &pair.1.counts;
        [
            c[Counter::MetadataCommitted as usize],
            c[Counter::ClaimsWithHint as usize] + c[Counter::ClaimsWithoutHint as usize],
            c[Counter::PeerAttempts as usize],
        ]
    }
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
        snapshots.0.timings[timing as usize].record(duration);
        snapshots.1.timings[timing as usize].record(duration);
    }
    /// 创建由调用者持有的计时 guard；Arc 共享指标所有权，Drop 时记录一次耗时。
    pub(crate) fn timer(self: &Arc<Self>, timing: Timing) -> Timer {
        Timer {
            metrics: self.clone(),
            timing,
            start: tokio::time::Instant::now(),
        }
    }
    /// 锁内克隆累计值、取走区间值，释放锁后输出；即使日志被过滤或丢弃，区间也已重置。
    pub(crate) fn log(&self) {
        self.log_snapshot(false);
    }
    /// 会话回收 worker 后输出关闭尾段；不额外查询数据库。
    pub(crate) fn log_final(&self) {
        self.log_snapshot(true);
    }
    fn log_snapshot(&self, final_snapshot: bool) {
        self.diagnostics.log_snapshot(final_snapshot);
        let (total, interval) = {
            let mut snapshots = self.snapshots.lock().expect("指标锁");
            (snapshots.0.clone(), std::mem::take(&mut snapshots.1))
        };
        for (scope, snapshot) in [("total", total), ("interval", interval)] {
            for (counter, value) in COUNTERS.iter().zip(snapshot.counts) {
                tracing::info!(
                    event = "collector_counter",
                    schema_version = 2u64,
                    scope,
                    ?counter,
                    value,
                    "采集运行计数"
                );
            }
            for (timing, histogram) in TIMINGS.iter().zip(snapshot.timings) {
                log_histogram(&histogram, scope, &format!("{timing:?}"), "collector");
            }
        }
    }
}
/// 作用域计时器；正常返回、提前返回和 future 被丢弃都会记录，不能据此判断任务成功。
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
/// 本切片拥有日志契约；固定桶算法只返回数值，不引用任何业务模块。
fn log_histogram(histogram: &crate::histogram::Histogram, scope: &str, timing: &str, class: &str) {
    let p50 = histogram.quantile(50);
    let p95 = histogram.quantile(95);
    let p99 = histogram.quantile(99);
    tracing::info!(
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_histogram_reports_bucket_upper_bound_and_overflow() {
        let mut h = Histogram::default();
        for ms in 1..=100 {
            h.record(Duration::from_millis(ms));
        }
        assert_eq!(h.quantile(50).upper_bound_ms, Some(50));
        assert_eq!(h.quantile(95).upper_bound_ms, Some(100));
        let mut h = Histogram::new(crate::histogram::Buckets::Diagnostic);
        h.record(Duration::from_secs(1801));
        assert_eq!(h.quantile(95).upper_bound_ms, Some(3600000));
        h.record(Duration::from_secs(86401));
        assert_eq!(
            h.quantile(99),
            Quantile {
                upper_bound_ms: None,
                exceeds_ms: Some(86400000)
            }
        );
        assert_eq!(h.overflow(), 1);
        assert_eq!(Histogram::default().quantile(95).exceeds_ms, None);
    }
    #[tokio::test(start_paused = true)]
    async fn phase_reports_success_failure_and_cancellation_once() {
        let metrics = Arc::new(Metrics::default());
        let mut report = crate::collection::diagnostics::PeerObservation::new(
            metrics.clone(),
            Default::default(),
            false,
            Arc::default(),
            Arc::default(),
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        report.advance(crate::collection::diagnostics::Stage::StandardHandshake);
        report.finish(
            crate::collection::diagnostics::ResultKind::Io,
            crate::collection::diagnostics::Deadline::None,
        );
        drop(report);
        drop(crate::collection::diagnostics::PeerObservation::new(
            metrics.clone(),
            Default::default(),
            false,
            Arc::default(),
            Arc::default(),
        ));
        let data = metrics.diagnostics.snapshots.lock().unwrap();
        use crate::collection::diagnostics::{ResultKind, Stage};
        let count = |stage, result| {
            data.0
                .peers
                .iter()
                .filter(|(key, _)| key.stage == stage && key.result == result)
                .map(|(_, d)| d.count)
                .sum::<u64>()
        };
        assert_eq!(count(Stage::Connect, ResultKind::Success), 1);
        assert_eq!(count(Stage::Connect, ResultKind::Cancelled), 1);
        assert_eq!(count(Stage::StandardHandshake, ResultKind::Io), 1);
        assert_eq!(
            data.0
                .peers
                .iter()
                .filter(|(key, _)| key.stage == Stage::Connect)
                .map(|(_, d)| d.count)
                .sum::<u64>(),
            2
        );
    }
}

#[cfg(test)]
#[test]
fn inspection_does_not_consume_interval() {
    let metrics = Metrics::default();
    metrics.add(Counter::MetadataCommitted, 3);
    let before = metrics.interval_counts();
    assert_eq!(metrics.snapshot(), metrics.snapshot());
    assert_eq!(metrics.interval_counts(), before);
}
