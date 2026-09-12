//! 固定大小的累计与区间聚合；不保留 hash、IP 或单次任务记录。
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 采集事件计数；枚举序号对应 Snapshot.counts，COUNTERS 只负责按同序输出。
/// 累计的是已观察事件，不可把开始数、结束数或区间数直接当成功率。
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub(crate) enum Counter {
    /// 实际领取到具有有效 peer 提示的任务数，不等于提示连接成功。
    ClaimsFresh,
    /// 实际领取到的其他任务数，不由本轮的领取偏好直接决定。
    ClaimsOther,
    /// 开始执行流式 DHT 查找的次数，双栈合计为一次。
    Lookups,
    /// 查找结束或被丢弃时汇总的实际 RPC 发送数，可能跨统计区间。
    RpcSent,
    /// 查找退出时已发现至少一个 peer 的次数，不保证该 peer 可下载。
    LookupWithPeers,
    /// 通过地址筛选后开始尝试 peer 的次数，不是 TCP 连接成功数。
    Connections,
    /// complete_job 返回 Applied 后确认的结果数，不包含 Stale。
    MetadataCount,
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
/// 耗时观察维度；枚举序号对应 Snapshot.timings，取消也可形成一次样本。
#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub(crate) enum Timing {
    /// 任务到期至实际领取的等待，使用最长至 24 小时的桶。
    ClaimWait,
    /// 一次双栈查找从开始到返回或被丢弃的耗时。
    Lookup,
    /// 同 IP 连接许可的等待；不包含 TCP 建连。
    TcpWait,
    /// worker 单次任务总耗时，覆盖本地等待、查找和下载。
    Task,
    /// 查找开始至首次发现 peer；没有结果时不产生此样本。
    FirstPeer,
    /// 单 peer 的 TCP 建连阶段。
    PeerConnect,
    /// 标准与扩展握手阶段。
    PeerHandshake,
    /// 请求与接收 metadata 分片阶段。
    PeerTransfer,
    /// 校验原始 info 的阶段。
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
// 桶上界单位均为毫秒；最后一个有效桶之后另留溢出桶。
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
/// 固定桶直方图；21 个槽容纳领取桶及溢出桶，网络桶只使用其中前一部分。
/// 同一个实例的 record、quantile、overflow 必须始终选用相同 long 值。
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
    /// 毫秒向下截断后归入第一个不小于样本的桶；long=true 选领取等待桶。
    pub(crate) fn record(&mut self, duration: Duration, long: bool) {
        let bounds = if long { CLAIM_BOUNDS } else { NETWORK_BOUNDS };
        let ms = duration.as_millis();
        let index = bounds
            .iter()
            .position(|bound| ms <= u128::from(*bound))
            .unwrap_or(bounds.len());
        self.buckets[index] = self.buckets[index].saturating_add(1);
    }
    /// percent 由调用者限制在 1..=100，采用向上取整的样本位次返回近似上界。
    /// 空样本两个字段均为 None；超过最大桶只设置 exceeds_ms，不返回伪造的精确值。
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
/// 数组索引分别对应 Counter、Timing；阶段表按 connect/handshake/transfer/verify 排列。
struct Snapshot {
    counts: [u64; 16],
    timings: [Histogram; 9],
    /// 每个阶段依次记录成功、失败、取消；阶段切换和析构负责提交。
    phases: [[u64; 3]; 4],
}
/// 可共享的进程内统计，Mutex 保护累计与区间同时更新，不跨 await 持锁。
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    /// 第一个快照累计保留，第二个快照在 log 时取走并清零。
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
/// 生命周期内只保留当前阶段，错误和取消都会记录已花费时间。
pub(crate) struct PhaseReport {
    metrics: Arc<Metrics>,
    /// 有效索引 0..=3，依次为建连、握手、传输、校验；由协议会话推进。
    phase: usize,
    start: tokio::time::Instant,
    /// 0 成功、1 失败、2 取消；初值与每次切换后的默认值都是取消。
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
    /// 切到不同阶段时先把上一阶段记为成功；同阶段调用无副作用。phase 必须为 0..=3。
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
    /// 只设置当前阶段结果，不立即计数；阶段推进或 Drop 才记录，推进会把前一阶段记为成功。
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
