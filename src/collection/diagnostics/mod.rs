//! 固定维度的诊断聚合，统一管理执行 guard、样本预算和短期端点历史。
//! 聚合不使用地址或 hash 标签；端点地址仅保留在有界内存表，不随快照输出。
mod attempts;
mod compatibility;
mod connections;
pub(crate) mod metrics;
mod observations;
mod samples;
#[cfg(test)]
use crate::collection::failure::AttemptFailure;
pub(crate) use attempts::{AttemptContext, AttemptResult};
pub(crate) use observations::{PeerObservation, TaskReport};
#[cfg(test)]
use std::sync::Arc;
use std::{collections::BTreeMap, sync::Mutex, time::Duration};

/// 候选首次实际尝试时的来源，不按返回响应重新归类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) enum Source {
    Announce,
    #[default]
    Dht,
}
pub(crate) use crate::collection::peer::{Deadline, Stage};
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ResultKind {
    Success,
    Timeout,
    Eof,
    Reset,
    Io,
    Unsupported,
    Protocol,
    HashMismatch,
    Limit,
    Rejected,
    Cancelled,
}
/// 失败细分只来自类型，不保存报文、地址或系统 errno。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FailureReason {
    Protocol(crate::collection::peer::wire::WireErrorKind),
    Resource(crate::collection::peer::ResourceLimit),
    ConnectionRefused,
    NetworkUnreachable,
    HostUnreachable,
    PermissionDenied,
    OtherIo,
    Eof,
    Reset,
    Timeout,
    Unsupported,
    Rejected,
    HashMismatch,
}
fn failure_reason(error: &crate::collection::peer::PeerError) -> FailureReason {
    use crate::collection::peer::PeerError;
    use std::io::ErrorKind;
    match error {
        PeerError::Protocol(reason) => FailureReason::Protocol(reason.kind),
        PeerError::Limit(reason) => FailureReason::Resource(*reason),
        PeerError::Io(error) => match error.kind() {
            ErrorKind::ConnectionRefused => FailureReason::ConnectionRefused,
            ErrorKind::NetworkUnreachable => FailureReason::NetworkUnreachable,
            ErrorKind::HostUnreachable => FailureReason::HostUnreachable,
            ErrorKind::PermissionDenied => FailureReason::PermissionDenied,
            ErrorKind::UnexpectedEof => FailureReason::Eof,
            ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe => {
                FailureReason::Reset
            }
            ErrorKind::TimedOut => FailureReason::Timeout,
            _ => FailureReason::OtherIo,
        },
        PeerError::Disconnected => FailureReason::Eof,
        PeerError::Timeout(_) => FailureReason::Timeout,
        PeerError::Unsupported => FailureReason::Unsupported,
        PeerError::Rejected(_) => FailureReason::Rejected,
        PeerError::HashMismatch | PeerError::HandshakeHashMismatch => FailureReason::HashMismatch,
    }
}

use crate::histogram::Distribution;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PeerKey {
    stage: Stage,
    source: Source,
    ipv6: bool,
    result: ResultKind,
    deadline: Deadline,
}
#[derive(Debug, Clone, Default)]
struct Snapshot {
    compatibility: compatibility::CompatibilityCounts,
    connections: connections::ConnectionStats,
    attempts: attempts::AttemptStats,
    samples: samples::SampleCounts,
    peers: BTreeMap<PeerKey, Distribution>,
    handshakes: BTreeMap<(Source, bool, ResultKind, Deadline), Distribution>,
    failures: BTreeMap<(Stage, Source, bool, FailureReason), u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TaskTiming {
    DueWait,
    DiscoveryAge,
    Execution,
}
#[derive(Debug, Default)]
pub(crate) struct Diagnostics {
    endpoints: Mutex<connections::EndpointHistory>,
    sample_window: Mutex<samples::SampleWindow>,
    snapshots: Mutex<(Snapshot, Snapshot)>,
}
impl Diagnostics {
    /// 基准报告只读取固定聚合，不重置日志区间。
    #[cfg(test)]
    pub(crate) fn report(&self) -> serde_json::Value {
        let pair = self.snapshots.lock().expect("诊断聚合锁");
        let views = pair.0.attempts.project();
        let mut peers = Vec::new();
        for (key, distribution) in &pair.0.peers {
            peers.push(serde_json::json!({
                "stage": format!("{:?}", key.stage),
                "source": format!("{:?}", key.source),
                "ipv6": key.ipv6,
                "result": format!("{:?}", key.result),
                "deadline": format!("{:?}", key.deadline),
                "count": distribution.count,
                "sum_ms": distribution.sum_ms,
                "p95_upper_bound_ms": distribution.quantile(95),
                "p95_exceeds_ms": distribution.exceeds(95),
            }));
        }
        let mut failures = Vec::new();
        for ((stage, source, ipv6, reason), count) in &pair.0.failures {
            failures.push(serde_json::json!({
                "stage": format!("{stage:?}"),
                "source": format!("{source:?}"),
                "ipv6": ipv6,
                "reason": format!("{reason:?}"),
                "count": count,
            }));
        }
        let commits: Vec<_> = views
            .committed
            .iter()
            .map(|(context, count)| {
                serde_json::json!({
                    "attempt_kind": format!("{:?}", context.kind),
                    "failed_attempts_before": context.failed_attempts_before,
                    "committed": count,
                })
            })
            .collect();
        let class_commits: Vec<_> = views
            .class_committed
            .iter()
            .map(|((context, class), count)| {
                serde_json::json!({
                    "attempt_kind": format!("{:?}", context.kind),
                    "claim_class": format!("{class:?}"),
                    "failed_attempts_before": context.failed_attempts_before,
                    "committed": count,
                })
            })
            .collect();
        let hints: Vec<_> = views
            .hints
            .iter()
            .map(|((kind, hint), counts)| {
                serde_json::json!({
                    "attempt_kind": format!("{kind:?}"),
                    "had_valid_hint": hint,
                    "claims": counts.claims,
                    "executions": counts.executions,
                    "execution_sum_ms": counts.execution_sum_ms,
                    "downloaded": counts.downloaded,
                    "committed": counts.committed,
                })
            })
            .collect();
        serde_json::json!({
            "extension_compatibility": pair.0.compatibility,
            "attempt_hints": hints,
            "peers": peers,
            "failures": failures,
            "attempt_commits": commits,
            "attempt_class_commits": class_commits,
        })
    }
    fn record(&self, key: PeerKey, duration: Duration) {
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            snapshot.peers.entry(key).or_default().record(duration);
        }
    }
    fn failure(&self, key: PeerKey, reason: FailureReason) {
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            *snapshot
                .failures
                .entry((key.stage, key.source, key.ipv6, reason))
                .or_default() += 1;
        }
    }
    #[cfg(test)]
    pub(crate) fn log(&self) {
        self.log_snapshot(false);
    }
    pub(crate) fn log_snapshot(&self, final_snapshot: bool) {
        let entries = self.endpoint_count();
        let (total, interval) = {
            let mut p = self.snapshots.lock().expect("诊断聚合锁");
            (p.0.clone(), std::mem::take(&mut p.1))
        };
        for (scope, snapshot) in [("total", total), ("interval", interval)] {
            snapshot.log_attempts(scope, final_snapshot);
            snapshot.log_compatibility(scope, final_snapshot);
            snapshot.log_connections(scope, final_snapshot, entries);
            tracing::info!(
                event = "bencode_sample_summary",
                schema_version = 1u64,
                scope,
                final_snapshot,
                emitted = snapshot.samples.emitted,
                suppressed = snapshot.samples.suppressed,
                "底层说明采样预算；窗口与日志区间独立，日志输出不重置额度"
            );
            for ((stage, source, ipv6, reason), count) in snapshot.failures {
                tracing::info!(
                    event = "peer_failure_detail",
                    schema_version = 2u64,
                    scope,
                    ?stage,
                    ?source,
                    family = if ipv6 { "ipv6" } else { "ipv4" },
                    ?reason,
                    count,
                    "peer 失败细分"
                );
            }
            for ((source, ipv6, result, deadline), d) in snapshot.handshakes {
                tracing::info!(
                    event = "peer_handshake_diagnostic",
                    schema_version = 1u64,
                    scope,
                    final_snapshot,
                    ?source,
                    family = if ipv6 { "ipv6" } else { "ipv4" },
                    ?result,
                    ?deadline,
                    count = d.count,
                    sum_ms = d.sum_ms,
                    p50_upper_bound_ms = d.quantile(50),
                    p95_upper_bound_ms = d.quantile(95),
                    p99_upper_bound_ms = d.quantile(99),
                    p50_exceeds_ms = d.exceeds(50),
                    p95_exceeds_ms = d.exceeds(95),
                    p99_exceeds_ms = d.exceeds(99),
                    overflow = d.overflow(),
                    "标准握手至扩展协商结束的完整耗时"
                );
            }
            for (key, d) in snapshot.peers {
                tracing::info!(event = "peer_diagnostic",
                    schema_version = 2u64,
                    scope,
                    stage = ?key.stage,
                    source = ?key.source,
                    family = if key.ipv6 { "ipv6" } else { "ipv4" },
                    result = ?key.result,
                    deadline = ?key.deadline,
                    count = d.count,
                    sum_ms = d.sum_ms,
                    p50_upper_bound_ms = d.quantile(50),
                    p95_upper_bound_ms = d.quantile(95),
                    p99_upper_bound_ms = d.quantile(99),
                    p50_exceeds_ms = d.exceeds(50),
                    p95_exceeds_ms = d.exceeds(95),
                    p99_exceeds_ms = d.exceeds(99),
                    overflow = d.overflow(),
                    "peer 分类诊断"
                );
            }
        }
    }
}
pub(crate) fn error_kind(error: &crate::collection::peer::PeerError) -> ResultKind {
    use crate::collection::peer::PeerError;
    match error {
        PeerError::Io(e) => match e.kind() {
            std::io::ErrorKind::UnexpectedEof => ResultKind::Eof,
            std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe => ResultKind::Reset,
            _ => ResultKind::Io,
        },
        PeerError::Timeout(_) => ResultKind::Timeout,
        PeerError::Disconnected => ResultKind::Eof,
        PeerError::Protocol(_) => ResultKind::Protocol,
        PeerError::Unsupported => ResultKind::Unsupported,
        PeerError::Limit(_) => ResultKind::Limit,
        PeerError::Rejected(_) => ResultKind::Rejected,
        PeerError::HashMismatch | PeerError::HandshakeHashMismatch => ResultKind::HashMismatch,
    }
}

#[cfg(test)]
mod tests;
