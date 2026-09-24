//! 仅观察短期端点复用，不参与连接决策；地址只留在有界内存表中。
use super::{
    Deadline, Diagnostics, Distribution, FailureReason, PeerKey, ResultKind, Snapshot, Source,
};
use crate::collection::jobs::AttemptKind;
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    time::Duration,
};
use tokio::time::Instant;

const CAPACITY: usize = 4096;
const TTL: Duration = Duration::from_secs(300);

/// 历史只描述 TCP 建连，Success 不代表 metadata 成功。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum PreviousConnect {
    NoHistory,
    Success,
    Timeout,
    ConnectionRefused,
    Unreachable,
    OtherIo,
}
#[derive(Default)]
pub(super) struct EndpointHistory {
    entries: HashMap<SocketAddr, (PreviousConnect, Instant)>,
}
// 上层 Metrics/PeerClient 可实现 Debug；端点地址不能由这些调试输出间接泄露。
impl std::fmt::Debug for EndpointHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointHistory")
            .field("entries", &self.entries.len())
            .finish()
    }
}
impl EndpointHistory {
    fn expire(&mut self, now: Instant) -> u64 {
        let before = self.entries.len();
        self.entries
            .retain(|_, (_, ended)| now.duration_since(*ended) < TTL);
        (before - self.entries.len()) as u64
    }
}
/// 一次真实连接的历史快照；guard 持有，结束后不会保存到聚合键中。
#[derive(Debug)]
pub(super) struct Observation {
    address: SocketAddr,
    previous: PreviousConnect,
    attempt_kind: Option<AttemptKind>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ConnectionKey {
    attempt_kind: Option<AttemptKind>,
    source: Source,
    ipv6: bool,
    previous: PreviousConnect,
    result: ResultKind,
    reason: Option<FailureReason>,
    deadline: Deadline,
}
#[derive(Debug, Default, Clone)]
pub(super) struct ConnectionStats {
    timings: BTreeMap<ConnectionKey, Distribution>,
    expired: u64,
    capacity_dropped: u64,
}
impl Diagnostics {
    /// 查询不刷新 TTL；先释放地址表锁，再更新聚合，任何锁都不跨 await。
    pub(super) fn observe_connect(
        &self,
        address: SocketAddr,
        attempt_kind: Option<AttemptKind>,
    ) -> Observation {
        let (previous, expired) = {
            let mut history = self.endpoints.lock().expect("端点历史锁");
            let expired = history.expire(Instant::now());
            (
                history
                    .entries
                    .get(&address)
                    .map_or(PreviousConnect::NoHistory, |entry| entry.0),
                expired,
            )
        };
        self.connection_maintenance(expired, 0);
        Observation {
            address,
            previous,
            attempt_kind,
        }
    }
    fn connection_maintenance(&self, expired: u64, dropped: u64) {
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            snapshot.connections.expired += expired;
            snapshot.connections.capacity_dropped += dropped;
        }
    }
    /// 日志快照前清理过期条目；返回 gauge，不把当前条目数当成区间增量。
    pub(super) fn endpoint_count(&self) -> usize {
        let (count, expired) = {
            let mut history = self.endpoints.lock().expect("端点历史锁");
            let expired = history.expire(Instant::now());
            (history.entries.len(), expired)
        };
        self.connection_maintenance(expired, 0);
        count
    }
    /// guard 完成 Connect 后调用一次；先更新短期历史，再记录不含地址的固定聚合。
    pub(super) fn finish_connect(
        &self,
        observation: Observation,
        key: PeerKey,
        reason: Option<FailureReason>,
        elapsed: Duration,
    ) {
        let outcome = match key.result {
            ResultKind::Success => Some(PreviousConnect::Success),
            ResultKind::Timeout if key.deadline == Deadline::Stage => {
                Some(PreviousConnect::Timeout)
            }
            ResultKind::Io | ResultKind::Eof | ResultKind::Reset => Some(match reason {
                Some(FailureReason::ConnectionRefused) => PreviousConnect::ConnectionRefused,
                Some(FailureReason::NetworkUnreachable | FailureReason::HostUnreachable) => {
                    PreviousConnect::Unreachable
                }
                _ => PreviousConnect::OtherIo,
            }),
            _ => None, // 取消及外层期限不将端点误判为失效。
        };
        if let Some(outcome) = outcome {
            let (expired, dropped) = {
                let mut history = self.endpoints.lock().expect("端点历史锁");
                let now = Instant::now();
                let expired = history.expire(now);
                let full = history.entries.len() >= CAPACITY
                    && !history.entries.contains_key(&observation.address);
                if !full {
                    history.entries.insert(observation.address, (outcome, now));
                }
                (expired, u64::from(full))
            };
            self.connection_maintenance(expired, dropped);
        }
        let key = ConnectionKey {
            attempt_kind: observation.attempt_kind,
            source: key.source,
            ipv6: key.ipv6,
            previous: observation.previous,
            result: key.result,
            deadline: key.deadline,
            reason,
        };
        let mut pair = self.snapshots.lock().expect("诊断聚合锁");
        let (total, interval) = &mut *pair;
        for snapshot in [total, interval] {
            snapshot
                .connections
                .timings
                .entry(key)
                .or_default()
                .record(elapsed);
        }
    }
}
impl Snapshot {
    pub(super) fn log_connections(&self, scope: &str, final_snapshot: bool, entries: usize) {
        for (key, distribution) in &self.connections.timings {
            tracing::info!(event = "connect_history_diagnostic",
                schema_version = 1u64,
                scope,
                final_snapshot,
                attempt_kind = match key.attempt_kind { Some(AttemptKind::First) => "First", Some(AttemptKind::Repeat) => "Repeat", None => "Unknown" },
                source = ?key.source,
                family = if key.ipv6 { "ipv6" } else { "ipv4" },
                history = ?key.previous,
                result = ?key.result,
                reason = key.reason.map(|reason| format!("{reason:?}")),
                deadline = ?key.deadline,
                count = distribution.count,
                sum_ms = distribution.sum_ms,
                p50_upper_bound_ms = distribution.quantile(50),
                p95_upper_bound_ms = distribution.quantile(95),
                p99_upper_bound_ms = distribution.quantile(99),
                p50_exceeds_ms = distribution.exceeds(50),
                p95_exceeds_ms = distribution.exceeds(95),
                p99_exceeds_ms = distribution.exceeds(99),
                overflow = distribution.overflow(),
                "短期端点历史只供观察，NoHistory 不代表从未连接"
            );
        }
        tracing::info!(
            event = "connect_history_summary",
            schema_version = 1u64,
            scope,
            final_snapshot,
            entries,
            expired = self.connections.expired,
            capacity_dropped = self.connections.capacity_dropped,
            "端点内存历史覆盖范围；不记录地址，不跳过连接"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::diagnostics::PeerObservation;
    use crate::collection::diagnostics::Stage;
    use crate::collection::diagnostics::metrics::Metrics;
    use std::sync::Arc;

    fn report(metrics: &Arc<Metrics>, address: SocketAddr) -> PeerObservation {
        let mut report = PeerObservation::new(
            metrics.clone(),
            Source::Dht,
            address.is_ipv6(),
            Arc::default(),
            Arc::default(),
        );
        report.observe_connect(address, Some(AttemptKind::Repeat));
        report
    }
    fn finish(metrics: &Arc<Metrics>, address: SocketAddr, result: ResultKind, deadline: Deadline) {
        let mut report = report(metrics, address);
        report.finish(result, deadline);
        report.finish(result, deadline);
    }
    #[tokio::test(start_paused = true)]
    async fn history_expires_without_reads_refreshing_and_success_replaces_failure() {
        let metrics = Arc::new(Metrics::default());
        let address = "127.0.0.1:1234".parse().unwrap();
        finish(&metrics, address, ResultKind::Timeout, Deadline::Stage);
        tokio::time::advance(Duration::from_secs(299)).await;
        assert_eq!(
            metrics.diagnostics.observe_connect(address, None).previous,
            PreviousConnect::Timeout
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            metrics.diagnostics.observe_connect(address, None).previous,
            PreviousConnect::NoHistory
        );
        finish(&metrics, address, ResultKind::Timeout, Deadline::Stage);
        finish(&metrics, address, ResultKind::Success, Deadline::None);
        assert_eq!(
            metrics.diagnostics.observe_connect(address, None).previous,
            PreviousConnect::Success
        );
        for other in ["127.0.0.1:1235", "[::1]:1234"] {
            assert_eq!(
                metrics
                    .diagnostics
                    .observe_connect(other.parse().unwrap(), None)
                    .previous,
                PreviousConnect::NoHistory
            );
        }
        // 丢弃连接 future 和外层期限不能覆盖之前确认的成功。
        drop(report(&metrics, address));
        finish(&metrics, address, ResultKind::Timeout, Deadline::Peer);
        let timeout = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut outer = PeerObservation::new(
            metrics.clone(),
            Source::Dht,
            false,
            Arc::default(),
            timeout.clone(),
        );
        outer.observe_connect(address, Some(AttemptKind::Repeat));
        timeout.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(outer);
        assert_eq!(
            metrics.diagnostics.observe_connect(address, None).previous,
            PreviousConnect::Success
        );
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert_eq!(
            pair.0
                .connections
                .timings
                .values()
                .map(|d| d.count)
                .sum::<u64>(),
            6
        );
        assert_eq!(pair.0.connections.expired, 1);
        drop(pair);
        metrics.diagnostics.log_snapshot(true);
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        assert!(pair.1.connections.timings.is_empty());
        assert_eq!(pair.1.connections.expired, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn capacity_is_bounded_and_known_endpoints_can_still_update() {
        let metrics = Arc::new(Metrics::default());
        for port in 1..=CAPACITY as u16 {
            let address = SocketAddr::from(([127, 0, 0, 1], port));
            let mut report = report(&metrics, address);
            let error = crate::collection::peer::PeerError::Io(
                std::io::ErrorKind::ConnectionRefused.into(),
            );
            report.failure(&error);
            report.finish(ResultKind::Io, Deadline::None);
        }
        let known = SocketAddr::from(([127, 0, 0, 1], 1));
        assert_eq!(
            metrics.diagnostics.observe_connect(known, None).previous,
            PreviousConnect::ConnectionRefused
        );
        let unknown = "127.0.0.1:5000".parse().unwrap();
        finish(&metrics, unknown, ResultKind::Success, Deadline::None);
        assert_eq!(metrics.diagnostics.endpoint_count(), CAPACITY);
        assert_eq!(
            metrics.diagnostics.observe_connect(unknown, None).previous,
            PreviousConnect::NoHistory
        );
        finish(&metrics, known, ResultKind::Success, Deadline::None);
        assert_eq!(
            metrics.diagnostics.observe_connect(known, None).previous,
            PreviousConnect::Success
        );
        assert_eq!(
            metrics
                .diagnostics
                .snapshots
                .lock()
                .unwrap()
                .0
                .connections
                .capacity_dropped,
            1
        );
        tokio::time::advance(TTL).await;
        assert_eq!(metrics.diagnostics.endpoint_count(), 0);
        finish(&metrics, unknown, ResultKind::Success, Deadline::None);
        assert_eq!(metrics.diagnostics.endpoint_count(), 1);
    }
    /// TCP 已连接但握手失败，历史仍为 Success；Connect 阶段统计与真实 accept 次数一致。
    #[tokio::test]
    async fn loopback_reuse_does_not_skip_connections_or_confuse_handshake_failures() {
        use crate::address::AddressPolicy;
        use crate::collection::peer::MetadataConfig;
        use crate::collection::peer::PeerClient;
        use crate::info_hash::SwarmKey;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                drop(socket);
            }
        });
        let metrics = Arc::new(Metrics::default());
        let fetcher = PeerClient::new(MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap()
        .with_metrics(metrics.clone());
        let context = crate::collection::peer::PeerContext {
            observer: Default::default(),
            attempt: Some(AttemptKind::First),
            ..Default::default()
        };
        for _ in 0..2 {
            assert!(
                fetcher
                    .fetch_one(
                        SwarmKey([1; 20]),
                        address,
                        &tokio_util::sync::CancellationToken::new(),
                        context.clone()
                    )
                    .await
                    .is_err()
            );
        }
        server.await.unwrap();
        let pair = metrics.diagnostics.snapshots.lock().unwrap();
        let connections = &pair.0.connections.timings;
        assert_eq!(connections.len(), 2);
        assert!(
            connections
                .keys()
                .any(|key| key.previous == PreviousConnect::Success)
        );
        assert!(
            connections
                .keys()
                .all(|key| key.result == ResultKind::Success
                    && key.attempt_kind == Some(AttemptKind::First))
        );
        assert_eq!(connections.values().map(|d| d.count).sum::<u64>(), 2);
        assert_eq!(
            pair.0
                .peers
                .iter()
                .filter(|(key, _)| key.stage == Stage::Connect)
                .map(|(_, d)| d.count)
                .sum::<u64>(),
            2
        );
    }
}
