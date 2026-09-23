//! 协调器的每分钟诊断；数据库快照与进程内指标分别取得，不新增后台任务。
use super::jobs;
use super::{Collector, CollectorError, CompletionTotals, SampleBackpressure, WorkerResources};
use std::sync::atomic::Ordering;

impl Collector {
    /// 先取得非数据库指标，再读取同一时间的数据库快照并输出；不参与调度。
    /// 数据库快照完整成功后才输出相关事件；查询失败向主循环报告。
    pub(super) async fn log_status(
        &mut self,
        resources: &WorkerResources,
        totals: &CompletionTotals,
        state_bytes: u64,
        running_workers: usize,
    ) -> Result<(), CollectorError> {
        let interval = resources.metrics.interval_counts();
        resources.metrics.log();
        let now_ms = self.now()?;
        self.backpressure.log(now_ms);
        let snapshot = self
            .store
            .status_snapshot(now_ms, self.config.policy)
            .await?;
        let super::jobs::CollectionStatusSnapshot {
            due,
            stats,
            recent_active,
            first_attempt_waiting,
            backlog,
        } = snapshot;
        self.store.observer.state("database", || {
            serde_json::json!({
                "jobs": {
                    "pending": stats.pending,
                    "running": stats.running,
                    "retry_wait": stats.retry_wait,
                    "dormant": stats.dormant,
                    "succeeded": stats.succeeded,
                },
                "metadata_count": stats.metadata_count,
                "metadata_bytes": stats.metadata_bytes,
                "due_count": due.count,
                "recent_active": recent_active,
                "first_attempt_waiting": first_attempt_waiting,
            })
        });
        stats.log(false);
        let buffer_limit = self
            .config
            .max_active
            .min(self.config.concurrency.saturating_mul(4));
        backlog.log();
        self.store.log_backfill(false);
        tracing::info!(
            event = "admission_status",
            schema_version = 1u64,
            active = stats.active(),
            recent_active,
            first_attempt_waiting,
            buffer_limit,
            high = buffer_limit,
            low = buffer_limit / 4,
            admission_policy_version = jobs::admission::POLICY_VERSION,
            scheduling_policy_version = jobs::SCHEDULING_POLICY_VERSION,
            extension_handshake_policy_version =
                crate::collection::peer::wire::EXTENSION_HANDSHAKE_POLICY_VERSION,
            backpressure_basis = if self.config.sample_backpressure == SampleBackpressure::Freshness
            {
                "first_attempt_waiting"
            } else {
                "capacity"
            },
            "采集接纳快照"
        );
        tracing::info!(
            event = "collector_summary",
            schema_version = 1u64,
            scope = "interval",
            final_snapshot = false,
            metadata = interval[0],
            claims = interval[1],
            tcp_attempts = interval[2],
            running_workers,
            due_count = due.count,
            recent_active,
            capacity_paused = self.backpressure.capacity,
            backlog_paused = self.backpressure.backlog,
            storage_paused = self.storage_paused,
            "采集区间摘要"
        );
        for (category, count) in &totals.failure_categories {
            tracing::info!(
                event = "collector_failure",
                schema_version = 1u64,
                scope = "total",
                category,
                count,
                "任务失败类别"
            );
        }
        tracing::info!(
            event = "collector_status",
            schema_version = 1u64,
            due_count = due.count,
            oldest_wait_ms = due.oldest_wait_ms,
            fresh_due = due.fresh,
            sample_hashes = self.store.sample_observations(),
            active = stats.active(),
            succeeded = totals.succeeded,
            failed = totals.failed,
            connections = resources.tcp.tracked_tcp_ips(),
            announces = self.ingress.observed.load(Ordering::Relaxed),
            announce_dropped = self.ingress.dropped.load(Ordering::Relaxed),
            state_bytes,
            capacity_paused = self.backpressure.capacity,
            backlog_paused = self.backpressure.backlog,
            storage_paused = self.storage_paused,
            admitted_tasks_database = stats.active() + stats.dormant + stats.succeeded,
            "metadata 采集状态"
        );
        Ok(())
    }
}
