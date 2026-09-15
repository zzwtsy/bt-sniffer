//! 在协调器任务内驱动维护、补建、领取与背压检查，不另建调度任务。
use tokio::time::MissedTickBehavior;

use crate::collection::diagnostics::{TaskReport, TaskTiming};

use super::diagnostics::{AttemptContext, AttemptResult, metrics::Counter};
use super::jobs::{self, LocalReason, RetryReason};
use super::worker::{Outcome, run_job};
use super::{
    Collector, CollectorError, CompletionTotals, SampleBackpressure, WorkerResources, Workers,
    state_size,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
impl Collector {
    /// 返回 Err 包含收尾期间收集的错误；直接丢弃整个 future 不等于完成该收尾。
    pub(crate) async fn run(self) -> Result<(), Vec<CollectorError>> {
        self.supervise(Workers::default()).await
    }
    pub(super) async fn supervise(
        mut self,
        mut workers: Workers,
    ) -> Result<(), Vec<CollectorError>> {
        let cancel = CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let mut errors = Vec::new();
        if let Err(error) = self.run_inner(&mut workers, &cancel).await {
            // 先通知监督者，让异常收尾也受会话的共同期限约束。
            self.record_error(&mut errors, error);
        }
        self.finish(&mut workers, &cancel, &mut errors).await;
        let interval = self.metrics.interval_counts();
        tracing::info!(
            event = "collector_summary",
            schema_version = 1u64,
            scope = "interval",
            final_snapshot = true,
            metadata = interval[0],
            claims = interval[1],
            tcp_attempts = interval[2],
            running_workers = workers.len(),
            "采集关闭区间摘要"
        );
        self.store.log_backfill(true);
        self.metrics.log_final();
        if let Ok(now) = self.now() {
            self.backpressure.log(now);
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
    pub(super) async fn run_inner(
        &mut self,
        workers: &mut Workers,
        work_cancel: &CancellationToken,
    ) -> Result<(), CollectorError> {
        let mut families = Vec::new();
        for handle in &self.handles {
            families.push(
                handle
                    .status()
                    .await
                    .map_err(|source| CollectorError::Control {
                        operation: "读取节点状态",
                        source,
                    })?
                    .family,
            );
        }
        let resources = Arc::new(WorkerResources::new(
            self.peer.clone(),
            self.metrics.clone(),
        ));
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut claim_policy = jobs::ClaimPolicy::new();
        let mut cycles = 0u64;
        let mut observed_pause = (false, false, false);
        let mut totals = CompletionTotals::default();
        let mut state_bytes = 0;
        let mut backfill_cursor = None;
        let mut recent_cursor = None;
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => break,
                changed = self.faults.changed() => {
                    if changed.is_err() {
                        return Err(CollectorError::SupervisorClosed);
                    }
                    // 每次单位通知都对应一次已接纳故障，不需要读取应用层的故障详情。
                    self.storage_paused = true;
                    self.backpressure.update(self.now()?, 0, None, true);
                    work_cancel.cancel();
                    self.pause(true).await?;
                }
                // 异常 worker 也要保留领取记录，交给 finish 收尾。
                result = workers.next(), if !workers.is_empty() => {
                    let (job, result) = result.expect("非空 worker 集合");
                    let result = match result {
                        Ok(result) => result,
                        Err(error) => {
                            workers.interrupted.push(job);
                            return Err(error);
                        }
                    };
                    self.apply_running_outcome(job, result, &mut totals).await?;
                    if !self.storage_paused && !self.stop.is_cancelled() {
                        self.claim_workers(
                            workers, work_cancel, &resources, &families, &mut claim_policy,
                        ).await?;
                    }
                }
                event = self.receiver.recv(), if !self.storage_paused => {
                    if let Some(event) = event {
                        self.accept_announce(event).await?;
                    }
                }
                _ = tick.tick() => {
                    let paused=(self.backpressure.capacity,self.backpressure.backlog,self.storage_paused);
                    if observed_pause!=paused{self.store.observer.emit(crate::observation::Kind::Backpressure,"sampling_pause","changed",||serde_json::json!({"capacity":paused.0,"backlog":paused.1,"storage":paused.2}));observed_pause=paused;}
                    self.store.observer.state("collector",||serde_json::json!({"running_workers":workers.len(),"capacity_paused":self.backpressure.capacity,"backlog_paused":self.backpressure.backlog,"storage_paused":self.storage_paused,"state_bytes":state_bytes,"tracked_tcp_ips":resources.tcp.tracked_tcp_ips(),"metrics":resources.metrics.snapshot()}));
                    // 先检查容量，再补建和领取，达到保护阈值后停止扩张。
                    if cycles.is_multiple_of(5) && !self.storage_paused {
                        self.check_storage_capacity(work_cancel, &mut state_bytes).await?;
                    }
                    if !self.storage_paused {
                        if self.config.sample_backpressure == SampleBackpressure::Freshness {
                            recent_cursor = self.store
                                .backfill_recent_page(self.now()?, recent_cursor).await?;
                        }
                        backfill_cursor = self.store.backfill_page(self.now()?, backfill_cursor).await?;
                        self.claim_workers(
                            workers, work_cancel, &resources, &families, &mut claim_policy,
                        ).await?;
                        self.update_sampling_backpressure(cycles.is_multiple_of(5)).await?;
                    }
                    if cycles.is_multiple_of(60) {
                        self.log_status(&resources, &totals, state_bytes, workers.len()).await?;
                    }
                    cycles = cycles.wrapping_add(1);
                }
            }
        }
        Ok(())
    }
    /// 调用者控制检查频率；I/O 或控制错误向上返回，不继续补建或领取。
    pub(super) async fn check_storage_capacity(
        &mut self,
        work_cancel: &CancellationToken,
        state_bytes: &mut u64,
    ) -> Result<(), CollectorError> {
        *state_bytes = state_size(self.config.directory.clone()).await?;
        if *state_bytes >= self.config.state_max_bytes.saturating_sub(64 * 1024 * 1024) {
            self.storage_paused = true;
            self.backpressure.update(self.now()?, 0, None, true);
            work_cancel.cancel();
            self.pause(true).await?;
            tracing::warn!(
                event = "storage_capacity_paused",
                schema_version = 1u64,
                phase = "running",
                action = "pause_collection",
                state_bytes = *state_bytes,
                limit_bytes = self.config.state_max_bytes,
                "状态容量达到保护阈值，暂停采集；保留数据，重启后重新检查"
            );
        }
        Ok(())
    }

    /// 在补建和领取后读取活跃量及按需读取首试缓冲快照，更新主动采样暂停与恢复统计；不关闭宣布入口。
    pub(super) async fn update_sampling_backpressure(
        &mut self,
        refresh_due: bool,
    ) -> Result<(), CollectorError> {
        let active = self.store.active_jobs().await?;
        let now = self.now()?;
        let due = if refresh_due && self.config.sample_backpressure == SampleBackpressure::Freshness
        {
            Some(
                self.store
                    .first_attempt_waiting(now, self.config.policy)
                    .await?,
            )
        } else {
            None
        };
        let resumes = self.backpressure.resumes;
        if self.backpressure.update(now, active, due, false) {
            self.pause(self.backpressure.paused()).await?;
            self.backpressure.log(now);
        }
        self.metrics.add(
            Counter::SamplingResumes,
            self.backpressure.resumes - resumes,
        );
        Ok(())
    }

    /// 补足并发空位；每次领取后先记指标、推进轮次，再把领取副本与网络任务交给 Workers。
    /// 事务领取时间保持原位置；成功领取后共用一次观察时间计算两种等待。
    pub(super) async fn claim_workers(
        &self,
        workers: &mut Workers,
        work_cancel: &CancellationToken,
        resources: &Arc<WorkerResources>,
        families: &[crate::dht::routing::AddressFamily],
        claim_policy: &mut jobs::ClaimPolicy,
    ) -> Result<(), CollectorError> {
        while workers.len() < self.config.concurrency {
            let Some(claim) = self
                .store
                .claim_by_order(self.now()?, claim_policy.order(), self.config.policy)
                .await?
            else {
                break;
            };
            let jobs::Claim {
                job,
                due_at,
                first_seen,
            } = claim;
            let attempt = AttemptContext::from_job(&job);
            let observed_at_ms = self.now()?;
            for (timing, at) in [
                (TaskTiming::DueWait, due_at),
                (TaskTiming::DiscoveryAge, first_seen),
            ] {
                let duration =
                    Duration::from_millis(observed_at_ms.saturating_sub(at).max(0) as u64);
                resources.metrics.diagnostics.attempt(
                    attempt,
                    timing,
                    AttemptResult::Observed,
                    duration,
                );
            }
            resources.metrics.add(
                if job.had_valid_hint {
                    Counter::ClaimsWithHint
                } else {
                    Counter::ClaimsWithoutHint
                },
                1,
            );
            claim_policy.on_claimed();
            let observation = job.clone();
            let work = run_job(
                job,
                self.handles.clone(),
                resources.clone(),
                self.config.policy,
                families.to_vec(),
                work_cancel.clone(),
            );
            let metrics = resources.metrics.clone();
            workers.spawn(observation, async move {
                let mut report = TaskReport::new(metrics, attempt);
                let outcome = work.await;
                let attempt_result = match &outcome {
                    Outcome::Success(_) => AttemptResult::Downloaded,
                    Outcome::Retry(RetryReason::Failed(reason)) => {
                        AttemptResult::RemoteFailure(*reason)
                    }
                    Outcome::Retry(RetryReason::Local(LocalReason::Cancelled)) => {
                        AttemptResult::Cancelled
                    }
                    Outcome::Retry(_) => AttemptResult::LocalDeferral,
                    Outcome::Control(_) => AttemptResult::ControlFailure,
                };
                report.finish(attempt_result);
                outcome
            });
        }
        Ok(())
    }
}
