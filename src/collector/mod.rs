//! 协调持久化任务：接纳发现 → 领取任务 → 网络获取 → 保存结果。
//!
//! SQLite 是任务状态的唯一事实来源；Workers 持有网络任务及其领取记录。
//! run_inner 负责正常调度，supervise 接收故障，finish 回收任务并保存可提交的结果。
//! 取消通知只要求网络任务停止，收尾仍需等待任务并处理每条领取记录。
//!
//! 入口由 app::session 创建并监督；worker 执行单任务，lookup 负责查找，lifecycle 配对退出与领取记录。
mod backpressure;
mod lifecycle;
mod lookup;
mod worker;
use crate::metrics::{Counter, Timing};
pub(crate) use backpressure::Mode as SampleBackpressure;
pub(crate) use lifecycle::CollectorError;
use lifecycle::Workers;
use worker::{Outcome, run_job};
#[cfg(test)]
mod tests;
use crate::{
    dht::dispatcher::{AnnounceEvent, DhtHandle, FetchIngress},
    metadata::{MetadataConfig, MetadataFetcher},
    net::address::AddressPolicy,
    storage::{
        Clock, StorageError, StorageHandle,
        jobs::{Job, LocalReason, RetryReason, UpdateResult},
        unix_millis,
    },
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

/// 应用交给协调器的资源边界；只控制采集与接纳，不改变 DHT 的协议配额。
#[derive(Clone, Debug)]
pub(crate) struct Config {
    /// 同时领取并运行的 worker 上限，也是 fetcher 的共享并发上限。
    pub(crate) concurrency: usize,
    /// pending、running、retry_wait 合计的接纳上限，不限制历史成功记录数。
    pub(crate) max_active: usize,
    pub(crate) sample_backpressure: SampleBackpressure,
    /// 状态目录软字节预算；接近上限时保留清理空间并暂停新增工作。
    pub(crate) state_max_bytes: u64,
    pub(crate) directory: PathBuf,
    pub(crate) policy: AddressPolicy,
}
/// 会话创建并监督的协调器；持有宣布接收端、任务调度状态与共享指标。
/// 数据库 handle 只提交命令，数据库的最终关闭责任仍属于会话。
pub(crate) struct Collector {
    metrics: Arc<crate::metrics::Metrics>,
    backpressure: backpressure::Backpressure,
    store: StorageHandle,
    handles: Vec<DhtHandle>,
    config: Config,
    clock: Clock,
    storage_paused: bool,
    receiver: mpsc::Receiver<AnnounceEvent>,
    ingress: FetchIngress,
    stop: CancellationToken,
    /// 只接收暂停通知，故障详情和应用是否退出由会话解释。
    faults: watch::Receiver<()>,
    /// 同步报告模块错误；调用者在返回前保存诊断并发布故障，不执行 I/O。
    report_error: Box<dyn Fn(&CollectorError) + Send + Sync>,
}
/// 单次协调器运行中已经提交的结果累计数；只归 run_inner 所有，不包含调度或共享状态。
#[derive(Default)]
struct CompletionTotals {
    succeeded: u64,
    failed: u64,
    failure_categories: std::collections::BTreeMap<&'static str, u64>,
}

impl Collector {
    /// 启用任务接纳、恢复磁盘任务并向节点安装宣布入口，尚不运行 worker。
    /// 中途失败可能已有数据库更新或部分入口安装；调用者须关闭已创建的会话资源。
    pub(crate) async fn new(
        store: StorageHandle,
        handles: Vec<DhtHandle>,
        config: Config,
        clock: Clock,
        stop: CancellationToken,
        pause_notifications: watch::Sender<()>,
        report_error: Box<dyn Fn(&CollectorError) + Send + Sync>,
    ) -> Result<Self, CollectorError> {
        store.enable_fetch(config.max_active);
        store
            .recover_jobs(
                clock
                    .millis_at(tokio::time::Instant::now().into_std())
                    .map_err(CollectorError::Clock)?,
            )
            .await?;
        let (sender, receiver) = mpsc::channel(1024);
        let ingress = FetchIngress {
            sender,
            paused: Arc::new(AtomicBool::new(false)),
            dropped: Arc::new(AtomicU64::new(0)),
            observed: Arc::new(AtomicU64::new(0)),
        };
        // start_fetch 返回前发布入口；启动被取消时由会话关闭已创建的节点。
        for handle in &handles {
            if let Err(source) = handle.fetch_ingress(Some(ingress.clone())).await {
                ingress.paused.store(true, Ordering::Relaxed);
                return Err(CollectorError::Control {
                    operation: "安装发现入口",
                    source,
                });
            }
        }
        Ok(Self {
            metrics: Arc::new(crate::metrics::Metrics::default()),
            backpressure: backpressure::Backpressure::new(
                config.sample_backpressure,
                config.max_active,
            ),
            store,
            handles,
            config,
            clock,
            storage_paused: false,
            receiver,
            ingress,
            stop,
            faults: pause_notifications.subscribe(),
            report_error,
        })
    }
    /// 使用事件观察时间保存提示；接纳确认后才增加 AnnouncesAccepted，拒绝计入丢弃。
    async fn accept_announce(&self, event: AnnounceEvent) -> Result<(), StorageError> {
        if !self
            .store
            .discover_peer(event.hash, event.peer, unix_millis(event.observed_at)?)
            .await?
        {
            self.ingress.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.add(Counter::AnnouncesAccepted, 1);
        }
        Ok(())
    }
    /// 主动采样采用总体背压值；宣布入口只随存储暂停关闭，容量/积压暂停不关闭它。
    async fn pause(&self, paused: bool) -> Result<(), CollectorError> {
        self.ingress
            .paused
            .store(self.storage_paused, Ordering::Relaxed);
        for handle in &self.handles {
            handle
                .pause_sampling(paused)
                .await
                .map_err(|source| CollectorError::Control {
                    operation: "暂停采样",
                    source,
                })?;
        }
        Ok(())
    }
    fn now(&self) -> Result<i64, CollectorError> {
        self.clock
            .millis_at(tokio::time::Instant::now().into_std())
            .map_err(CollectorError::Clock)
    }
    fn record_error(&self, errors: &mut Vec<CollectorError>, error: CollectorError) {
        (self.report_error)(&error);
        errors.push(error);
    }
    /// 持续调度直到停止或故障，随后回收 worker、保存可提交结果并输出最终统计。
    /// 返回 Err 包含收尾期间收集的错误；直接丢弃整个 future 不等于完成该收尾。
    pub(crate) async fn run(self) -> Result<(), Vec<CollectorError>> {
        self.supervise(Workers::default()).await
    }
    async fn supervise(mut self, mut workers: Workers) -> Result<(), Vec<CollectorError>> {
        let cancel = CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let mut errors = Vec::new();
        if let Err(error) = self.run_inner(&mut workers, &cancel).await {
            // 先通知监督者，让异常收尾也受会话的共同期限约束。
            self.record_error(&mut errors, error);
        }
        self.finish(&mut workers, &cancel, &mut errors).await;
        self.metrics.log();
        if let Ok(now) = self.now() {
            self.backpressure.log(now);
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
    async fn run_inner(
        &mut self,
        workers: &mut Workers,
        work_cancel: &CancellationToken,
    ) -> Result<(), CollectorError> {
        let fetcher = MetadataFetcher::new(MetadataConfig {
            concurrency: self.config.concurrency,
            address_policy: self.config.policy,
            ..Default::default()
        })
        .map_err(CollectorError::Configuration)?
        .with_metrics(self.metrics.clone());
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
        let network = Arc::new(lookup::Network::with_metrics(self.metrics.clone()));
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut claim_turn = 0u8;
        let mut cycles = 0u64;
        let mut totals = CompletionTotals::default();
        let mut state_bytes = 0;
        let mut backfill_cursor = None;
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => break,
                changed = self.faults.changed() => {
                    if changed.is_err() {
                        return Err(CollectorError::SupervisorClosed);
                    }
                    // 每次单位通知都对应一次已接纳故障，不需要读取应用层的故障详情。
                    self.storage_paused = true;
                    self.backpressure.update(self.now()?,0,None,true);
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
                }
                event = self.receiver.recv(), if !self.storage_paused => {
                    if let Some(event) = event {
                        self.accept_announce(event).await?;
                    }
                }
                _ = tick.tick() => {
                    // 先检查容量，再补建和领取，达到保护阈值后停止扩张。
                    if cycles.is_multiple_of(5) && !self.storage_paused {
                        self.check_storage_capacity(work_cancel, &mut state_bytes).await?;
                    }
                    if !self.storage_paused {
                        self.update_sampling_backpressure(cycles.is_multiple_of(5)).await?;
                        backfill_cursor = self.store.backfill_page(self.now()?, backfill_cursor).await?;
                        self.claim_workers(
                            workers, work_cancel, &fetcher, &network, &families, &mut claim_turn,
                        ).await?;
                    }
                    if cycles.is_multiple_of(60) {
                        self.log_status(&network, &totals, state_bytes).await?;
                    }
                    cycles = cycles.wrapping_add(1);
                }
            }
        }
        Ok(())
    }
    /// 正常运行的结果提交；只有 Applied 更新累计数，远端失败与本地延期保留各自分类。
    /// 与 save_outcome 的退出策略不同，此处不能把全部重试改成本地取消。
    async fn apply_running_outcome(
        &self,
        job: Job,
        result: Outcome,
        totals: &mut CompletionTotals,
    ) -> Result<(), CollectorError> {
        match result {
            Outcome::Control(source) => {
                return Err(CollectorError::Control {
                    operation: "查找 peer",
                    source,
                });
            }
            Outcome::Success(metadata) => {
                let bytes = metadata.info().len() as u64;
                if self.store.complete_job(job, metadata, self.now()?).await?
                    == UpdateResult::Applied
                {
                    totals.succeeded += 1;
                    self.metrics.add(Counter::MetadataCount, 1);
                    self.metrics.add(Counter::MetadataBytes, bytes);
                }
            }
            Outcome::Retry(reason) => {
                let applied =
                    self.store.retry_job(job, self.now()?, reason).await? == UpdateResult::Applied;
                if applied {
                    self.metrics.add(
                        if reason.failure_category().is_some() {
                            Counter::RemoteFailures
                        } else {
                            Counter::LocalDeferrals
                        },
                        1,
                    );
                }
                if applied && let Some(category) = reason.failure_category() {
                    totals.failed += 1;
                    *totals.failure_categories.entry(category).or_insert(0u64) += 1;
                }
            }
        }
        Ok(())
    }

    /// 更新磁盘占用快照；达到保护阈值后依次暂停存储接纳、通知取消并暂停采样。
    /// 调用者控制检查频率；I/O 或控制错误向上返回，不继续补建或领取。
    async fn check_storage_capacity(
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
                state_bytes = *state_bytes,
                limit = self.config.state_max_bytes,
                "状态容量达到保护阈值，暂停采集；保留数据，重启后重新检查"
            );
        }
        Ok(())
    }

    /// 读取活跃量及按需读取到期快照，更新主动采样暂停与恢复统计；不关闭宣布入口。
    async fn update_sampling_backpressure(
        &mut self,
        refresh_due: bool,
    ) -> Result<(), CollectorError> {
        let active = self.store.active_jobs().await?;
        let now = self.now()?;
        let due = if refresh_due && self.config.sample_backpressure == SampleBackpressure::Freshness
        {
            Some(self.store.due_stats(now, self.config.policy).await?)
        } else {
            None
        };
        let resumes = self.backpressure.resumes;
        if self.backpressure.update(now, active, due.as_ref(), false) {
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
    /// 领取时间和等待统计时间分别读取，保持数据库接纳与指标观察的原有边界。
    async fn claim_workers(
        &self,
        workers: &mut Workers,
        work_cancel: &CancellationToken,
        fetcher: &MetadataFetcher,
        network: &Arc<lookup::Network>,
        families: &[crate::dht::routing::AddressFamily],
        claim_turn: &mut u8,
    ) -> Result<(), CollectorError> {
        while workers.len() < self.config.concurrency {
            let Some((job, fresh, due_at)) = self
                .store
                .claim_preferred(self.now()?, Some(*claim_turn < 3), self.config.policy)
                .await?
            else {
                break;
            };
            network.metrics.add(
                if fresh {
                    Counter::ClaimsFresh
                } else {
                    Counter::ClaimsOther
                },
                1,
            );
            network.metrics.observe(
                Timing::ClaimWait,
                Duration::from_millis(self.now()?.saturating_sub(due_at).max(0) as u64),
            );
            *claim_turn = (*claim_turn + 1) % 4;
            workers.spawn(
                job.clone(),
                run_job(
                    job,
                    self.handles.clone(),
                    fetcher.clone(),
                    network.clone(),
                    self.config.policy,
                    families.to_vec(),
                    work_cancel.clone(),
                ),
            );
        }
        Ok(())
    }

    /// 按原顺序输出网络、背压、数据库和协调器累计状态；查询失败仍向主循环报告。
    async fn log_status(
        &mut self,
        network: &lookup::Network,
        totals: &CompletionTotals,
        state_bytes: u64,
    ) -> Result<(), CollectorError> {
        network.metrics.log();
        self.backpressure.log(self.now()?);
        let due = self
            .store
            .due_stats(self.now()?, self.config.policy)
            .await?;
        let stats = self.store.fetch_stats().await?;
        stats.log(false);
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
            connections = network.tracked_tcp_ips(),
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

    /// 退出时保留成功结果，其余任务只延期，避免把本地关闭计作远端失败。
    async fn save_outcome(&self, job: Job, outcome: Outcome) -> Result<(), CollectorError> {
        match outcome {
            Outcome::Control(source) => {
                return Err(CollectorError::Control {
                    operation: "查找 peer",
                    source,
                });
            }
            Outcome::Success(metadata) => {
                let bytes = metadata.info().len() as u64;
                if self.store.complete_job(job, metadata, self.now()?).await?
                    == UpdateResult::Applied
                {
                    self.metrics.add(Counter::MetadataCount, 1);
                    self.metrics.add(Counter::MetadataBytes, bytes);
                }
            }
            Outcome::Retry(_) => {
                if self
                    .store
                    .retry_job(job, self.now()?, RetryReason::Local(LocalReason::Cancelled))
                    .await?
                    == UpdateResult::Applied
                {
                    self.metrics.add(Counter::LocalDeferrals, 1);
                }
            }
        }
        Ok(())
    }
    fn record_cleanup_control(
        &self,
        errors: &mut Vec<CollectorError>,
        operation: &'static str,
        result: Result<(), crate::dht::dispatcher::QueryError>,
    ) {
        if let Err(source) = result {
            if lifecycle::already_closed(&source) {
                return;
            }
            self.record_error(errors, CollectorError::Control { operation, source });
        }
    }
    async fn finish(
        &mut self,
        workers: &mut Workers,
        cancel: &CancellationToken,
        errors: &mut Vec<CollectorError>,
    ) {
        self.ingress.paused.store(true, Ordering::Relaxed);
        self.receiver.close();
        cancel.cancel();
        for handle in &self.handles {
            let result = handle.pause_sampling(true).await;
            self.record_cleanup_control(errors, "停止采样", result);
            let result = handle.fetch_ingress(None).await;
            self.record_cleanup_control(errors, "撤销发现入口", result);
        }
        let mut writable = !errors.iter().any(CollectorError::is_storage);
        while let Some((job, result)) = workers.next().await {
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.record_error(errors, error);
                    Outcome::Retry(RetryReason::Deferred)
                }
            };
            if writable && let Err(error) = self.save_outcome(job, outcome).await {
                writable = false;
                self.record_error(errors, error);
            }
        }
        for job in std::mem::take(&mut workers.interrupted) {
            if writable
                && let Err(error) = self
                    .save_outcome(job, Outcome::Retry(RetryReason::Deferred))
                    .await
            {
                writable = false;
                self.record_error(errors, error);
            }
        }
        // 暂停时不扩大发现记录；已经领取的任务仍按 generation 安全收尾。
        if writable && !self.storage_paused {
            while let Some(event) = self.receiver.recv().await {
                if let Err(error) = self.accept_announce(event).await {
                    self.record_error(errors, error.into());
                    break;
                }
            }
        }
    }
}

/// 文件系统检查放到阻塞线程；统计数据库与 WAL，结果用于采集软容量保护。
async fn state_size(directory: PathBuf) -> Result<u64, CollectorError> {
    tokio::task::spawn_blocking(move || {
        let mut bytes = 0u64;
        for name in ["state.sqlite3", "state.sqlite3-wal"] {
            match std::fs::metadata(directory.join(name)) {
                Ok(meta) => bytes = bytes.saturating_add(meta.len()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(StorageError::from(error)),
            }
        }
        Ok(bytes)
    })
    .await
    .map_err(CollectorError::InspectionTask)?
    .map_err(CollectorError::Storage)
}
