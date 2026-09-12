//! 协调持久化任务：接纳发现 → 领取任务 → 网络获取 → 保存结果。
//!
//! SQLite 是任务状态的唯一事实来源；Workers 持有网络任务及其领取记录。
//! run_inner 负责正常调度，supervise 接收故障，finish 回收任务并保存可提交的结果。
//! 取消通知只要求网络任务停止，收尾仍需等待任务并处理每条领取记录。
//!
//! 入口由 persistence 创建并监督；lookup 负责查找，lifecycle 负责 worker 退出与领取记录的配对。
mod backpressure;
mod lifecycle;
mod lookup;
use crate::metrics::{Counter, Timing};
pub(crate) use backpressure::Mode as SampleBackpressure;
pub(crate) use lifecycle::CollectorError;
use lifecycle::Workers;
#[cfg(test)]
mod tests;
use crate::{
    dht::dispatcher::{AnnounceEvent, DhtHandle, FetchIngress},
    krpc::InfoHashV1,
    metadata::{MetadataConfig, MetadataError, MetadataFetcher, VerifiedMetadata},
    net::address::AddressPolicy,
    persistence::{FaultLog, SessionFault, report_fault},
    storage::{
        Clock, StorageError, StorageHandle,
        jobs::{Job, LocalReason, RetryReason, UpdateResult},
        unix_millis,
    },
};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) concurrency: usize,
    pub(crate) max_active: usize,
    pub(crate) sample_backpressure: SampleBackpressure,
    pub(crate) state_max_bytes: u64,
    pub(crate) directory: PathBuf,
    pub(crate) policy: AddressPolicy,
}
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
    faults: watch::Receiver<Option<SessionFault>>,
    report: watch::Sender<Option<SessionFault>>,
    fault_log: FaultLog,
}
impl Collector {
    pub(crate) async fn new(
        store: StorageHandle,
        handles: Vec<DhtHandle>,
        config: Config,
        clock: Clock,
        stop: CancellationToken,
        report: watch::Sender<Option<SessionFault>>,
        fault_log: FaultLog,
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
            faults: report.subscribe(),
            report,
            fault_log,
        })
    }
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
        self.fault_log.push(error.to_string());
        report_fault(&self.report, error.fault());
        errors.push(error);
    }
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
        let mut failed = 0u64;
        let mut succeeded = 0u64;
        let mut failure_categories = std::collections::BTreeMap::new();
        let mut state_bytes = 0;
        let mut backfill_cursor = None;
        loop {
            tokio::select! {
                _ = self.stop.cancelled() => break,
                changed = self.faults.changed() => {
                    if changed.is_err() {
                        return Err(CollectorError::SupervisorClosed);
                    }
                    if self.faults.borrow().is_some() {
                        self.storage_paused = true;
                        self.backpressure.update(self.now()?,0,None,true);
                        work_cancel.cancel();
                        self.pause(true).await?;
                    }
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
                    match result {
                        Outcome::Control(source) => return Err(CollectorError::Control { operation: "查找 peer", source }),
                        Outcome::Success(metadata) => {
                            let bytes = metadata.info().len() as u64;
                            if self.store.complete_job(job, metadata, self.now()?).await? == UpdateResult::Applied {
                                succeeded += 1;
                                network.metrics.add(Counter::MetadataCount, 1);
                                network.metrics.add(Counter::MetadataBytes, bytes);
                            }
                        }
                        Outcome::Retry(reason) => {
                            let applied = self.store.retry_job(job, self.now()?, reason).await? == UpdateResult::Applied;
                            if applied { network.metrics.add(if reason.failure_category().is_some() { Counter::RemoteFailures } else { Counter::LocalDeferrals }, 1); }
                            if applied && let Some(category) = reason.failure_category() {
                                failed += 1;
                                *failure_categories.entry(category).or_insert(0u64) += 1;
                            }
                        }
                    }
                }
                event = self.receiver.recv(), if !self.storage_paused => {
                    if let Some(event) = event {
                        self.accept_announce(event).await?;
                    }
                }
                _ = tick.tick() => {
                    // 先检查容量，再补建和领取，达到保护阈值后停止扩张。
                    if cycles.is_multiple_of(5) && !self.storage_paused {
                        state_bytes = state_size(self.config.directory.clone()).await?;
                        if state_bytes >= self.config.state_max_bytes.saturating_sub(64 * 1024 * 1024) {
                            self.storage_paused = true;
                            self.backpressure.update(self.now()?, 0, None, true);
                            work_cancel.cancel();
                            self.pause(true).await?;
                            tracing::warn!(state_bytes, limit = self.config.state_max_bytes,
                                "状态容量达到保护阈值，暂停采集；保留数据，重启后重新检查");
                        }
                    }
                    if !self.storage_paused {
                        let active = self.store.active_jobs().await?;
                        let now = self.now()?;
                        let due = if cycles.is_multiple_of(5) && self.config.sample_backpressure == SampleBackpressure::Freshness {
                            Some(self.store.due_stats(now,self.config.policy).await?)
                        } else { None };
                        let resumes = self.backpressure.resumes;
                        if self.backpressure.update(now,active,due.as_ref(),false) {
                            self.pause(self.backpressure.paused()).await?;
                            self.backpressure.log(now);
                        }
                        self.metrics.add(Counter::SamplingResumes,self.backpressure.resumes-resumes);
                        backfill_cursor = self.store.backfill_page(self.now()?, backfill_cursor).await?;
                        while workers.len() < self.config.concurrency {
                            let Some((job, fresh, due_at)) = self.store.claim_preferred(self.now()?, Some(claim_turn < 3), self.config.policy).await? else {
                                break;
                            };
                            network.metrics.add(if fresh { Counter::ClaimsFresh } else { Counter::ClaimsOther }, 1);
                            network.metrics.observe(Timing::ClaimWait, Duration::from_millis(self.now()?.saturating_sub(due_at).max(0) as u64));
                            claim_turn = (claim_turn + 1) % 4;
                            workers.spawn(job.clone(), run_job(
                                job, self.handles.clone(), fetcher.clone(), network.clone(),
                                self.config.policy, families.clone(), work_cancel.clone(),
                            ));
                        }
                    }
                    if cycles.is_multiple_of(60) {
                        network.metrics.log();
                        self.backpressure.log(self.now()?);
                        let due = self.store.due_stats(self.now()?, self.config.policy).await?;
                        let stats = self.store.fetch_stats().await?;
                        stats.log(false);
                        for (category,count) in &failure_categories {
                            tracing::info!(event="collector_failure",schema_version=1u64,scope="total",category,count,"任务失败类别");
                        }
                        tracing::info!(
                            event="collector_status",schema_version=1u64, due_count=due.count, oldest_wait_ms=due.oldest_wait_ms, fresh_due=due.fresh,
                            sample_hashes = self.store.sample_observations(), active = stats.active(),
                            succeeded, failed, connections = network.connections(),
                            announces = self.ingress.observed.load(Ordering::Relaxed),
                            announce_dropped = self.ingress.dropped.load(Ordering::Relaxed),
                            state_bytes, capacity_paused=self.backpressure.capacity, backlog_paused=self.backpressure.backlog, storage_paused = self.storage_paused,
                            admitted_tasks_database=stats.active()+stats.dormant+stats.succeeded,
                            "metadata 采集状态"
                        );
                    }
                    cycles = cycles.wrapping_add(1);
                }
            }
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionStage {
    LocalWait,
    Dht,
    Peer,
    Validation,
}
pub(super) enum Outcome {
    Control(crate::dht::dispatcher::QueryError),
    Success(VerifiedMetadata),
    Retry(RetryReason),
}
async fn run_job(
    job: Job,
    handles: Vec<DhtHandle>,
    fetcher: MetadataFetcher,
    network: Arc<lookup::Network>,
    policy: AddressPolicy,
    families: Vec<crate::dht::routing::AddressFamily>,
    cancel: CancellationToken,
) -> Outcome {
    let _timer = network.metrics.timer(Timing::Task);
    let stage = std::sync::Mutex::new(ExecutionStage::LocalWait);
    let progress = Arc::new(crate::dht::dispatcher::RpcProgress::default());
    let dht_active = AtomicBool::new(true);
    let work = async {
        let mut tried = std::collections::HashSet::new();
        let mut last = None;
        let mut hints: std::collections::VecDeque<_> = job
            .peers
            .iter()
            .copied()
            .filter(|p| policy.accepts(*p) && families.iter().any(|f| f.accepts(*p)))
            .collect();
        let mut first_hints: std::collections::VecDeque<_> =
            hints.drain(..hints.len().min(2)).collect();
        let (sender, mut peers) = mpsc::channel(32);
        let mut lookup = Box::pin(lookup::stream(
            &handles,
            job.hash,
            network.clone(),
            Some(sender),
            progress.clone(),
        ));
        let mut summary = None;
        while tried.len() < 8 {
            let mut next = first_hints
                .pop_front()
                .or_else(|| peers.try_recv().ok())
                .or_else(|| hints.pop_front());
            while next.is_none() && summary.is_none() {
                *stage.lock().expect("执行阶段锁") = ExecutionStage::Dht;
                tokio::select! {
                    biased;
                    result = &mut lookup => {
                        dht_active.store(false, Ordering::Relaxed);
                        if let Some(error) = result.fault { return Outcome::Control(error); }
                        summary = Some(result);
                        next = peers.try_recv().ok();
                    }
                    peer = peers.recv() => { next = peer; }
                }
            }
            let Some(peer) = next else {
                break;
            };
            if !policy.accepts(peer)
                || !families.iter().any(|f| f.accepts(peer))
                || !tried.insert(peer)
            {
                continue;
            }
            let attempt = attempt(
                &fetcher, &network, job.hash, peer, &cancel, &mut last, &stage,
            );
            tokio::pin!(attempt);
            let metadata = loop {
                tokio::select! {
                    biased;
                    result = &mut lookup, if summary.is_none() => {
                        dht_active.store(false, Ordering::Relaxed);
                        if let Some(error) = result.fault { return Outcome::Control(error); }
                        summary = Some(result);
                    }
                    result = &mut attempt => break result,
                }
            };
            if let Some(metadata) = metadata {
                if summary.is_none() {
                    network.metrics.add(Counter::LookupCancelledSuccess, 1);
                }
                return Outcome::Success(metadata);
            }
        }
        // 没有再可尝试的地址时，已观察到的远端失败优先；纯本地等待不消耗 attempts。
        let reason = match last {
            Some(category) => RetryReason::Failed(category),
            None if summary
                .as_ref()
                .map_or_else(|| progress.sent.load(Ordering::Relaxed), |s| s.sent)
                > 0 =>
            {
                RetryReason::Failed("no_peers")
            }
            None if summary.as_ref().map_or_else(
                || progress.limited.load(Ordering::Relaxed),
                |s| s.local_limited || s.had_seeds,
            ) =>
            {
                RetryReason::Local(LocalReason::ResourceWait)
            }
            None => RetryReason::Local(LocalReason::NoRoute),
        };
        Outcome::Retry(reason)
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Outcome::Retry(RetryReason::Local(LocalReason::Cancelled)),
        result = tokio::time::timeout(Duration::from_secs(180), work) => {
            result.unwrap_or_else(|_| Outcome::Retry(if matches!(*stage.lock().expect("执行阶段锁"), ExecutionStage::LocalWait | ExecutionStage::Dht)
                && (!dht_active.load(Ordering::Relaxed) || progress.sent.load(Ordering::Relaxed) == 0) {
                RetryReason::Local(LocalReason::ResourceWait)
            } else { RetryReason::Failed("task_timeout") }))
        },
    }
}
async fn attempt(
    fetcher: &MetadataFetcher,
    network: &lookup::Network,
    hash: InfoHashV1,
    peer: SocketAddr,
    cancel: &CancellationToken,
    last: &mut Option<&'static str>,
    stage: &std::sync::Mutex<ExecutionStage>,
) -> Option<VerifiedMetadata> {
    *stage.lock().expect("执行阶段锁") = ExecutionStage::LocalWait;
    let wait = network.metrics.timer(Timing::TcpWait);
    let _connection = network.connect(peer.ip()).await;
    drop(wait);
    *stage.lock().expect("执行阶段锁") = ExecutionStage::Peer;
    match fetcher.fetch(hash, &[peer], cancel).await {
        Ok(metadata) => {
            *stage.lock().expect("执行阶段锁") = ExecutionStage::Validation;
            Some(metadata)
        }
        Err(error) => {
            if let MetadataError::AllPeersFailed(failures) = &error {
                for failure in failures {
                    tracing::debug!(address=%failure.address,stage=?failure.stage,error=%failure.error,"metadata 阶段失败");
                }
            }
            *last = Some(match &error {
                MetadataError::AllPeersFailed(errors) => match errors.last().map(|f| &f.error) {
                    Some(crate::metadata::PeerError::HashMismatch) => "hash_mismatch",
                    Some(crate::metadata::PeerError::Protocol(_)) => "protocol",
                    Some(crate::metadata::PeerError::Timeout(_)) => "peer_timeout",
                    Some(crate::metadata::PeerError::Unsupported) => "unsupported",
                    Some(crate::metadata::PeerError::Limit(_)) => "receive_limit",
                    Some(crate::metadata::PeerError::Rejected(_)) => "rejected",
                    _ => "peer_io",
                },
                MetadataError::TaskTimeout => "task_timeout",
                _ => "metadata_unavailable",
            });
            tracing::debug!(%peer,%error,category=*last,"metadata peer 获取失败");
            None
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
