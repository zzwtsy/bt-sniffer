//! 协调持久化任务：接纳发现 → 领取任务 → 网络获取 → 保存结果。
//!
//! SQLite 是任务状态的唯一事实来源；Workers 持有网络任务及其领取记录。
//! run_inner 负责正常调度，supervise 接收故障，finish 回收任务并保存可提交的结果。
//! 取消通知只要求网络任务停止，收尾仍需等待任务并处理每条领取记录。
//!
//! 入口由 app::session 创建并监督；worker 执行单任务，lookup 负责查找，lifecycle 配对退出与领取记录。
pub(crate) mod backpressure;
mod candidate_selector;
pub(crate) mod catalog;
#[cfg(test)]
mod comparison;
pub(crate) mod diagnostics;
mod failure;
pub(crate) mod ingest;
pub(crate) mod inspection;
pub(crate) mod jobs;
mod lifecycle;
mod lookup;
mod metadata_store;
pub(crate) mod metainfo;
pub(crate) mod peer;
mod records;
mod scheduler;
mod status;
pub(crate) mod store;
mod tcp_limits;
mod worker;
use crate::collection::diagnostics::AttemptContext;
use crate::collection::diagnostics::metrics::Counter;
use crate::collection::diagnostics::metrics::Metrics;
use crate::collection::peer::VerifiedMetadata;
use crate::dht::dispatcher::QueryError;
pub(crate) use backpressure::Mode as SampleBackpressure;
pub(crate) use lifecycle::CollectorError;
use lifecycle::Workers;
use tokio::time::Instant;
pub(crate) use worker::MAX_PEER_ATTEMPTS;
use worker::{Outcome, WorkerResources};
#[cfg(test)]
mod tests;
use crate::address::AddressPolicy;
use crate::clock::Clock;
#[cfg(test)]
use crate::clock::unix_millis;
use crate::collection::jobs::Job;
use crate::collection::jobs::LocalReason;
use crate::collection::jobs::RetryReason;
use crate::collection::jobs::UpdateResult;
use crate::collection::peer::MetadataConfig;
use crate::collection::peer::PeerClient;
use crate::collection::store::CollectionStore;
use crate::dht::dispatcher::AnnounceEvent;
use crate::dht::dispatcher::DhtHandle;
use crate::dht::dispatcher::FetchIngress;
use crate::storage::StorageError;
use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
#[cfg(test)]
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

/// 应用交给协调器的资源边界；只控制采集与接纳，不改变 DHT 的协议配额。
#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) metadata: MetadataConfig,
    /// 同时领取并运行的 worker 上限；peer 客户端不另设全局并发名额。
    pub(crate) concurrency: usize,
    /// pending、running、retry_wait 合计的接纳上限，不限制历史成功记录数。
    pub(crate) max_active: usize,
    pub(crate) sample_backpressure: SampleBackpressure,
    /// 状态目录软字节预算；接近上限时保留清理空间并暂停新增工作。
    pub(crate) state_max_bytes: u64,
    pub(crate) directory: PathBuf,
    pub(crate) policy: AddressPolicy,
}
impl Config {
    /// 并发属于领取调度器；在启用接纳或恢复数据库之前拒绝无效值。
    fn validate(&self) -> Result<(), peer::PeerInitError> {
        if self.concurrency == 0 || self.concurrency > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(peer::PeerInitError::InvalidConfig);
        }
        Ok(())
    }
}
/// 会话创建并监督的协调器；持有宣布接收端、任务调度状态与共享指标。
/// 数据库 handle 只提交命令，数据库的最终关闭责任仍属于会话。
pub(crate) struct Collector {
    peer: PeerClient,
    metrics: Arc<Metrics>,
    backpressure: backpressure::Backpressure,
    store: CollectionStore,
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
    failure_categories: BTreeMap<&'static str, u64>,
}

impl Collector {
    /// 启用任务接纳、恢复磁盘任务并向节点安装宣布入口，尚不运行 worker。
    /// 中途失败可能已有数据库更新或部分入口安装；调用者须关闭已创建的会话资源。
    pub(crate) async fn new(
        store: CollectionStore,
        handles: Vec<DhtHandle>,
        config: Config,
        clock: Clock,
        stop: CancellationToken,
        pause_notifications: watch::Sender<()>,
        report_error: Box<dyn Fn(&CollectorError) + Send + Sync>,
    ) -> Result<Self, CollectorError> {
        config.validate().map_err(CollectorError::Configuration)?;
        let metrics = Arc::new(Metrics::default());
        let peer = PeerClient::with_resources(config.metadata.clone(), metrics.clone())
            .map_err(CollectorError::Configuration)?
            .with_observer(store.observer.clone());
        store.enable_fetch(config.max_active);
        store.enable_recent_admission(
            if config.sample_backpressure == SampleBackpressure::Freshness {
                config.max_active.min(config.concurrency.saturating_mul(4))
            } else {
                0
            },
            config.policy,
        );
        store
            .recover_jobs(
                clock
                    .millis_at(Instant::now().into_std())
                    .map_err(|error| CollectorError::Clock(error.into()))?,
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
            metrics,
            peer,
            backpressure: backpressure::Backpressure::new(
                config.sample_backpressure,
                config.max_active,
                config.concurrency,
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
            .millis_at(Instant::now().into_std())
            .map_err(|error| CollectorError::Clock(error.into()))
    }
    fn record_error(&self, errors: &mut Vec<CollectorError>, error: CollectorError) {
        (self.report_error)(&error);
        errors.push(error);
    }
    /// 持续调度直到停止或故障，随后回收 worker、保存可提交结果并输出最终统计。
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
                if self.commit_metadata(job, metadata).await? == UpdateResult::Applied {
                    totals.succeeded += 1;
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

    /// 成功结果的唯一提交入口；事务确认 Applied 后才累计收益，关闭与运行共用。
    async fn commit_metadata(
        &self,
        job: Job,
        metadata: VerifiedMetadata,
    ) -> Result<UpdateResult, CollectorError> {
        let attempt = AttemptContext::from_job(&job);
        let bytes = metadata.info().len() as u64;
        let compatible = metadata.used_extension_compatibility();
        let result = self.store.complete_job(job, metadata, self.now()?).await?;
        if result == UpdateResult::Applied {
            self.metrics.diagnostics.committed(attempt);
            if compatible {
                self.metrics.diagnostics.compatibility_committed();
            }
            self.metrics.add(Counter::MetadataCommitted, 1);
            self.metrics.add(Counter::MetadataBytes, bytes);
        }
        Ok(result)
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
                self.commit_metadata(job, metadata).await?;
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
        result: Result<(), QueryError>,
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
            match fs::metadata(directory.join(name)) {
                Ok(meta) => bytes = bytes.saturating_add(meta.len()),
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(StorageError::from(error)),
            }
        }
        Ok(bytes)
    })
    .await
    .map_err(CollectorError::InspectionTask)?
    .map_err(CollectorError::Storage)
}

#[cfg(test)]
pub(crate) mod test_storage;

#[cfg(test)]
mod worker_tests;

#[cfg(test)]
mod store_tests;
