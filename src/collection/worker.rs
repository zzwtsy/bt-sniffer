//! 执行一个已领取任务：选择候选 → 交替推进查找与下载 → 返回结果或重试原因。
//!
//! run_job 持有查找 future、当前 peer 的 attempt、阶段与进度；不写数据库、不另起查找任务。
//! 先看候选选择，再看两段 select：等待候选时推进查找，下载期间继续推进同一个查找。
//! 外层取消或总超时丢弃整段工作，连同查询和连接许可一起收尾；结果由协调器提交。
use super::lookup;
use crate::address::AddressPolicy;
use crate::collection::diagnostics::Source;
use crate::collection::diagnostics::metrics::Counter;
use crate::collection::diagnostics::metrics::Timing;
use crate::collection::failure::AttemptFailure;
use crate::collection::jobs::Job;
use crate::collection::jobs::LocalReason;
use crate::collection::jobs::RetryReason;
use crate::collection::peer::PeerClient;
use crate::collection::peer::PeerError;
use crate::collection::peer::PeerFetchError;
use crate::collection::peer::VerifiedMetadata;
use crate::dht::dispatcher::DhtHandle;
use crate::info_hash::InfoHashV1;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// 协调器一次组装的具体执行资源；克隆 Arc 共享许可、查询节奏、Peer ID 和指标。
pub(super) struct WorkerResources {
    pub(super) observer: crate::observation::Observer,
    pub(super) peer: PeerClient,
    pub(super) lookup: Arc<lookup::LookupPacer>,
    pub(super) tcp: super::tcp_limits::TcpLimits,
    pub(super) metrics: Arc<super::diagnostics::metrics::Metrics>,
}
impl WorkerResources {
    pub(super) fn new(
        peer: PeerClient,
        metrics: Arc<super::diagnostics::metrics::Metrics>,
    ) -> Self {
        Self {
            observer: peer.observer.clone(),
            peer,
            metrics,
            lookup: Arc::default(),
            tcp: Default::default(),
        }
    }
}
/// 一轮领取最多尝试的不同候选数；日志与执行共用此常量。
pub(crate) const MAX_PEER_ATTEMPTS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionStage {
    LocalWait,
    Dht,
    Peer,
    Validation,
}
/// 网络执行结果；成功尚未落库，控制故障须交监督者处理，重试原因保留本地与远端区别。
pub(super) enum Outcome {
    Control(crate::dht::dispatcher::QueryError),
    Success(VerifiedMetadata),
    Retry(RetryReason),
}
/// 持有一次领取的网络工作；取消和总期限覆盖本地等待、查找及下载，不改变领取状态。
pub(super) async fn run_job(
    job: Job,
    handles: Vec<DhtHandle>,
    resources: Arc<WorkerResources>,
    policy: AddressPolicy,
    families: Vec<crate::dht::routing::AddressFamily>,
    cancel: CancellationToken,
) -> Outcome {
    let observer = resources.observer.for_job(&job.hash.0, job.generation);
    let mut execution = observer.span(crate::observation::Kind::Job, "execution");
    let task_timeout = Arc::new(AtomicBool::new(false));
    let stage = std::sync::Mutex::new(ExecutionStage::LocalWait);
    let progress = Arc::new(crate::dht::dispatcher::RpcProgress {
        observer: execution.observer.clone(),
        ..Default::default()
    });
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
            resources.lookup.clone(),
            resources.metrics.clone(),
            Some(sender),
            progress.clone(),
        ));
        let mut summary = None;
        while tried.len() < MAX_PEER_ATTEMPTS {
            let mut next = first_hints
                .pop_front()
                .map(|p| (p, Source::Announce))
                .or_else(|| peers.try_recv().ok().map(|p| (p, Source::Dht)))
                .or_else(|| hints.pop_front().map(|p| (p, Source::Announce)));
            while next.is_none() && summary.is_none() {
                *stage.lock().expect("执行阶段锁") = ExecutionStage::Dht;
                tokio::select! {
                    biased;
                    result = &mut lookup => {
                        dht_active.store(false, Ordering::Relaxed);
                        if let Some(error) = result.fault { return Outcome::Control(error); }
                        summary = Some(result);
                        next = peers.try_recv().ok().map(|p|(p,Source::Dht));
                    }
                    peer = peers.recv() => { next = peer.map(|p|(p,Source::Dht)); }
                }
            }
            let Some((peer, source)) = next else {
                break;
            };
            let rejected = if !policy.accepts(peer) {
                Some("address_policy")
            } else if !families.iter().any(|f| f.accepts(peer)) {
                Some("address_family")
            } else if !tried.insert(peer) {
                Some("duplicate")
            } else {
                None
            };
            if let Some(reason) = rejected {
                observer.emit(
                    crate::observation::Kind::Peer,
                    "candidate",
                    reason,
                    || serde_json::json!({"peer":peer.to_string()}),
                );
                continue;
            }
            let peer_observer = observer.child(crate::observation::Kind::Peer);
            peer_observer.emit(crate::observation::Kind::Peer,"candidate","selected",||serde_json::json!({"peer":peer.to_string(),"source":if source==Source::Announce{"announce"}else{"dht"}}));
            let context = super::peer::PeerContext {
                observer: peer_observer,
                source,
                attempt: Some(job.attempt_kind()),
                outer_timeout: task_timeout.clone(),
            };
            let attempt = attempt(&resources, job.hash, peer, &cancel, &stage, context);
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
            if matches!(metadata, Err(RetryReason::Local(LocalReason::Cancelled))) {
                return Outcome::Retry(RetryReason::Local(LocalReason::Cancelled));
            }
            if let Ok(metadata) = metadata {
                if summary.is_none() {
                    resources.metrics.add(Counter::LookupCancelledSuccess, 1);
                }
                return Outcome::Success(metadata);
            } else {
                last = metadata.err();
            }
        }
        // 没有再可尝试的地址时，已观察到的远端失败优先；纯本地等待不消耗 attempts。
        let reason = match last {
            Some(reason) => reason,
            None if summary
                .as_ref()
                .map_or_else(|| progress.sent.load(Ordering::Relaxed), |s| s.sent)
                > 0 =>
            {
                RetryReason::Failed(AttemptFailure::NoPeers)
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
    // timeout 借用 work，记录真实触发原因后再由作用域回收网络 future。
    tokio::pin!(work);
    let outcome = tokio::select! {
        biased;
        _ = cancel.cancelled() => Outcome::Retry(RetryReason::Local(LocalReason::Cancelled)),
        result = tokio::time::timeout(Duration::from_secs(180), &mut work) => {
            result.unwrap_or_else(|_| {
                task_timeout.store(true, Ordering::Relaxed);
                let locally_waiting = matches!(
                    *stage.lock().expect("执行阶段锁"),
                    ExecutionStage::LocalWait | ExecutionStage::Dht
                ) && (!dht_active.load(Ordering::Relaxed)
                    || progress.sent.load(Ordering::Relaxed) == 0);
                Outcome::Retry(if locally_waiting {
                    RetryReason::Local(LocalReason::ResourceWait)
                } else {
                    RetryReason::Failed(crate::collection::failure::AttemptFailure::TaskTimeout)
                })
            })
        },
    };
    execution.finish(match &outcome {
        Outcome::Success(_) => "downloaded",
        Outcome::Control(_) => "control_failure",
        Outcome::Retry(reason) => reason.category().unwrap_or("retry"),
    });
    outcome
}
/// 持有同 IP 许可完成一个 peer 尝试；失败返回固定类别，成功返回已校验的 metadata。
async fn attempt(
    resources: &WorkerResources,
    hash: InfoHashV1,
    peer: SocketAddr,
    cancel: &CancellationToken,
    stage: &std::sync::Mutex<ExecutionStage>,
    context: super::peer::PeerContext,
) -> Result<VerifiedMetadata, RetryReason> {
    *stage.lock().expect("执行阶段锁") = ExecutionStage::LocalWait;
    let mut wait_span = context
        .observer
        .span(crate::observation::Kind::Peer, "tcp_permit");
    let wait = resources.metrics.timer(Timing::TcpWait);
    // 这里只等同 IP 许可；guard 保留到本次尝试结束，真正连接由下方 fetch 驱动。
    // run_job 的取消或总超时会丢弃 attempt，等待中的申请或已取得的许可随之释放。
    let _connection_permit = resources.tcp.acquire_for_ip(peer.ip()).await;
    drop(wait);
    wait_span.finish("acquired");
    *stage.lock().expect("执行阶段锁") = ExecutionStage::Peer;
    match resources.peer.fetch_one(hash, peer, cancel, context).await {
        Ok(metadata) => {
            *stage.lock().expect("执行阶段锁") = ExecutionStage::Validation;
            Ok(metadata)
        }
        Err(PeerFetchError::Cancelled) => Err(RetryReason::Local(LocalReason::Cancelled)),
        Err(error) => {
            let (peer, stage) = match &error {
                PeerFetchError::PeerFailed(failure) => (failure.address, Some(failure.stage)),
                _ => (peer, None),
            };
            let category = peer_retry_reason(&error);
            tracing::debug!(
                event = "peer_fetch_failed",
                schema_version = 1u64,
                %peer,
                ?stage,
                %error,
                category = category.failure_category().expect("取消以外均为固定失败类别"),
                "metadata peer 获取失败"
            );
            Err(category)
        }
    }
}
/// 将一次 peer 调用结果映射为重试原因；取消属于本地延期，其他失败保留原固定类别。
fn peer_retry_reason(error: &PeerFetchError) -> RetryReason {
    let failure = match error {
        PeerFetchError::PeerFailed(failure) => match &failure.error {
            PeerError::HashMismatch => AttemptFailure::HashMismatch,
            PeerError::Protocol(_) | PeerError::HandshakeHashMismatch => AttemptFailure::Protocol,
            PeerError::Timeout(_) => AttemptFailure::PeerTimeout,
            PeerError::Unsupported => AttemptFailure::Unsupported,
            PeerError::Limit(_) => AttemptFailure::ReceiveLimit,
            PeerError::Rejected(_) => AttemptFailure::Rejected,
            PeerError::Io(_) | PeerError::Disconnected => AttemptFailure::PeerIo,
        },
        PeerFetchError::TaskTimeout => AttemptFailure::TaskTimeout,
        PeerFetchError::NoUsablePeers => AttemptFailure::MetadataUnavailable,
        PeerFetchError::Cancelled => return RetryReason::Local(LocalReason::Cancelled),
    };
    RetryReason::Failed(failure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::peer::MetadataConfig;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tracing::instrument::WithSubscriber;

    /// 通过真实失败握手验证默认模块来源，错误只输出一次且字段完整。
    #[tokio::test]
    async fn failed_peer_logs_use_module_target_and_fields() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let logs = tempfile::NamedTempFile::new().unwrap();
        let writer = logs.reopen().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            // 按当前生产模块过滤，不依赖历史模块别名。
            .with_env_filter("off,bt_sniffer::collection::worker=debug")
            .with_writer(move || writer.try_clone().unwrap())
            .finish();
        let fetcher = PeerClient::new(MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap();
        let resources = WorkerResources::new(fetcher.clone(), fetcher.test_metrics());
        let cancel = CancellationToken::new();
        let stage = std::sync::Mutex::new(ExecutionStage::LocalWait);
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            // 完整但非法的握手，稳定触发协议错误，不依赖远端服务或连接重置时机。
            socket.write_all(&[0; 68]).await.unwrap();
        };
        // subscriber 只随客户端 future 的 poll 生效，不跨 await 持有线程局部默认 guard。
        let client = attempt(
            &resources,
            InfoHashV1([1; 20]),
            peer,
            &cancel,
            &stage,
            super::super::peer::PeerContext::default(),
        )
        .with_subscriber(subscriber);
        let (_, metadata) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(server, client)
        })
        .await
        .unwrap();
        assert!(matches!(
            metadata,
            Err(RetryReason::Failed(
                crate::collection::failure::AttemptFailure::Protocol
            ))
        ));
        assert_eq!(resources.tcp.tracked_tcp_ips(), 0);
        let text = std::fs::read_to_string(logs.path()).unwrap();
        let events: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 1);
        for event in &events {
            assert_eq!(event["target"], "bt_sniffer::collection::worker");
            assert_eq!(event["level"], "DEBUG");
            assert!(event["fields"]["error"].is_string());
        }
        assert_eq!(events[0]["fields"]["stage"], "Some(StandardHandshake)");
        assert_eq!(events[0]["fields"]["message"], "metadata peer 获取失败");
        assert_eq!(events[0]["fields"]["peer"], peer.to_string());
        assert_eq!(events[0]["fields"]["category"], "protocol");
    }
}
