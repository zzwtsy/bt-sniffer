//! 执行一个已领取任务：选择候选 → 交替推进查找与下载 → 返回结果或重试原因。
//!
//! run_job 持有查找 future、当前 peer 的 attempt、阶段与进度；不写数据库、不另起查找任务。
//! 先看候选选择，再看两段 select：等待候选时推进查找，下载期间继续推进同一个查找。
//! 外层取消或总超时丢弃整段工作，连同查询和连接许可一起收尾；结果由协调器提交。
use super::lookup;
use crate::{
    dht::dispatcher::DhtHandle,
    krpc::InfoHashV1,
    metadata::{MetadataError, MetadataFetcher, VerifiedMetadata},
    metrics::{Counter, Timing},
    net::address::AddressPolicy,
    storage::jobs::{Job, LocalReason, RetryReason},
};
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
/// 持有同 IP 许可完成一个 peer 尝试；失败更新最后分类，成功返回已校验的 metadata。
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
    // 这里只等同 IP 许可；guard 保留到本次尝试结束，真正连接由下方 fetch 驱动。
    // run_job 的取消或总超时会丢弃 attempt，等待中的申请或已取得的许可随之释放。
    let _connection_permit = network.acquire_for_ip(peer.ip()).await;
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
                    tracing::debug!(target: "bt_sniffer::collector", address=%failure.address,stage=?failure.stage,error=%failure.error,"metadata 阶段失败");
                }
            }
            *last = Some(metadata_failure_category(&error));
            tracing::debug!(target: "bt_sniffer::collector", %peer,%error,category=*last,"metadata peer 获取失败");
            None
        }
    }
}
/// 将一次 peer 获取失败映射为既有重试类别；只读取最后一次 peer 失败，不记录或更新状态。
fn metadata_failure_category(error: &MetadataError) -> &'static str {
    match error {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MetadataConfig;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tracing::instrument::WithSubscriber;

    /// 通过真实失败握手覆盖两条 attempt 日志；模块移动不能改变旧 target 或业务字段。
    #[tokio::test]
    async fn failed_peer_logs_keep_collector_target_and_fields() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let logs = tempfile::NamedTempFile::new().unwrap();
        let writer = logs.reopen().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
            // 故意关闭新模块 target，确保仍能按旧 collector target 获取事件。
            .with_env_filter("off,bt_sniffer::collector=debug,bt_sniffer::collector::worker=off")
            .with_writer(move || writer.try_clone().unwrap())
            .finish();
        let fetcher = MetadataFetcher::new(MetadataConfig {
            address_policy: AddressPolicy::LocalUnicast,
            ..Default::default()
        })
        .unwrap();
        let network = lookup::Network::default();
        let cancel = CancellationToken::new();
        let stage = std::sync::Mutex::new(ExecutionStage::LocalWait);
        let mut last = None;
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            // 完整但非法的握手，稳定触发协议错误，不依赖远端服务或连接重置时机。
            socket.write_all(&[0; 68]).await.unwrap();
        };
        // subscriber 只随客户端 future 的 poll 生效，不跨 await 持有线程局部默认 guard。
        let client = attempt(
            &fetcher,
            &network,
            InfoHashV1([1; 20]),
            peer,
            &cancel,
            &mut last,
            &stage,
        )
        .with_subscriber(subscriber);
        let (_, metadata) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(server, client)
        })
        .await
        .unwrap();
        assert!(metadata.is_none());
        assert_eq!(last, Some("protocol"));
        assert_eq!(network.tracked_tcp_ips(), 0);
        let text = std::fs::read_to_string(logs.path()).unwrap();
        let events: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 2);
        for event in &events {
            assert_eq!(event["target"], "bt_sniffer::collector");
            assert_eq!(event["level"], "DEBUG");
            assert!(event["fields"]["error"].is_string());
        }
        assert_eq!(events[0]["fields"]["message"], "metadata 阶段失败");
        assert_eq!(events[0]["fields"]["address"], peer.to_string());
        assert_eq!(events[0]["fields"]["stage"], "Handshake");
        assert_eq!(events[1]["fields"]["message"], "metadata peer 获取失败");
        assert_eq!(events[1]["fields"]["peer"], peer.to_string());
        assert_eq!(events[1]["fields"]["category"], "protocol");
    }
}
