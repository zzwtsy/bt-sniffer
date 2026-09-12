//! 有界双栈查找；RPC 仍由对应 Dispatcher 发送。
//!
//! Network 共享查询节奏与同 IP TCP 许可；query 等待查询间隔，worker 的 attempt 申请并持有许可。
//! 本模块不创建 TCP socket；取得许可后由 MetadataFetcher 驱动连接、握手与下载。
use crate::dht::{
    dispatcher::{DiscoveredNode, QueryError, RemoteNode},
    routing::xor_distance,
    shortlist::{CandidateState, closest_valid},
};
use crate::{dht::dispatcher::DhtHandle, krpc::InfoHashV1};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::collections::{HashMap, HashSet};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

/// 查询节奏预留：next 是所有 worker 的下一次机会，ips 是各目的 IP 的下一次机会。
#[derive(Default)]
struct Rate {
    next: Option<Instant>,
    ips: HashMap<std::net::IpAddr, Instant>,
}
/// collector 的 worker 共用查询节奏、TCP 许可表和指标，不代表已建立的网络连接。
#[derive(Default)]
pub(super) struct Network {
    pub(super) metrics: Arc<crate::metrics::Metrics>,
    rate: tokio::sync::Mutex<Rate>,
    tcp: Arc<TcpState>,
}
#[derive(Default)]
struct TcpState {
    /// 持有者和等待者共用每 IP 的信号量；弱引用不延长条目所指信号量的生命周期。
    ips: std::sync::Mutex<HashMap<std::net::IpAddr, std::sync::Weak<tokio::sync::Semaphore>>>,
}
/// 一次许可申请在 IP 表中的登记；等待期间由申请 future 持有，成功后移入许可对象。
struct TcpTicket {
    state: Arc<TcpState>,
    ip: std::net::IpAddr,
    semaphore: Arc<tokio::sync::Semaphore>,
}
impl Drop for TcpTicket {
    fn drop(&mut self) {
        let mut ips = self.state.ips.lock().expect("TCP 地址锁");
        // 检查和删除共用申请时的锁，避免删除期间另一个申请取得同一信号量。
        // 仅剩本登记的强引用时，已无其他持有者或等待者，可以移除弱引用条目。
        if Arc::strong_count(&self.semaphore) == 1 {
            ips.remove(&self.ip);
        }
    }
}
/// 同 IP 的单个 TCP 并发许可，不持有 socket。调用者在整个 peer 尝试期间保存它。
/// 离开作用域或持有它的 future 被丢弃时，字段自动析构，归还许可并清理登记。
pub(super) struct ConnectionPermit {
    // Rust 按字段声明顺序释放：先归还许可、释放它持有的信号量强引用，再检查登记。
    // 若交换顺序，最后一个 ticket 仍会看到许可的强引用，无法删除最后的 IP 表条目。
    _permit: tokio::sync::OwnedSemaphorePermit,
    _ticket: TcpTicket,
}
impl Network {
    pub(super) fn with_metrics(metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self {
            metrics,
            ..Self::default()
        }
    }
    /// 同时满足全局 100 ms 和同 IP 1 秒间隔才预留机会；等待时释放锁。
    /// 返回只获得节奏许可，后续实际发包还受 dispatcher 配额限制。
    async fn pace(&self, ip: std::net::IpAddr) {
        loop {
            let wait = {
                let mut rate = self.rate.lock().await;
                let now = Instant::now();
                rate.ips.retain(|_, until| *until > now);
                let due = rate
                    .next
                    .unwrap_or(now)
                    .max(rate.ips.get(&ip).copied().unwrap_or(now));
                if due <= now {
                    rate.next = Some(now + Duration::from_millis(100));
                    rate.ips.insert(ip, now + Duration::from_secs(1));
                    return;
                }
                due
            };
            tokio::time::sleep_until(wait).await;
        }
    }
    /// 申请同 IP 的单个并发许可；不同 IP 使用各自的信号量，不相互排队。
    /// worker 的 attempt 取得许可后才调用 MetadataFetcher，后者在 fetch_peer 中连接 TCP。
    /// 等待期间也登记在 IP 表中；丢弃申请 future 会退出排队并释放登记，失去原排队位置。
    /// 此方法不监听取消 token；外层须丢弃 future 才取消等待。成功后由返回对象归还许可。
    pub(super) async fn acquire_for_ip(&self, ip: std::net::IpAddr) -> ConnectionPermit {
        let ticket = {
            let mut ips = self.tcp.ips.lock().expect("TCP 地址锁");
            let semaphore = ips
                .get(&ip)
                .and_then(std::sync::Weak::upgrade)
                .unwrap_or_else(|| {
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
                    ips.insert(ip, Arc::downgrade(&semaphore));
                    semaphore
                });
            TcpTicket {
                state: self.tcp.clone(),
                ip,
                semaphore,
            }
        };
        // IP 表的同步锁已释放，等待只持有 ticket 和信号量，不跨 await 持表锁。
        let permit = ticket
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("TCP semaphore 不关闭");
        ConnectionPermit {
            _permit: permit,
            _ticket: ticket,
        }
    }

    /// 包含许可持有者或等待者的 IP 条目数，不是 socket 数；保留现有日志统计口径。
    pub(super) fn tracked_tcp_ips(&self) -> usize {
        self.tcp.ips.lock().expect("TCP 地址锁").len()
    }
}

/// 本地容量或配额等待可重试；其他结果交给查找状态机，外层总期限限制整个循环。
async fn query(
    handle: DhtHandle,
    node: DiscoveredNode,
    hash: InfoHashV1,
    network: Arc<Network>,
    progress: Arc<crate::dht::dispatcher::RpcProgress>,
) -> Result<crate::dht::dispatcher::GetPeersResponse, QueryError> {
    loop {
        network.pace(node.address.ip()).await;
        match handle
            .get_peers_observed(
                RemoteNode {
                    address: node.address,
                    expected_id: Some(node.id),
                },
                hash,
                progress.clone(),
            )
            .await
        {
            Err(QueryError::AtCapacity { .. } | QueryError::LocalWait) => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            result => return result,
        }
    }
}
async fn family(
    handle: DhtHandle,
    hash: InfoHashV1,
    network: Arc<Network>,
    found_seeds: Arc<AtomicBool>,
    progress: Arc<crate::dht::dispatcher::RpcProgress>,
    fault: Arc<std::sync::Mutex<Option<QueryError>>>,
    output: Arc<PeerOutput>,
) -> bool {
    let seeds = match handle.fetch_seeds(hash).await {
        Ok(seeds) => seeds,
        Err(error) => {
            *fault.lock().expect("查找故障锁") = Some(error);
            return false;
        }
    };
    if seeds.is_empty() {
        return false;
    }
    found_seeds.store(true, Ordering::Relaxed);
    let result = search(
        hash,
        seeds,
        output.peers.clone(),
        Some(output),
        move |node| {
            query(
                handle.clone(),
                node,
                hash,
                network.clone(),
                progress.clone(),
            )
        },
    )
    .await;
    if let Err(error) = result {
        *fault.lock().expect("查找故障锁") = Some(error);
    }
    true
}
/// 单地址族迭代查找；最多 3 条并发、32 次候选查询，候选表保留近邻，失败项释放 shortlist 名额。
/// peers 与另一地址族共享并去重，合计最多 32 个；结果通过 try_send 尽力通知，不等待慢消费者。
/// 普通远端失败继续尝试，dispatcher 关闭或 transaction 故障向上返回；总期限由 stream 施加。
async fn search<Q, F>(
    hash: InfoHashV1,
    seeds: Vec<DiscoveredNode>,
    peers: Arc<std::sync::Mutex<Vec<SocketAddr>>>,
    output: Option<Arc<PeerOutput>>,
    query_node: Q,
) -> Result<(), QueryError>
where
    Q: Fn(DiscoveredNode) -> F,
    F: std::future::Future<Output = Result<crate::dht::dispatcher::GetPeersResponse, QueryError>>,
{
    let mut candidates: HashMap<_, _> = seeds.into_iter().map(|n| (n.id, n)).collect();
    let mut states = HashMap::new();
    let mut addresses = HashSet::new();
    let mut pending = FuturesUnordered::new();
    let mut queries = 0;
    loop {
        if peers.lock().expect("peer 结果锁").len() >= 32 {
            break;
        }
        let closest = closest_valid(
            candidates.keys().map(|id| {
                (
                    *id,
                    states.get(id).copied().unwrap_or(CandidateState::Unqueried),
                )
            }),
            crate::krpc::NodeId(hash.0),
            8,
        );
        for id in closest {
            if pending.len() >= 3 || queries >= 32 {
                break;
            }
            if states.contains_key(&id) {
                continue;
            }
            let node = candidates[&id];
            if !addresses.insert(node.address) {
                states.insert(id, CandidateState::Failed);
                continue;
            }
            states.insert(id, CandidateState::InFlight);
            queries += 1;
            let response = query_node(node);
            pending.push(async move { (id, response.await) });
        }
        let Some((id, result)) = pending.next().await else {
            if queries < 32
                && closest_valid(
                    candidates.keys().map(|id| {
                        (
                            *id,
                            states.get(id).copied().unwrap_or(CandidateState::Unqueried),
                        )
                    }),
                    crate::krpc::NodeId(hash.0),
                    8,
                )
                .iter()
                .any(|id| !states.contains_key(id))
            {
                continue;
            }
            break;
        };
        if matches!(
            result,
            Err(QueryError::DispatcherClosed
                | QueryError::ShuttingDown
                | QueryError::Transaction(_))
        ) {
            return Err(result.expect_err("已确认控制故障"));
        }
        states.insert(
            id,
            if result.is_ok() {
                CandidateState::Succeeded
            } else {
                CandidateState::Failed
            },
        );
        if let Ok(response) = result {
            for node in response.nodes {
                candidates.entry(node.id).or_insert(node);
            }
            if candidates.len() > 256 {
                let mut nodes: Vec<_> = candidates.values().copied().collect();
                nodes.sort_by_key(|n| xor_distance(&n.id.0, &hash.0));
                candidates = nodes.into_iter().take(256).map(|n| (n.id, n)).collect();
            }
            let mut collected = peers.lock().expect("peer 结果锁");
            for peer in response.peers {
                if collected.len() < 32 && !collected.contains(&peer) {
                    if let Some(output) = &output {
                        if collected.is_empty() {
                            output
                                .metrics
                                .add(crate::metrics::Counter::LookupFirstPeer, 1);
                            output
                                .metrics
                                .observe(crate::metrics::Timing::FirstPeer, output.start.elapsed());
                        }
                        if let Some(sender) = &output.sender {
                            let _ = sender.try_send(peer);
                        }
                    }
                    collected.push(peer);
                }
            }
        }
    }
    Ok(())
}
/// peers 可能为空；had_seeds 记录是否曾取到非空路由种子，不代表网络查询成功。
pub(super) struct LookupResult {
    #[cfg(test)]
    pub(super) peers: Vec<SocketAddr>,
    pub(super) had_seeds: bool,
    /// 退出时已观察到的实际 RPC 发送数；0 不等于没有尝试申请本地资源。
    pub(super) sent: u64,
    /// 是否观察到本地限流，用于区别本地等待与远端失败，不能表示一直处于限流。
    pub(super) local_limited: bool,
    /// 需要上层处理的控制故障；即使此前已发现 peer，也不能把此故障吞掉。
    pub(super) fault: Option<QueryError>,
}

/// 双栈共享的去重结果与可选通知端；通知失败仍保留去重记录，不保证消费者收到每个地址。
struct PeerOutput {
    peers: Arc<std::sync::Mutex<Vec<SocketAddr>>>,
    sender: Option<tokio::sync::mpsc::Sender<SocketAddr>>,
    metrics: Arc<crate::metrics::Metrics>,
    start: Instant,
}

/// 取消查找也保留已经发生的 RPC 与结果统计。
struct LookupReport {
    metrics: Arc<crate::metrics::Metrics>,
    progress: Arc<crate::dht::dispatcher::RpcProgress>,
    peers: Arc<std::sync::Mutex<Vec<SocketAddr>>>,
}
impl Drop for LookupReport {
    fn drop(&mut self) {
        self.metrics
            .add(crate::metrics::Counter::LookupsFinished, 1);
        self.metrics.add(
            crate::metrics::Counter::RpcSent,
            self.progress.sent.load(Ordering::Relaxed),
        );
        if !self.peers.lock().expect("peer 结果锁").is_empty() {
            self.metrics
                .add(crate::metrics::Counter::LookupWithPeers, 1);
        }
    }
}

/// 测试用的非流式包装；查询、期限和取消契约与 stream 相同，另返回收集到的 peer。
#[cfg(test)]
pub(super) async fn lookup(
    handles: &[DhtHandle],
    hash: InfoHashV1,
    network: Arc<Network>,
) -> LookupResult {
    stream(handles, hash, network, None, Arc::default()).await
}
/// 同时驱动各地址族，合计等待最多 30 秒；sender 将新 peer 及时交给 worker 下载。
/// 正常结束或超时返回已观察的种子、发送和故障状态；超时不是独立错误分支。
/// 丢弃 future 会丢弃内部查找并通知 RPC 取消；dispatcher 稍后实际回收 transaction。
/// LookupReport 在退出时记录统计，不能把结束计数理解成查找成功。
pub(super) async fn stream(
    handles: &[DhtHandle],
    hash: InfoHashV1,
    network: Arc<Network>,
    sender: Option<tokio::sync::mpsc::Sender<SocketAddr>>,
    progress: Arc<crate::dht::dispatcher::RpcProgress>,
) -> LookupResult {
    network.metrics.add(crate::metrics::Counter::Lookups, 1);
    let _timer = network.metrics.timer(crate::metrics::Timing::Lookup);
    let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let found_seeds = Arc::new(AtomicBool::new(false));
    let output = Arc::new(PeerOutput {
        peers: peers.clone(),
        sender,
        metrics: network.metrics.clone(),
        start: Instant::now(),
    });
    let fault = Arc::new(std::sync::Mutex::new(None));
    let _report = LookupReport {
        metrics: network.metrics.clone(),
        progress: progress.clone(),
        peers: peers.clone(),
    };
    let mut families: FuturesUnordered<_> = handles
        .iter()
        .map(|h| {
            let (network, found_seeds) = (network.clone(), found_seeds.clone());
            let progress = progress.clone();
            let fault = fault.clone();
            let output = output.clone();
            async move {
                family(
                    h.clone(),
                    hash,
                    network,
                    found_seeds,
                    progress,
                    fault,
                    output,
                )
                .await;
            }
        })
        .collect();
    let work = async {
        while families.next().await.is_some() {
            if fault.lock().expect("查找故障锁").is_some() {
                break;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(30), work).await;
    drop(families);
    #[cfg(test)]
    let result = peers.lock().expect("peer 结果锁").clone();
    LookupResult {
        #[cfg(test)]
        peers: result,
        had_seeds: found_seeds.load(Ordering::Relaxed),
        sent: progress.sent.load(Ordering::Relaxed),
        local_limited: progress.limited.load(Ordering::Relaxed),
        fault: fault.lock().expect("查找故障锁").take(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 不同目标共享总发送间隔，同一 IP 的多个地址还须共用 IP 预算。
    #[tokio::test(start_paused = true)]
    async fn rate_budget_is_shared_across_destinations_and_per_ip() {
        let network = Network::default();
        let first = "127.0.0.1".parse().unwrap();
        let second = "127.0.0.2".parse().unwrap();
        let start = Instant::now();
        network.pace(first).await;
        network.pace(second).await;
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        network.pace(first).await;
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        assert!(network.rate.lock().await.ips.len() <= 2);
    }
}

#[cfg(test)]
mod fairness_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn fifo_waiters_cancel_and_release_without_leaking_ip_entries() {
        let network = Network::default();
        let ip = "127.0.0.1".parse().unwrap();
        let first = network.acquire_for_ip(ip).await;
        let mut second = Box::pin(network.acquire_for_ip(ip));
        let mut cancelled = Box::pin(network.acquire_for_ip(ip));
        let mut last = Box::pin(network.acquire_for_ip(ip));
        assert!(futures_util::poll!(&mut second).is_pending());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        assert!(futures_util::poll!(&mut last).is_pending());
        drop(cancelled);
        drop(first);
        assert!(futures_util::poll!(&mut last).is_pending());
        let second = second.await;
        assert!(futures_util::poll!(&mut last).is_pending());
        drop(second);
        drop(last.await);
        assert_eq!(network.tracked_tcp_ips(), 0);
    }
}

#[cfg(test)]
mod search_tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    #[tokio::test]
    async fn shared_stream_is_unique_and_bounded_without_consumer_progress() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(32);
        let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let output = Arc::new(PeerOutput {
            peers: peers.clone(),
            sender: Some(sender),
            metrics: Arc::default(),
            start: Instant::now(),
        });
        let work = || {
            search(
                InfoHashV1([0; 20]),
                nodes(8),
                peers.clone(),
                Some(output.clone()),
                |_| async {
                    Ok(crate::dht::dispatcher::GetPeersResponse {
                        nodes: vec![],
                        peers: (1..=64)
                            .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
                            .collect(),
                    })
                },
            )
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(work(), work())
        })
        .await
        .unwrap()
        .0
        .unwrap();
        assert_eq!(peers.lock().unwrap().len(), 32);
        let mut received = HashSet::new();
        while let Ok(peer) = receiver.try_recv() {
            assert!(received.insert(peer));
        }
        assert_eq!(received.len(), 32);
    }
    fn nodes(count: u8) -> Vec<DiscoveredNode> {
        (1..=count)
            .map(|n| DiscoveredNode {
                id: crate::krpc::NodeId([n; 20]),
                address: SocketAddr::from(([127, 0, 0, n], 6881)),
            })
            .collect()
    }
    #[tokio::test]
    async fn all_failures_stop_at_query_limit_and_successful_shortlist_converges() {
        for succeeds in [false, true] {
            let attempts = std::sync::Mutex::new(Vec::new());
            let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
            search(InfoHashV1([0; 20]), nodes(40), peers, None, |node| {
                attempts.lock().unwrap().push(node.id);
                async move {
                    if succeeds {
                        Ok(crate::dht::dispatcher::GetPeersResponse {
                            nodes: vec![node],
                            peers: vec![],
                        })
                    } else {
                        Err(QueryError::Timeout)
                    }
                }
            })
            .await
            .unwrap();
            let attempts = attempts.into_inner().unwrap();
            assert_eq!(attempts.len(), if succeeds { 8 } else { 32 });
            assert_eq!(
                attempts.iter().collect::<HashSet<_>>().len(),
                attempts.len()
            );
        }
    }
    #[tokio::test]
    async fn duplicate_addresses_do_not_hide_reserve_or_repeat_rpc() {
        let mut seeds = nodes(12);
        for index in 1..8 {
            seeds[index].address = seeds[0].address;
        }
        let attempts = std::sync::Mutex::new(Vec::new());
        search(InfoHashV1([0; 20]), seeds, Arc::default(), None, |node| {
            attempts.lock().unwrap().push(node.address);
            async { Err(QueryError::Timeout) }
        })
        .await
        .unwrap();
        assert_eq!(attempts.lock().unwrap().len(), 5);
    }
    #[tokio::test(start_paused = true)]
    async fn deadline_retains_peers_and_drops_remaining_queries() {
        struct InFlight(Arc<AtomicU64>);
        impl Drop for InFlight {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Relaxed);
            }
        }
        let active = Arc::new(AtomicU64::new(0));
        let peers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let expected = "127.0.0.1:9999".parse().unwrap();
        let work = search(InfoHashV1([0; 20]), nodes(8), peers.clone(), None, |node| {
            let active = active.clone();
            async move {
                active.fetch_add(1, Ordering::Relaxed);
                let _guard = InFlight(active);
                if node.id.0[0] == 1 {
                    return Ok(crate::dht::dispatcher::GetPeersResponse {
                        nodes: vec![],
                        peers: vec![expected],
                    });
                }
                std::future::pending().await
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(30), work)
                .await
                .is_err()
        );
        assert_eq!(*peers.lock().unwrap(), vec![expected]);
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }
}
