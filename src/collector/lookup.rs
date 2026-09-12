//! 有界双栈查找；RPC 仍由对应 Dispatcher 发送。
//!
//! worker 在共享的查询间隔和同 IP TCP 排他规则下寻找 peer；许可随工作退出而释放。
use super::*;
use crate::dht::{
    dispatcher::{DiscoveredNode, QueryError, RemoteNode},
    routing::xor_distance,
    shortlist::{CandidateState, closest_valid},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::collections::{HashMap, HashSet};
use tokio::time::Instant;

#[derive(Default)]
struct Rate {
    next: Option<Instant>,
    ips: HashMap<std::net::IpAddr, Instant>,
}
#[derive(Default)]
pub(super) struct Network {
    pub(super) metrics: Arc<crate::metrics::Metrics>,
    rate: tokio::sync::Mutex<Rate>,
    tcp: Arc<TcpState>,
}
#[derive(Default)]
struct TcpState {
    ips: std::sync::Mutex<HashMap<std::net::IpAddr, std::sync::Weak<tokio::sync::Semaphore>>>,
}
struct TcpTicket {
    state: Arc<TcpState>,
    ip: std::net::IpAddr,
    semaphore: Arc<tokio::sync::Semaphore>,
}
impl Drop for TcpTicket {
    fn drop(&mut self) {
        let mut ips = self.state.ips.lock().expect("TCP 地址锁");
        if Arc::strong_count(&self.semaphore) == 1 {
            ips.remove(&self.ip);
        }
    }
}
pub(super) struct Connection {
    // 字段依次释放：先释放许可，再检查最后一个登记者。
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
    /// 只取得同 IP 的逻辑独占权，实际 TCP 连接由 MetadataFetcher 创建。
    /// 等待时取消不占用 IP；成功返回后由 Connection 的 Drop 释放并唤醒等待者。
    pub(super) async fn connect(&self, ip: std::net::IpAddr) -> Connection {
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
        let permit = ticket
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("TCP semaphore 不关闭");
        Connection {
            _permit: permit,
            _ticket: ticket,
        }
    }

    pub(super) fn connections(&self) -> usize {
        self.tcp.ips.lock().expect("TCP 地址锁").len()
    }
}

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
/// collector 自己的协议状态机；注入 RPC 便于验证查询上限、截止和取消。
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
    pub(super) sent: u64,
    pub(super) local_limited: bool,
    pub(super) fault: Option<QueryError>,
}

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

/// 两种地址族共用结果集与总期限；超时保留已经找到的 peer，取消会丢弃在途查找。
#[cfg(test)]
pub(super) async fn lookup(
    handles: &[DhtHandle],
    hash: InfoHashV1,
    network: Arc<Network>,
) -> LookupResult {
    stream(handles, hash, network, None, Arc::default()).await
}
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
        let first = network.connect(ip).await;
        let mut second = Box::pin(network.connect(ip));
        let mut cancelled = Box::pin(network.connect(ip));
        let mut last = Box::pin(network.connect(ip));
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
        assert_eq!(network.connections(), 0);
    }
}

#[cfg(test)]
mod search_tests {
    use super::*;
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
