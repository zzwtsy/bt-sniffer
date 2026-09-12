//! 自动采集所需的有类型 RPC 与非阻塞宣布入口。
//!
//! collector 经控制句柄查找 peer；dispatcher 持有对应 transaction，取消后负责撤销登记。
use super::{api::*, runtime::DhtDispatcher};
use crate::krpc::{CompactPeerAddress, InfoHashV1, NodeId, QueryMethod, ResponseArgs};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::SystemTime,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// 已通过协议校验的宣布及其观察时间；到达入口不代表数据库已接纳或提交。
#[derive(Debug, Clone)]
pub(crate) struct AnnounceEvent {
    pub(crate) hash: InfoHashV1,
    pub(crate) peer: SocketAddr,
    pub(crate) observed_at: SystemTime,
}
/// collector 拥有接收端，节点共享此发送入口；暂停/满载/关闭时尽力丢弃并计数。
#[derive(Debug, Clone)]
pub(crate) struct FetchIngress {
    pub(crate) sender: mpsc::Sender<AnnounceEvent>,
    pub(crate) paused: Arc<AtomicBool>,
    pub(crate) dropped: Arc<AtomicU64>,
    pub(crate) observed: Arc<AtomicU64>,
}
impl FetchIngress {
    /// observed 先计入所有进入此处的事件；丢弃不阻塞 UDP 循环，也不撤销协议 ACK。
    pub(super) fn announce(&self, event: AnnounceEvent) {
        self.observed.fetch_add(1, Ordering::Relaxed);
        if self.paused.load(Ordering::Relaxed) || self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
/// 经过响应校验的候选节点与 peer 地址；地址并不保证可达，实际下载仍需握手和校验。
#[derive(Debug)]
pub(crate) struct GetPeersResponse {
    pub(crate) nodes: Vec<DiscoveredNode>,
    pub(crate) peers: Vec<SocketAddr>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Outbound {
    Ping,
    FindNode(NodeId),
    Sample(NodeId),
    GetPeers(InfoHashV1),
}
impl Outbound {
    pub(super) fn fields(self) -> (QueryMethod, Option<NodeId>, Option<InfoHashV1>) {
        match self {
            Self::Ping => (QueryMethod::Ping, None, None),
            Self::FindNode(id) => (QueryMethod::FindNode, Some(id), None),
            Self::Sample(id) => (QueryMethod::SampleInfohashes, Some(id), None),
            Self::GetPeers(hash) => (QueryMethod::GetPeers, None, Some(hash)),
        }
    }
}
impl DhtHandle {
    #[cfg(test)]
    pub(crate) async fn get_peers(
        &self,
        remote: RemoteNode,
        hash: InfoHashV1,
    ) -> Result<GetPeersResponse, QueryError> {
        self.get_peers_observed(remote, hash, Arc::new(RpcProgress::default()))
            .await
    }
    /// 提交一次 get_peers 并等待结果；progress 观察实际发送和本地等待，不拥有 transaction。
    /// 丢弃 future 触发取消，queued/pending 登记由 dispatcher 清理；不能把取消通知当回收完成。
    pub(crate) async fn get_peers_observed(
        &self,
        remote: RemoteNode,
        hash: InfoHashV1,
        progress: Arc<RpcProgress>,
    ) -> Result<GetPeersResponse, QueryError> {
        let cancel = CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::GetPeers {
                remote,
                hash,
                progress,
                cancel,
                reply,
            })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)?
    }
    /// 从当前路由表读取查找种子；空集合是无可用种子，不是网络查询返回空结果。
    pub(crate) async fn fetch_seeds(
        &self,
        hash: InfoHashV1,
    ) -> Result<Vec<DiscoveredNode>, QueryError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::FetchSeeds { hash, reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)
    }
    /// 安装或撤销宣布入口；返回表示 dispatcher 已处理命令，不表示先前宣布已持久化。
    pub(crate) async fn fetch_ingress(
        &self,
        ingress: Option<FetchIngress>,
    ) -> Result<(), QueryError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::FetchIngress { ingress, reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)
    }
    /// 暂停主动采样调度；不等同于停止 dispatcher，也不直接关闭宣布接纳。
    pub(crate) async fn pause_sampling(&self, paused: bool) -> Result<(), QueryError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::SamplingPause { paused, reply })
            .await
            .map_err(|_| QueryError::DispatcherClosed)?;
        result.await.map_err(|_| QueryError::DispatcherClosed)
    }
}
/// 等待任一 RPC 取消；没有 token 时永久 pending，避免事件循环空转。
pub(super) async fn wait_cancelled(tokens: Vec<CancellationToken>) {
    if tokens.is_empty() {
        std::future::pending::<()>().await;
    }
    let mut waits: FuturesUnordered<_> = tokens
        .into_iter()
        .map(|t| async move { t.cancelled().await })
        .collect();
    waits.next().await;
}
impl DhtDispatcher {
    pub(super) fn cancel_fetch_queries(&mut self) {
        self.queued.retain(|q| !q.purpose.cancelled());
        // 先收集需要撤销的 ID，再同时移除协议登记与业务等待者。
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, pending)| pending.purpose.cancelled().then_some(*id))
            .collect();
        for id in ids {
            self.transactions.cancel(id);
            if let Some(pending) = self.pending.remove(&id) {
                self.budget.inflight_cancelled(pending.purpose.class());
            }
        }
    }
    pub(super) fn decode_peers(
        &self,
        response: &ResponseArgs,
    ) -> Result<GetPeersResponse, QueryError> {
        if response
            .token
            .as_ref()
            .is_none_or(|t| t.0.is_empty() || t.0.len() > 256)
        {
            return Err(QueryError::InvalidResponse("get_peers 缺少有效 token"));
        }
        if response.values.is_none() && response.nodes.is_none() && response.nodes6.is_none() {
            return Err(QueryError::InvalidResponse("get_peers 缺少 peers/nodes"));
        }
        let mut nodes = Vec::new();
        if let Some(values) = &response.nodes {
            nodes.extend(values.0.iter().map(|n| DiscoveredNode {
                id: n.id,
                address: SocketAddr::V4(n.address),
            }));
        }
        if let Some(values) = &response.nodes6 {
            nodes.extend(values.0.iter().map(|n| DiscoveredNode {
                id: n.id,
                address: SocketAddr::V6(n.address),
            }));
        }
        let mut seen = HashSet::new();
        nodes.retain(|n| {
            self.routing.address_family().accepts(n.address)
                && self.automatic_policy.accepts(n.address)
                && n.id != self.routing.local_id()
                && seen.insert((n.id, n.address))
        });
        nodes.truncate(256);
        let mut seen = HashSet::new();
        let peers = response
            .values
            .iter()
            .flatten()
            .map(|p| match p {
                CompactPeerAddress::V4(a) => SocketAddr::V4(*a),
                CompactPeerAddress::V6(a) => SocketAddr::V6(*a),
            })
            .filter(|a| self.automatic_policy.accepts(*a) && seen.insert(*a))
            .take(32)
            .collect();
        Ok(GetPeersResponse { nodes, peers })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// 宣布通知队列满时按约定丢弃并计数，不能阻塞 UDP 事件循环。
    #[test]
    fn announce_backpressure_never_blocks_and_counts_drops() {
        let (sender, mut receiver) = mpsc::channel(1);
        let ingress = FetchIngress {
            sender,
            paused: Arc::new(AtomicBool::new(false)),
            observed: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
        };
        let event = AnnounceEvent {
            hash: InfoHashV1([1; 20]),
            peer: "127.0.0.1:1".parse().unwrap(),
            observed_at: SystemTime::now(),
        };
        ingress.announce(event.clone());
        ingress.announce(event.clone());
        assert_eq!(ingress.dropped.load(Ordering::Relaxed), 1);
        receiver.try_recv().unwrap();
        ingress.paused.store(true, Ordering::Relaxed);
        ingress.announce(event);
        assert!(receiver.try_recv().is_err());
        assert_eq!(ingress.observed.load(Ordering::Relaxed), 3);
        assert_eq!(ingress.dropped.load(Ordering::Relaxed), 2);
    }
}
