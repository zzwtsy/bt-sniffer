//! Dispatcher 状态、事件循环以及主动查询的启动过程。
//!
//! 事件循环交替处理网络、控制命令和期限；磁盘确认通过 sampler 的持久化状态等待。

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
use super::api::FindNodeResponse;
use super::api::{
    Command, DhtDispatcherConfig, DhtHandle, DispatcherCreateError, DispatcherError, PingResponse,
    QueryError, RemoteNode,
};
use super::maintenance::MaintenanceState;
use super::sampling::SampleCache;
use crate::dht::krpc::MessageType;
use crate::dht::krpc::NodeId;
use crate::dht::krpc::QueryMethod;
use crate::dht::peer_store::PeerStore;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::NodeContact;
use crate::dht::routing::RoutingTable;
use crate::dht::token::TokenManager;
use crate::dht::transaction::TransactionError;
use crate::dht::transaction::TransactionId;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::ReceivedMessage;
use crate::dht::udp::UdpTransport;
use crate::dht::udp::UdpTransportError;

/// transaction manager 之外，dispatcher 完成请求所需的业务上下文。
///
/// [`TransactionManager`] 只关心 ID、地址、方法和期限；这里额外保存返回通道、预期
/// Node ID 以及 routing table 探测目的。
#[derive(Debug)]
pub(super) struct PendingDispatch {
    /// 响应应该来自哪个地址，以及预期使用哪个 Node ID。
    pub(super) remote: RemoteNode,
    /// 查询完成后应该通知谁、执行哪一种 routing table 后续动作。
    pub(super) purpose: PendingPurpose,
}

/// 请求业务目的；从待发意图移入 pending，结束时据此回复调用者或推进内部状态机。
#[derive(Debug)]
pub(super) enum PendingPurpose {
    Fetch {
        progress: std::sync::Arc<super::api::RpcProgress>,
        cancel: tokio_util::sync::CancellationToken,
        reply: oneshot::Sender<Result<super::fetch::GetPeersResponse, QueryError>>,
    },
    /// 从磁盘恢复的联系人必须独立验证，不能直接标记为 good。
    Recovery { id: NodeId },
    /// 主动采样及其一次性 find_node 回退；上下文持有预留结果槽位。
    Sampling(super::sampler::Request),
    /// 由程序内部的 `ping` 接口发起，需要把结果返回给调用者。
    UserPing {
        reply: oneshot::Sender<Result<PingResponse, QueryError>>,
        cancel: tokio_util::sync::CancellationToken,
    },
    /// 由程序内部的 `find_node` 接口发起，需要解析并返回候选节点。
    #[cfg(test)]
    UserFindNode {
        reply: oneshot::Sender<Result<FindNodeResponse, QueryError>>,
        cancel: tokio_util::sync::CancellationToken,
    },
    /// 用反向 ping 确认一个主动查询我们的陌生节点确实可以接收数据报。
    Verification {
        /// 同时用于防止给同一个 Node ID 和地址重复发送验证 ping。
        key: (NodeId, SocketAddr),
        permit: Option<crate::dht::traffic::VerificationPermit>,
    },
    /// bucket 已满时，探测旧节点是否仍应保留。
    BucketProbe {
        /// 当前占据 bucket、需要接受 ping 检查的旧节点。
        incumbent: NodeContact,
        /// 当前旧节点响应后还需要继续检查的 questionable 节点。
        remaining: Vec<NodeContact>,
        /// 已经响应过我们、等待进入 bucket 的新节点。
        candidate: NodeContact,
        /// 当前是第几次探测；BEP 5 建议失败后再尝试一次。
        attempt: u8,
    },
    /// routing maintenance 发出的 find_node，不向普通 API 返回结果。
    MaintenanceLookup { node_id: NodeId },
}

/// 单个 UDP socket 对应的 DHT 状态所有者与消息分派器。
#[derive(Debug)]
pub(crate) struct DhtDispatcher {
    pub(super) budget: std::sync::Arc<crate::dht::traffic::Budget>,
    pub(super) queued: std::collections::VecDeque<super::traffic::Queued>,
    /// 待发队列下次需要推进的最早时间；None 表示无需为该队列设置定时唤醒。
    pub(super) queue_deadline: Option<Instant>,
    pub(super) fetch_ingress: Option<super::fetch::FetchIngress>,
    pub(super) sampling_paused: bool,
    pub(super) automatic_policy: crate::address::AddressPolicy,
    pub(super) clock: crate::clock::Clock,
    pub(super) recovery: super::recovery::Recovery,
    /// 负责编解码并收发 KRPC UDP 数据报。
    pub(super) transport: UdpTransport,
    /// 当前 socket 和地址族对应的 routing table。
    pub(super) routing: RoutingTable,
    /// 保存发送前已登记和已发出待响应的 transaction；明确发送失败会立即撤销登记。
    pub(super) transactions: TransactionManager,
    /// 接收各个 [`DhtHandle`] 提交的命令。
    commands: mpsc::Receiver<Command>,
    /// 以 transaction ID 找回查询的业务上下文。
    pub(super) pending: HashMap<TransactionId, PendingDispatch>,
    /// 正在进行的陌生节点验证，用来合并重复验证请求。
    pub(super) verifications: HashSet<(NodeId, SocketAddr)>,
    /// 正在进行的 bucket 旧节点探测，避免同时重复探测同一节点。
    pub(super) bucket_probes: HashSet<(NodeId, SocketAddr)>,
    /// 启动自查找、陈旧 bucket 刷新和失败退避状态。
    pub(super) maintenance: MaintenanceState,
    /// 只在收发 peer 查询时轮换密钥，不创建额外定时任务。
    pub(super) tokens: TokenManager,
    /// 只保存通过 token 和地址校验的宣布，过期时间参与下方事件循环。
    pub(super) peers: PeerStore,
    /// BEP 51 的短期样本快照；只有收到合法采样请求时才刷新。
    pub(super) samples: SampleCache,
    /// 主动采样的会话和跨会话冷却；不会与服务端样本快照混用。
    pub(super) sampler: super::sampler::Sampler,
}

impl DhtDispatcher {
    /// 使用默认命令队列容量创建 dispatcher。
    ///
    /// 返回的 dispatcher 应交给一个 Tokio task 调用 [`Self::run`]；上层保留
    /// [`DhtHandle`] 即可发起查询。
    #[cfg(test)]
    pub(crate) fn new(
        transport: UdpTransport,
        routing: RoutingTable,
        transactions: TransactionManager,
    ) -> Result<(Self, DhtHandle), DispatcherCreateError> {
        Self::with_config(
            transport,
            routing,
            transactions,
            DhtDispatcherConfig::default(),
        )
    }

    /// 使用自定义命令队列容量创建 dispatcher。
    ///
    /// 一个实例只服务一种地址族。构造时立即检查 socket 与 routing table，可以让
    /// 配置错误在联网前暴露，而不是运行后悄悄丢弃节点。
    #[cfg(test)]
    pub(crate) fn with_config(
        transport: UdpTransport,
        routing: RoutingTable,
        transactions: TransactionManager,
        config: DhtDispatcherConfig,
    ) -> Result<(Self, DhtHandle), DispatcherCreateError> {
        Self::with_budget(
            transport,
            routing,
            transactions,
            config,
            std::sync::Arc::default(),
        )
    }

    /// 接收会话共享预算；每个地址族只拥有自己的协议状态。
    pub(crate) fn with_budget(
        transport: UdpTransport,
        routing: RoutingTable,
        transactions: TransactionManager,
        config: DhtDispatcherConfig,
        budget: std::sync::Arc<crate::dht::traffic::Budget>,
    ) -> Result<(Self, DhtHandle), DispatcherCreateError> {
        if config.command_capacity == 0 {
            return Err(DispatcherCreateError::EmptyCommandQueue);
        }
        let maintenance = config.maintenance;
        if maintenance.enabled
            && (maintenance.refresh_after.is_zero()
                || maintenance.lookup_parallelism == 0
                || maintenance.shortlist_size == 0
                || maintenance.max_queries < maintenance.shortlist_size
                || maintenance.retry_initial.is_zero()
                || maintenance.retry_max < maintenance.retry_initial)
        {
            return Err(DispatcherCreateError::InvalidMaintenanceConfig(
                "时间必须大于零，且 max_queries 不得小于 shortlist_size",
            ));
        }

        let local_address = transport
            .local_addr()
            .map_err(|error| DispatcherCreateError::LocalAddress(error.to_string()))?;
        let family_matches = matches!(
            (routing.address_family(), local_address),
            (AddressFamily::Ipv4, SocketAddr::V4(_)) | (AddressFamily::Ipv6, SocketAddr::V6(_))
        );
        if !family_matches {
            return Err(DispatcherCreateError::AddressFamilyMismatch {
                socket: local_address,
                routing: routing.address_family(),
            });
        }

        // sender 交给外部 handle，receiver 始终只由这个 dispatcher 持有。
        let peers = PeerStore::new(config.peer_store, routing.address_family(), current_time())
            .map_err(DispatcherCreateError::InvalidPeerStoreConfig)?;
        let tokens =
            TokenManager::new(current_time()).map_err(|_| DispatcherCreateError::TokenEntropy)?;
        let (sender, commands) = mpsc::channel(config.command_capacity);
        let maintenance = MaintenanceState::new(maintenance, current_time());
        Ok((
            Self {
                budget,
                queued: Default::default(),
                queue_deadline: None,
                fetch_ingress: None,
                sampling_paused: false,
                automatic_policy: config.peer_store.address_policy,
                transport,
                routing,
                transactions,
                commands,
                pending: HashMap::new(),
                verifications: HashSet::new(),
                bucket_probes: HashSet::new(),
                maintenance,
                peers,
                tokens,
                samples: SampleCache::default(),
                sampler: super::sampler::Sampler::default(),
                recovery: super::recovery::Recovery::default(),
                clock: crate::clock::Clock::default(),
            },
            DhtHandle { commands: sender },
        ))
    }

    /// 持续处理网络消息、主动查询命令与 transaction 超时，直到收到关闭命令。
    ///
    /// 每轮都从 transaction manager 读取最近期限，因此新查询加入或旧查询完成后，
    /// 下一轮会自动调整定时器，不需要为每个 transaction 单独创建 task。
    #[cfg(test)]
    pub(crate) async fn run(mut self) -> Result<(), DispatcherError> {
        self.run_loop().await
    }

    /// 借用状态驱动网络循环；生产入口 run_persistent 在本循环返回后继续结算存储。
    /// 循环回复关闭只确认查询清理，最终快照、任务退出及资源释放仍由外层会话等待。
    pub(super) async fn run_loop(&mut self) -> Result<(), DispatcherError> {
        loop {
            let cancellations: Vec<_> = self
                .pending
                .values()
                .map(|p| &p.purpose)
                .chain(self.queued.iter().map(|q| &q.purpose))
                .filter_map(|purpose| match purpose {
                    PendingPurpose::Fetch { cancel, .. }
                    | PendingPurpose::UserPing { cancel, .. } => Some(cancel.clone()),
                    #[cfg(test)]
                    #[cfg(test)]
                    PendingPurpose::UserFindNode { cancel, .. } => Some(cancel.clone()),
                    _ => None,
                })
                .collect();
            let deadline = self.transactions.next_deadline();
            let maintenance_deadline = self.maintenance.deadline(&self.routing);
            let peer_deadline = self.peers.next_deadline();
            let sampler_deadline = self.sampler.deadline;
            let recovery_deadline = self.recovery.deadline(self.recovery_capacity());
            let output = self.sampler.output_watch();
            tokio::select! {
                _ = super::fetch::wait_cancelled(cancellations) => {
                    self.cancel_fetch_queries();
                },
                _ = wait_until(self.queue_deadline) => {},
                _ = wait_until(recovery_deadline) => {},
                _ = self.sampler.storage_event() => {},
                _ = wait_until(sampler_deadline), if !self.sampling_paused => {},
                permit = super::sampler::watch_output(output), if !self.sampling_paused => {
                    match permit {
                        Ok(permit) => self.sampler.accept_permit(permit),
                        Err(()) => self.stop_sampler(current_time()),
                    }
                }
                received = self.transport.recv_datagram() => {
                    match received {
                        Ok(datagram) => {
                            let pending_source=self.pending.values().any(|p| p.remote.address==datagram.source);
                            if self.budget.inbound(datagram.source.ip(),datagram.bytes.len(),pending_source)
                                && let Ok(received)=datagram.decode()
                                && (!pending_source || received.message.y != MessageType::Query || self.budget.disguised_query(received.source.ip())) {
                                self.handle_received(received,current_time()).await;
                            }
                        },
                        // 这些错误只影响当前数据报，不能让公网垃圾流量关闭服务。
                        Err(UdpTransportError::Decode { .. })
                        | Err(UdpTransportError::MessageTooLarge { source: Some(_), .. }) => {}
                        Err(error) => {
                            self.close_pending();
                            return Err(DispatcherError::Transport(error));
                        }
                    }
                }
                command = self.commands.recv() => {
                    match command {
                        Some(Command::Shutdown { reply }) => {
                            // 先通知所有查询等待者，再确认 shutdown 已完成。
                            self.close_pending();
                            let _ = reply.send(());
                            return Ok(());
                        }
                        Some(command) => self.handle_command(command, current_time()).await,
                        None => {
                            // 所有 handle 都已释放，节点已经没有控制者，可以正常退出。
                            self.close_pending();
                            return Ok(());
                        }
                    }
                }
                _ = wait_until(deadline) => {
                    self.expire_transactions(current_time()).await;
                }
                _ = wait_until(maintenance_deadline) => {}
                _ = wait_until(peer_deadline) => {
                    self.peers.expire(current_time(), 256);
                }
            }
            self.advance_outbound(current_time()).await;
            self.advance_maintenance(current_time()).await;
            self.advance_recovery(current_time()).await;
            self.advance_sampler(current_time()).await;
        }
    }

    /// 把上层命令转换成具体 KRPC 查询。
    async fn handle_command(&mut self, command: Command, now: Instant) {
        match command {
            Command::GetPeers {
                remote,
                hash,
                progress,
                cancel,
                reply,
            } => {
                if cancel.is_cancelled() || reply.is_closed() {
                    return;
                }
                if !self.automatic_policy.accepts(remote.address)
                    || !self.routing.address_family().accepts(remote.address)
                {
                    let _ = reply.send(Err(QueryError::InvalidResponse("get_peers 目标地址无效")));
                } else if self.occupied() >= self.transactions.max_pending().saturating_sub(1) {
                    progress
                        .limited
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    let _ = reply.send(Err(QueryError::AtCapacity {
                        limit: self.transactions.max_pending().saturating_sub(1),
                    }));
                } else {
                    self.start_rpc(
                        remote,
                        super::fetch::Outbound::GetPeers(hash),
                        PendingPurpose::Fetch {
                            cancel,
                            reply,
                            progress,
                        },
                        now,
                    )
                    .await;
                }
            }
            Command::FetchSeeds { hash, reply } => {
                let nodes = self
                    .routing
                    .closest_usable(NodeId(hash.0), 256, now)
                    .into_iter()
                    .filter(|n| self.automatic_policy.accepts(n.address))
                    .map(|n| super::api::DiscoveredNode {
                        id: n.id,
                        address: n.address,
                    })
                    .collect();
                let _ = reply.send(nodes);
            }
            Command::FetchIngress { ingress, reply } => {
                self.fetch_ingress = ingress;
                let _ = reply.send(());
            }
            Command::SamplingPause { paused, reply } => {
                self.sampling_paused = paused;
                if paused {
                    self.discard_queued_sampling(now);
                }
                let _ = reply.send(());
            }

            Command::RoutingSnapshot { reply } => {
                let _ = reply.send(self.saved_contacts());
            }
            Command::StoragePause { error, reply } => {
                self.stop_sampler(now);
                self.sampler.mark_storage_fault(error);
                let _ = reply.send(());
            }
            Command::StartSampling { config, reply } => {
                let reserve = self.maintenance.config.reserved_user_transactions.max(1);
                let available = self.transactions.max_pending().saturating_sub(reserve);
                let result = self.sampler.start(config, available.saturating_add(1), now);
                let _ = reply.send(result);
            }
            Command::StopSampling { reply } => {
                self.stop_sampler(now);
                let _ = reply.send(());
            }
            #[cfg(test)]
            Command::SamplingStatus { reply } => {
                let _ = reply.send(super::sampler::SamplerStatus {
                    collector_paused: self.sampling_paused,
                    ..self.sampler.status()
                });
            }
            #[cfg(test)]
            Command::Ping {
                remote,
                reply,
                cancel,
            } => {
                self.start_query(
                    remote,
                    QueryMethod::Ping,
                    None,
                    PendingPurpose::UserPing { reply, cancel },
                    now,
                )
                .await;
            }
            Command::BootstrapPing {
                remote,
                reply,
                cancel,
            } => {
                if !self.recovery_capacity() {
                    let _ = reply.send(Err(QueryError::AtCapacity {
                        limit: self.transactions.max_pending().saturating_sub(1),
                    }));
                } else {
                    self.start_query(
                        remote,
                        QueryMethod::Ping,
                        None,
                        PendingPurpose::UserPing { reply, cancel },
                        now,
                    )
                    .await;
                }
            }
            Command::Status { reply } => {
                let nodes = self
                    .routing
                    .closest_usable(self.routing.local_id(), usize::MAX, now);
                let good = nodes
                    .iter()
                    .filter(|node| node.status(now) == crate::dht::routing::NodeStatus::Good)
                    .count();
                let (recovery_queued, recovery_active) = self.recovery.counts();
                // socket 在构造时已经验证有效，监听地址在其生命周期内不变。
                let _ = reply.send(super::api::DhtStatus {
                    node_id: self.routing.local_id(),
                    family: self.routing.address_family(),
                    address: self.transport.local_addr().expect("已绑定的 UDP socket"),
                    good,
                    questionable: nodes.len() - good,
                    recovery_queued,
                    recovery_active,
                    pending: self.occupied(),
                    sampler: super::sampler::SamplerStatus {
                        collector_paused: self.sampling_paused,
                        ..self.sampler.status()
                    },
                });
            }
            #[cfg(test)]
            Command::FindNode {
                cancel,
                remote,
                target,
                reply,
            } => {
                self.start_query(
                    remote,
                    QueryMethod::FindNode,
                    Some(target),
                    PendingPurpose::UserFindNode { reply, cancel },
                    now,
                )
                .await;
            }
            Command::Shutdown { .. } => unreachable!("shutdown 已在事件循环中处理"),
        }
    }

    /// 注册并发送一条主动查询。
    ///
    /// 必须先注册 transaction、再发送 UDP。否则响应可能已经到达，而本地还没有对应
    /// 状态。发送失败时会同时撤销两张表中的记录，避免白白占用并发名额直到超时。
    pub(super) async fn start_query(
        &mut self,
        remote: RemoteNode,
        method: QueryMethod,
        target: Option<NodeId>,
        purpose: PendingPurpose,
        now: Instant,
    ) {
        let query = match (method, target) {
            (QueryMethod::Ping, None) => super::fetch::Outbound::Ping,
            (QueryMethod::FindNode, Some(target)) => super::fetch::Outbound::FindNode(target),
            (QueryMethod::SampleInfohashes, Some(target)) => super::fetch::Outbound::Sample(target),
            _ => {
                self.finish_start_error(
                    purpose,
                    QueryError::InvalidResponse("出站查询参数不匹配"),
                    now,
                );
                return;
            }
        };
        self.start_rpc(remote, query, purpose, now).await;
    }

    /// 先登记 transaction 和业务上下文，再尝试 UDP 发送，防止快速响应找不到等待者。
    /// 发送失败同步移除两处登记；只有发送成功才增加实际发送量。
    pub(super) async fn send_rpc(
        &mut self,
        remote: RemoteNode,
        query: super::fetch::Outbound,
        purpose: PendingPurpose,
        now: Instant,
    ) {
        let (method, _, _) = query.fields();
        let transaction_id = match self
            .transactions
            .register(remote.address, method.clone(), now)
        {
            Ok(id) => id,
            Err(error) => {
                self.finish_start_error(purpose, transaction_query_error(error), now);
                return;
            }
        };

        let message = query.message(self.routing.local_id(), transaction_id.to_byte_buf());
        let class = purpose.class();
        // 先保存业务上下文。即使 UDP 响应马上进入 socket，事件循环下一次接收它时
        // 也一定能够找到对应的等待者。
        self.pending
            .insert(transaction_id, PendingDispatch { remote, purpose });
        if let Err(error) = self.transport.try_send_to(remote.address, &message) {
            self.transactions.cancel(transaction_id);
            if let Some(pending) = self.pending.remove(&transaction_id) {
                self.finish_start_error(pending.purpose, QueryError::Transport(error), now);
            }
        } else {
            self.budget.sent(
                class,
                bendy::serde::to_bytes(&message).expect("已编码消息").len(),
            );
            if let Some(PendingDispatch {
                purpose: PendingPurpose::Fetch { progress, .. },
                ..
            }) = self.pending.get(&transaction_id)
            {
                progress
                    .sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// 处理查询尚未真正发出时的错误。
    ///
    /// 本地容量不足或发送失败不能证明远端失效，所以内部验证和 bucket probe 只清理
    /// 去重标记，不会增加远端节点的失败次数。
    pub(super) fn finish_start_error(
        &mut self,
        purpose: PendingPurpose,
        error: QueryError,
        now: Instant,
    ) {
        match purpose {
            PendingPurpose::Fetch { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            PendingPurpose::Recovery { id } => self.recovery.finished(id),
            PendingPurpose::Sampling(request) => {
                if matches!(&error,QueryError::Transport(UdpTransportError::Io(io)) if io.kind()==std::io::ErrorKind::WriteZero)
                {
                    self.sampler.uncertain_send(request, now);
                } else {
                    self.sampler.abandon_unsent(request, now);
                }
            }
            PendingPurpose::UserPing { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            #[cfg(test)]
            PendingPurpose::UserFindNode { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            PendingPurpose::Verification { key, .. } => {
                self.verifications.remove(&key);
            }
            PendingPurpose::BucketProbe { incumbent, .. } => {
                self.bucket_probes
                    .remove(&(incumbent.id, incumbent.address));
            }
            PendingPurpose::MaintenanceLookup { node_id } => {
                if let Some(lookup) = self.maintenance.lookup.as_mut() {
                    lookup.defer(node_id);
                }
            }
        }
    }

    /// 按 `y` 字段把一条已解码消息交给对应处理分支。
    async fn handle_received(&mut self, received: ReceivedMessage, now: Instant) {
        match received.message.y {
            MessageType::Query => self.handle_query(received, now).await,
            MessageType::Response => self.handle_response(received, now).await,
            MessageType::Error => self.handle_error(received, now).await,
        }
    }

    /// 关闭时撤销全部 transaction，并唤醒正在等待查询结果的调用者。
    fn close_pending(&mut self) {
        self.stop_sampler(current_time());
        while let Some(queued) = self.queued.pop_front() {
            self.finish_start_error(queued.purpose, QueryError::ShuttingDown, current_time());
        }
        let pending = std::mem::take(&mut self.pending);
        for (id, pending) in pending {
            self.budget.inflight_cancelled(pending.purpose.class());
            self.transactions.cancel(id);
            match pending.purpose {
                PendingPurpose::Fetch { reply, .. } => {
                    let _ = reply.send(Err(QueryError::ShuttingDown));
                }
                PendingPurpose::UserPing { reply, .. } => {
                    let _ = reply.send(Err(QueryError::ShuttingDown));
                }
                #[cfg(test)]
                PendingPurpose::UserFindNode { reply, .. } => {
                    let _ = reply.send(Err(QueryError::ShuttingDown));
                }
                PendingPurpose::Verification { .. }
                | PendingPurpose::Recovery { .. }
                | PendingPurpose::Sampling(_)
                | PendingPurpose::BucketProbe { .. }
                | PendingPurpose::MaintenanceLookup { .. } => {}
            }
        }
        self.verifications.clear();
        self.bucket_probes.clear();
        self.maintenance.clear();
    }
}

#[cfg(test)]
mod tests;

/// 把调用者最常关心的容量错误转换为更直接的查询错误。
fn transaction_query_error(error: TransactionError) -> QueryError {
    match error {
        TransactionError::AtCapacity { limit } => QueryError::AtCapacity { limit },
        error => QueryError::Transaction(error),
    }
}

/// 等待最近的 transaction 期限；没有等待项时保持 pending，不进行空轮询。
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => pending::<()>().await,
    }
}

/// 使用 Tokio 时钟生成业务时间，使暂停时间的测试不必真的等待十五分钟。
pub(super) fn current_time() -> Instant {
    tokio::time::Instant::now().into_std()
}
