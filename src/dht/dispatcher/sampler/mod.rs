//! 有界主动采样：结果槽位先预留，节点冷却跨启停保存，所有网络操作仍由 dispatcher 执行。
//!
//! dispatcher 选择请求；启用持久化时须由 durable 确认磁盘预约后才能发包，响应再更新冷却并交付批次。
mod api;
mod durable;
use super::{
    api::{DiscoveredNode, QueryError, RemoteNode},
    runtime::{DhtDispatcher, PendingPurpose},
};
use crate::{
    dht::routing::{NodeStatus, RoutingTable, xor_distance},
    krpc::{InfoHashV1, NodeId, QueryMethod, ResponseArgs},
};
pub(crate) use api::{SampleBatch, SamplerConfig, SamplerError, SamplerStatus};
use std::{
    collections::{HashMap, HashSet},
    future::pending,
    net::IpAddr,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const UNSUPPORTED_FOR: Duration = Duration::from_secs(21600);
/// 当前调度停顿原因，不是错误分类；Storage 可表示等确认，也可伴随实际存储故障。
/// 是否发生存储错误还需查看 SamplerStatus.storage_error，不能仅凭 pause 判定。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PauseReason {
    #[default]
    Stopped,
    Ready,
    NoSeeds,
    Cooldown,
    Transactions,
    Output,
    TrackingCapacity,
    /// 等待数据库确认；不阻塞 dispatcher 处理其他消息。
    Storage,
}

#[derive(Debug)]
pub(super) struct SampleResponse {
    pub(super) nodes: Vec<DiscoveredNode>,
    pub(super) interval: Duration,
    pub(super) num: u64,
    pub(super) samples: Vec<InfoHashV1>,
}
/// 缺少 samples 的普通节点响应不是采样成功，但可以复用其联系人。
pub(super) fn decode_sample(
    response: &ResponseArgs,
    nodes: Vec<DiscoveredNode>,
    encoded_len: usize,
) -> Result<Option<SampleResponse>, QueryError> {
    if encoded_len > 1024 {
        return Err(QueryError::InvalidResponse("BEP 51 响应超过 1024 字节"));
    }
    let Some(samples) = &response.samples else {
        return Ok(None);
    };
    let interval = response
        .interval
        .filter(|n| *n <= 21600)
        .ok_or(QueryError::InvalidResponse("采样响应缺少合法 interval"))?;
    let num = response
        .num
        .ok_or(QueryError::InvalidResponse("采样响应缺少 num"))?;
    let mut seen = HashSet::new();
    let samples: Vec<_> = samples
        .0
        .iter()
        .copied()
        .filter(|hash| seen.insert(*hash))
        .collect();
    if num < samples.len() as u64 {
        return Err(QueryError::InvalidResponse("num 小于不同样本数量"));
    }
    Ok(Some(SampleResponse {
        nodes,
        interval: Duration::from_secs(interval.into()),
        num,
        samples,
    }))
}

/// 回退只查询联系人，不再次采样；两种请求仍共用并发和发送间隔预算。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestKind {
    Sample,
    FindNodeFallback,
}

/// 事件循环本轮需要监听的输出事件；可写时只监听关闭，避免空转。
#[derive(Debug)]
pub(super) enum OutputWatch {
    Inactive,
    Closed(mpsc::Sender<SampleBatch>),
    Capacity(mpsc::Sender<SampleBatch>),
}

/// 预留的批次许可和磁盘租约随请求走到成功、失败或取消的收尾路径。
#[derive(Debug)]
pub(super) struct Request {
    /// 采样启停代数，拒绝旧会话的迟到结果；不同于 SQLite 任务领取 generation。
    generation: u64,
    /// 可选磁盘冷却预约；预留成功不代表请求已发出，结束时按发送确定性结算。
    lease: Option<crate::storage::CooldownLease>,
    pub(super) node: DiscoveredNode,
    pub(super) target: NodeId,
    pub(super) kind: RequestKind,
    permit: Option<mpsc::OwnedPermit<SampleBatch>>,
}
#[derive(Debug, Clone, Copy)]
struct Candidate {
    node: DiscoveredNode,
    visited: bool,
    kind: RequestKind,
}
#[derive(Debug, Clone, Copy)]
struct Cooldown {
    until: Instant,
    failures: u32,
}
#[derive(Debug)]
struct Session {
    config: SamplerConfig,
    output: mpsc::Sender<SampleBatch>,
    ready_permit: Option<mpsc::OwnedPermit<SampleBatch>>,
    candidates: HashMap<NodeId, Candidate>,
    in_flight: HashMap<NodeId, IpAddr>,
    target: NodeId,
    queries: usize,
    next_send: Instant,
}
/// 候选筛选结果中的期限和容量原因用于安排下一次唤醒，不代表已经发出请求。
struct CandidateSelection {
    candidate: Option<Candidate>,
    earliest: Option<Instant>,
    tracking_full: bool,
}

impl Session {
    /// 按距离选择一个可发送候选，同时报告无候选时的下一次冷却期限。
    /// 此处只读取状态；真正占用在途名额由 register_request 完成。
    fn select_candidate(
        &self,
        ids: &HashMap<NodeId, Cooldown>,
        ips: &HashMap<IpAddr, Instant>,
        now: Instant,
    ) -> CandidateSelection {
        let mut eligible = Vec::new();
        let mut earliest = None;
        let mut tracking_full = false;
        for candidate in self.candidates.values() {
            if (candidate.visited && candidate.kind == RequestKind::Sample)
                || self.in_flight.contains_key(&candidate.node.id)
                || self
                    .in_flight
                    .values()
                    .any(|ip| *ip == candidate.node.address.ip())
            {
                continue;
            }
            let id_until = ids
                .get(&candidate.node.id)
                .map(|cooldown| cooldown.until)
                .unwrap_or(now);
            let ip_until = ips
                .get(&candidate.node.address.ip())
                .copied()
                .unwrap_or(now);
            // 回退不是再次采样，允许在 204 后取一次联系人，但仍受全局发送间隔约束。
            let due = if candidate.kind == RequestKind::FindNodeFallback {
                now
            } else {
                id_until.max(ip_until)
            };
            if due > now {
                earliest = Some(earliest.map_or(due, |at: Instant| at.min(due)));
                continue;
            }
            if (!ids.contains_key(&candidate.node.id) && ids.len() >= self.config.cooldown_capacity)
                || (!ips.contains_key(&candidate.node.address.ip())
                    && ips.len() >= self.config.cooldown_capacity)
            {
                tracking_full = true;
                continue;
            }
            eligible.push(*candidate);
        }
        eligible.sort_by_key(|candidate| xor_distance(&candidate.node.id.0, &self.target.0));
        eligible.truncate(self.config.shortlist_size);
        CandidateSelection {
            candidate: eligible.first().copied(),
            earliest,
            tracking_full,
        }
    }

    /// 换一个查询目标并清除本轮进度；Node ID/IP 冷却保存在 Sampler 中，不随轮次清除。
    fn reset_round(&mut self, now: Instant) {
        self.target = NodeId(rand::random());
        self.queries = 0;
        for candidate in self.candidates.values_mut() {
            candidate.visited = false;
            candidate.kind = RequestKind::Sample;
        }
        self.next_send = now + self.config.send_spacing;
    }
}

/// dispatcher 独占的采样状态；session 管一轮启停，冷却表和 durable 跨启停保留。
#[derive(Debug, Default)]
pub(super) struct Sampler {
    generation: u64,
    durable: Option<durable::Durable>,
    session: Option<Session>,
    ids: HashMap<NodeId, Cooldown>,
    ips: HashMap<IpAddr, Instant>,
    status: SamplerStatus,
    pub(super) deadline: Option<Instant>,
}

impl Sampler {
    /// 校验配置并建立有界批次通道，更新启停代数；返回接收端不代表已经发包。
    /// 已运行、配置无效或存储故障时拒绝启动，不能用重启绕过冷却。
    pub(super) fn start(
        &mut self,
        config: SamplerConfig,
        max_pending: usize,
        now: Instant,
    ) -> Result<mpsc::Receiver<SampleBatch>, SamplerError> {
        if self.status.storage_error.is_some() {
            return Err(SamplerError::StorageFault);
        }
        // 持久化恢复最多加载两类各一万个键，避免损坏的数据库导致无界分配。
        if self.durable.is_some() && config.cooldown_capacity > 10_000 {
            return Err(SamplerError::InvalidConfig);
        }
        if self.session.is_some() {
            return Err(SamplerError::AlreadyRunning);
        }
        config.validate(max_pending, now)?;
        self.ids.retain(|_, value| value.until > now);
        self.ips.retain(|_, until| *until > now);
        if self.ids.len() > config.cooldown_capacity || self.ips.len() > config.cooldown_capacity {
            return Err(SamplerError::InvalidConfig);
        }
        let (output, receiver) = mpsc::channel(config.output_capacity);
        self.generation = self.generation.wrapping_add(1);
        self.session = Some(Session {
            config,
            output,
            ready_permit: None,
            candidates: HashMap::new(),
            in_flight: HashMap::new(),
            target: NodeId(rand::random()),
            queries: 0,
            next_send: now,
        });
        self.status = SamplerStatus {
            running: true,
            ..Default::default()
        };
        self.deadline = Some(now);
        Ok(receiver)
    }
    pub(super) fn status(&self) -> SamplerStatus {
        self.status.clone()
    }
    /// 容量恢复和 receiver 关闭都由 select 驱动，不能在网络响应处理里 await send。
    pub(super) fn output_watch(&self) -> OutputWatch {
        let Some(session) = &self.session else {
            return OutputWatch::Inactive;
        };
        if self.status.pause == PauseReason::Output {
            OutputWatch::Capacity(session.output.clone())
        } else {
            OutputWatch::Closed(session.output.clone())
        }
    }
    /// 保存输出通道预留槽位，后续移交请求；取得槽位不是生成或交付了一个批次。
    pub(super) fn accept_permit(&mut self, permit: mpsc::OwnedPermit<SampleBatch>) {
        if let Some(session) = &mut self.session {
            session.ready_permit = Some(permit);
        }
    }
    fn cooldown(&mut self, node: DiscoveredNode, until: Instant, failures: u32) {
        self.ids
            .entry(node.id)
            .and_modify(|v| {
                v.until = v.until.max(until);
                v.failures = failures;
            })
            .or_insert(Cooldown { until, failures });
        self.ips
            .entry(node.address.ip())
            .and_modify(|v| *v = (*v).max(until))
            .or_insert(until);
    }
    /// 停止当前调度并处理已就绪但未发出的预约，跨会话冷却仍保留。
    /// 等待中的数据库预约与结算不在这里消失；外层仍须驱动 storage_event/flush_storage。
    pub(super) fn stop(&mut self, now: Instant) {
        if let Some(request) = self.durable.as_mut().and_then(|d| d.ready.take()) {
            self.abandon_unsent(request, now);
        }
        // 在途请求的 interval 尚未知，按规范最大值保守等待；不能靠启停绕过冷却。
        if let Some(session) = self.session.take() {
            for (id, ip) in session.in_flight {
                if let Some(value) = self.ids.get_mut(&id) {
                    value.until = value.until.max(now + UNSUPPORTED_FOR);
                }
                if let Some(value) = self.ips.get_mut(&ip) {
                    *value = (*value).max(now + UNSUPPORTED_FOR);
                }
            }
        }
        self.status.running = false;
        self.status.in_flight = 0;
        self.status.candidates = 0;
        self.status.pause = PauseReason::Stopped;
        self.deadline = None;
    }
    fn add_nodes(&mut self, nodes: Vec<DiscoveredNode>, routing: &RoutingTable, now: Instant) {
        let Some(s) = &mut self.session else { return };
        for node in nodes {
            if node.id == routing.local_id() {
                continue;
            }
            let known = routing.contact(node.id);
            if known.is_some_and(|n| n.status(now) == NodeStatus::Bad) {
                continue;
            }
            let verified = known.is_some();
            let node = known
                .map(|n| DiscoveredNode {
                    id: n.id,
                    address: n.address,
                })
                .unwrap_or(node);
            if !routing.address_family().accepts(node.address)
                || !s.config.address_policy.accepts(node.address)
            {
                continue;
            }
            s.candidates
                .entry(node.id)
                .and_modify(|old| {
                    if verified && !s.in_flight.contains_key(&node.id) {
                        old.node = node;
                    }
                })
                .or_insert(Candidate {
                    node,
                    visited: false,
                    kind: RequestKind::Sample,
                });
        }
        // 一次响应最多一个数据报，临时新增量有界；绝不淘汰在途节点。
        while s.candidates.len() > s.config.candidate_capacity {
            let worst = s
                .candidates
                .values()
                .filter(|c| !s.in_flight.contains_key(&c.node.id))
                .max_by_key(|c| xor_distance(&c.node.id.0, &s.target.0))
                .map(|c| c.node.id);
            if let Some(id) = worst {
                s.candidates.remove(&id);
            } else {
                break;
            }
        }
    }
    /// 每次最多启动一条 RPC，下一条至少间隔 send_spacing，不积攒突发额度。
    fn next(&mut self, routing: &RoutingTable, capacity: usize, now: Instant) -> Option<Request> {
        self.deadline = None;
        if self.status.storage_error.is_some() {
            self.status.pause = PauseReason::Storage;
            return None;
        }
        if self
            .durable
            .as_ref()
            .is_some_and(|d| d.reserving.is_some() || !d.settling.is_empty() || d.ready.is_some())
        {
            self.status.pause = PauseReason::Storage;
            return None;
        }
        let s = self.session.as_ref()?;
        let seeds = routing
            .closest_usable(s.target, s.config.shortlist_size, now)
            .into_iter()
            .map(|n| DiscoveredNode {
                id: n.id,
                address: n.address,
            })
            .collect();
        self.add_nodes(seeds, routing, now);
        let s = self.session.as_mut()?;
        s.candidates.retain(|id, _| {
            s.in_flight.contains_key(id)
                || routing
                    .contact(*id)
                    .is_none_or(|n| n.status(now) != NodeStatus::Bad)
        });
        self.status.in_flight = s.in_flight.len();
        self.status.candidates = s.candidates.len();
        if s.candidates.is_empty() {
            self.status.pause = PauseReason::NoSeeds;
            return None;
        }
        if capacity == 0 || s.in_flight.len() >= s.config.parallelism {
            self.status.pause = PauseReason::Transactions;
            return None;
        }
        if s.output.is_closed() {
            return None;
        }
        if s.ready_permit.is_none() {
            match s.output.clone().try_reserve_owned() {
                Ok(p) => s.ready_permit = Some(p),
                Err(_) => {
                    self.status.pause = PauseReason::Output;
                    return None;
                }
            }
        }
        if now < s.next_send {
            self.status.pause = PauseReason::Cooldown;
            self.deadline = Some(s.next_send);
            return None;
        }
        if s.queries >= s.config.max_queries
            || s.candidates
                .values()
                .all(|c| c.visited && c.kind == RequestKind::Sample)
        {
            if !s.in_flight.is_empty() {
                self.status.pause = PauseReason::Transactions;
                return None;
            }
            s.reset_round(now);
            self.deadline = Some(s.next_send);
            self.status.pause = PauseReason::Cooldown;
            return None;
        }
        // 只在需要腾出记录槽位时清理到期项，平时保留失败次数用于指数退避。
        if self.ids.len() >= s.config.cooldown_capacity {
            self.ids
                .retain(|id, v| v.until > now || s.in_flight.contains_key(id));
        }
        if self.ips.len() >= s.config.cooldown_capacity {
            self.ips
                .retain(|ip, until| *until > now || s.in_flight.values().any(|busy| busy == ip));
        }
        let CandidateSelection {
            candidate,
            mut earliest,
            tracking_full,
        } = s.select_candidate(&self.ids, &self.ips, now);
        let Some(candidate) = candidate else {
            // 一轮有部分节点完成、其余都在冷却时，不必等最远期限才换 target。
            // 清掉 visited 后不会重复走这条分支，下一轮直接等待真实的冷却期限。
            if s.in_flight.is_empty() && s.candidates.values().any(|c| c.visited) {
                s.reset_round(now);
                self.deadline = Some(s.next_send);
                self.status.pause = PauseReason::Cooldown;
                return None;
            }
            self.status.pause = if tracking_full {
                PauseReason::TrackingCapacity
            } else {
                PauseReason::Cooldown
            };
            if tracking_full {
                earliest = earliest
                    .into_iter()
                    .chain(
                        self.ids
                            .values()
                            .map(|v| v.until)
                            .chain(self.ips.values().copied())
                            .filter(|at| *at > now),
                    )
                    .min();
            }
            self.deadline = earliest;
            return None;
        };
        Some(self.register_request(candidate, now))
    }

    /// 选中请求后同时登记候选、在途 IP 和发送间隔，并把输出许可移入请求。
    fn register_request(&mut self, candidate: Candidate, now: Instant) -> Request {
        let session = self.session.as_mut().expect("候选只能来自正在运行的会话");
        let tracked = session
            .candidates
            .get_mut(&candidate.node.id)
            .expect("候选来自当前会话，登记前没有删除");
        tracked.visited = true;
        tracked.kind = RequestKind::Sample;
        session
            .in_flight
            .insert(candidate.node.id, candidate.node.address.ip());
        session.queries += 1;
        session.next_send = now + session.config.send_spacing;
        self.ids.entry(candidate.node.id).or_insert(Cooldown {
            until: now,
            failures: 0,
        });
        self.ips.entry(candidate.node.address.ip()).or_insert(now);
        self.deadline = Some(session.next_send);
        self.status.pause = PauseReason::Ready;
        self.status.in_flight = session.in_flight.len();
        Request {
            generation: self.generation,
            lease: None,
            node: candidate.node,
            target: session.target,
            kind: candidate.kind,
            permit: session.ready_permit.take(),
        }
    }
    fn finished(&mut self, request: &Request) {
        if let Some(s) = &mut self.session {
            s.in_flight.remove(&request.node.id);
            self.status.in_flight = s.in_flight.len();
        }
    }
    pub(super) fn success(
        &mut self,
        mut request: Request,
        response: SampleResponse,
        routing: &RoutingTable,
        now: Instant,
    ) {
        self.finished(&request);
        let Some(s) = &self.session else { return };
        let observed_at = match self
            .durable
            .as_ref()
            .map(|d| d.clock.wall_at(now))
            .transpose()
        {
            Ok(value) => value.unwrap_or_else(std::time::SystemTime::now),
            Err(error) => {
                self.mark_storage_fault(error);
                return;
            }
        };
        let until = now + response.interval.max(s.config.minimum_interval);
        self.cooldown(request.node, until, 0);
        self.settle_request(&request, now);
        self.add_nodes(response.nodes, routing, now);
        self.status.successful += 1;
        if let Some(permit) = request.permit.take() {
            permit.send(SampleBatch {
                observed_at,
                responder: request.node,
                target: request.target,
                received_at: now,
                interval: response.interval,
                num: response.num,
                samples: response.samples,
            });
        }
    }
    pub(super) fn nodes(
        &mut self,
        request: Request,
        nodes: Vec<DiscoveredNode>,
        routing: &RoutingTable,
        now: Instant,
    ) {
        self.finished(&request);
        if request.kind == RequestKind::Sample {
            self.status.unsupported += 1;
            self.cooldown(request.node, now + UNSUPPORTED_FOR, 0);
        }
        self.settle_request(&request, now);
        self.add_nodes(nodes, routing, now);
    }
    pub(super) fn failure(&mut self, request: Request, error: &QueryError, now: Instant) {
        self.finished(&request);
        let Some(s) = self.session.as_mut() else {
            return;
        };
        if request.kind == RequestKind::Sample
            && matches!(error, QueryError::Remote { code: 204, .. })
        {
            if let Some(candidate) = s.candidates.get_mut(&request.node.id) {
                candidate.kind = RequestKind::FindNodeFallback;
            }
            self.status.unsupported += 1;
            self.cooldown(request.node, now + UNSUPPORTED_FOR, 0);
        } else {
            let failures = self
                .ids
                .get(&request.node.id)
                .map_or(1, |v| v.failures.saturating_add(1));
            let delay = s
                .config
                .retry_initial
                .saturating_mul(2_u32.saturating_pow(failures.saturating_sub(1)))
                .min(s.config.retry_max);
            self.status.failed += 1;
            self.cooldown(request.node, now + delay, failures);
        }
        self.settle_request(&request, now);
    }
}

/// 如果队列满就等待一个槽位；否则只监听消费者离开，避免可写队列导致空轮询。
pub(super) async fn watch_output(watch: OutputWatch) -> Result<mpsc::OwnedPermit<SampleBatch>, ()> {
    match watch {
        OutputWatch::Inactive => pending().await,
        OutputWatch::Capacity(sender) => sender.reserve_owned().await.map_err(|_| ()),
        OutputWatch::Closed(sender) => {
            sender.closed().await;
            Err(())
        }
    }
}

impl DhtDispatcher {
    pub(super) async fn advance_sampler(&mut self, now: Instant) {
        if self.sampling_paused {
            return;
        }
        if self
            .sampler
            .session
            .as_ref()
            .is_some_and(|s| s.output.is_closed())
        {
            self.stop_sampler(now);
            return;
        }
        let reserve = self.maintenance.config.reserved_user_transactions.max(1);
        let capacity = self
            .transactions
            .max_pending()
            .saturating_sub(reserve)
            .saturating_sub(self.occupied());
        let ready = self.sampler.take_reserved(capacity, now);
        if ready.is_none()
            && self
                .sampler
                .durable
                .as_ref()
                .is_some_and(|d| d.ready.is_some())
        {
            return;
        }
        let selected = ready.or_else(|| self.sampler.next(&self.routing, capacity, now));
        if let Some(request) = selected {
            let Some(request) = self.sampler.reserve_request(request, now) else {
                return;
            };
            self.start_query(
                RemoteNode {
                    address: request.node.address,
                    expected_id: Some(request.node.id),
                },
                if request.kind == RequestKind::FindNodeFallback {
                    QueryMethod::FindNode
                } else {
                    QueryMethod::SampleInfohashes
                },
                Some(request.target),
                PendingPurpose::Sampling(request),
                now,
            )
            .await;
        }
    }
    pub(super) fn stop_sampler(&mut self, now: Instant) {
        self.discard_queued_sampling(now);
        self.sampler.stop(now);
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                matches!(pending.purpose, PendingPurpose::Sampling(_)).then_some(*id)
            })
            .collect();
        for id in ids {
            self.transactions.cancel(id);
            if let Some(pending) = self.pending.remove(&id)
                && let PendingPurpose::Sampling(request) = pending.purpose
            {
                self.sampler.cancel_request(&request, now);
            }
        }
    }
}

#[cfg(test)]
mod tests;
