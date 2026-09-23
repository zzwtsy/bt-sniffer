//! 采样器的唯一状态所有者、启停生命周期和只读状态视图。

use super::super::api::DiscoveredNode;
use super::{
    api::{SampleBatch, SamplerConfig, SamplerError, SamplerStatus},
    durable,
};
use crate::dht::krpc::NodeId;
use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

pub(super) const UNSUPPORTED_FOR: Duration = Duration::from_secs(21600);

/// 当前调度停顿原因，不是错误分类；Storage 可表示等确认，也可伴随实际存储故障。
/// 是否发生存储错误还需查看 SamplerStatus.storage_error，不能仅凭 pause 判定。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
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

/// 回退只查询联系人，不再次采样；两种请求仍共用并发和发送间隔预算。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dht::dispatcher) enum RequestKind {
    Sample,
    FindNodeFallback,
}

/// 事件循环本轮需要监听的输出事件；可写时只监听关闭，避免空转。
#[derive(Debug)]
pub(in crate::dht::dispatcher) enum OutputWatch {
    Inactive,
    Closed(mpsc::Sender<SampleBatch>),
    Capacity(mpsc::Sender<SampleBatch>),
}

/// 预留的批次许可和磁盘租约随请求走到成功、失败或取消的收尾路径。
#[derive(Debug)]
pub(in crate::dht::dispatcher) struct Request {
    pub(in crate::dht::dispatcher) observer: crate::observation::Observer,
    /// 采样启停代数，拒绝旧会话的迟到结果；不同于 SQLite 任务领取 generation。
    pub(super) generation: u64,
    /// 可选磁盘冷却预约；预留成功不代表请求已发出，结束时按发送确定性结算。
    pub(super) lease: Option<crate::dht::persistence::CooldownLease>,
    pub(in crate::dht::dispatcher) node: DiscoveredNode,
    pub(in crate::dht::dispatcher) target: NodeId,
    pub(in crate::dht::dispatcher) kind: RequestKind,
    pub(super) permit: Option<mpsc::OwnedPermit<SampleBatch>>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Candidate {
    pub(super) node: DiscoveredNode,
    pub(super) visited: bool,
    pub(super) kind: RequestKind,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Cooldown {
    pub(super) until: Instant,
    pub(super) failures: u32,
}

/// 一次采样启停期间的候选和在途状态；一个 session 可以跨越多个随机 target 轮次。
#[derive(Debug)]
pub(super) struct SamplingSession {
    pub(super) observer: crate::observation::Observer,
    pub(super) config: SamplerConfig,
    pub(super) output: mpsc::Sender<SampleBatch>,
    pub(super) ready_permit: Option<mpsc::OwnedPermit<SampleBatch>>,
    pub(super) candidates: HashMap<NodeId, Candidate>,
    pub(super) in_flight: HashMap<NodeId, IpAddr>,
    pub(super) target: NodeId,
    pub(super) queries: usize,
    pub(super) next_send: Instant,
}

/// 候选筛选结果中的期限和容量原因用于安排下一次唤醒，不代表已经发出请求。
pub(super) struct CandidateSelection {
    pub(super) candidate: Option<Candidate>,
    pub(super) earliest: Option<Instant>,
    pub(super) tracking_full: bool,
}

/// dispatcher 独占的采样状态；session 管一次启停，冷却表和 durable 跨启停保留。
#[derive(Debug, Default)]
pub(in crate::dht::dispatcher) struct Sampler {
    pub(in crate::dht::dispatcher) observer: crate::observation::Observer,
    pub(super) generation: u64,
    pub(super) durable: Option<durable::Durable>,
    pub(super) session: Option<SamplingSession>,
    pub(super) ids: HashMap<NodeId, Cooldown>,
    pub(super) ips: HashMap<IpAddr, Instant>,
    pub(super) status: SamplerStatus,
    pub(in crate::dht::dispatcher) deadline: Option<Instant>,
}

impl Sampler {
    /// 校验配置并建立有界批次通道，更新启停代数；返回接收端不代表已经发包。
    /// 已运行、配置无效或存储故障时拒绝启动，不能用重启绕过冷却。
    pub(in crate::dht::dispatcher) fn start(
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
        self.session = Some(SamplingSession {
            observer: self.observer.child(crate::observation::Kind::Lifecycle),
            config,
            output,
            ready_permit: None,
            candidates: HashMap::new(),
            in_flight: HashMap::new(),
            target: NodeId(rand::random()),
            queries: 0,
            next_send: now,
        });
        if let Some(session) = &self.session {
            session.observer.emit(
                crate::observation::Kind::Sampling,
                "round",
                "started",
                || serde_json::json!({"target":crate::observation::hex(&session.target.0)}),
            );
        }
        self.status = SamplerStatus {
            running: true,
            ..Default::default()
        };
        self.deadline = Some(now);
        Ok(receiver)
    }

    pub(in crate::dht::dispatcher) fn status(&self) -> SamplerStatus {
        self.status.clone()
    }

    /// 容量恢复和 receiver 关闭都由 select 驱动，不能在网络响应处理里 await send。
    pub(in crate::dht::dispatcher) fn output_watch(&self) -> OutputWatch {
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
    pub(in crate::dht::dispatcher) fn accept_permit(
        &mut self,
        permit: mpsc::OwnedPermit<SampleBatch>,
    ) {
        if let Some(session) = &mut self.session {
            session.ready_permit = Some(permit);
        }
    }

    pub(super) fn cooldown(&mut self, node: DiscoveredNode, until: Instant, failures: u32) {
        self.ids
            .entry(node.id)
            .and_modify(|value| {
                value.until = value.until.max(until);
                value.failures = failures;
            })
            .or_insert(Cooldown { until, failures });
        self.ips
            .entry(node.address.ip())
            .and_modify(|value| *value = (*value).max(until))
            .or_insert(until);
    }

    /// 停止当前调度并处理已就绪但未发出的预约，跨会话冷却仍保留。
    /// 等待中的数据库预约与结算不在这里消失；外层仍须驱动 storage_event/flush_storage。
    pub(in crate::dht::dispatcher) fn stop(&mut self, now: Instant) {
        if let Some(request) = self
            .durable
            .as_mut()
            .and_then(|durable| durable.ready.take())
        {
            self.abandon_unsent(request, now);
        }
        // 在途请求的 interval 尚未知，按规范最大值保守等待；不能靠启停绕过冷却。
        if let Some(session) = self.session.take() {
            session.observer.emit(crate::observation::Kind::Sampling,"round","stopped",||serde_json::json!({"queries":session.queries,"in_flight":session.in_flight.len()}));
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

    pub(in crate::dht::dispatcher) fn inspection(&self) -> serde_json::Value {
        self.session.as_ref().map_or(serde_json::Value::Null, |session| {
            serde_json::json!({
                "target": crate::observation::hex(&session.target.0),
                "queries": session.queries,
                "candidates": session.candidates.values().map(|candidate| serde_json::json!({
                    "node_id": crate::observation::hex(&candidate.node.id.0),
                    "address": candidate.node.address.to_string(),
                    "visited": candidate.visited,
                    "fallback": candidate.kind == RequestKind::FindNodeFallback,
                    "cooldown_ms": self.ids.get(&candidate.node.id).map(|value| {
                        value.until.saturating_duration_since(std::time::Instant::now()).as_millis() as u64
                    }),
                })).collect::<Vec<_>>(),
            })
        })
    }
}
