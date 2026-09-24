//! Hub 与 Observer：事件追加、当前状态、分页和订阅。

use super::{
    Filter, Kind, Page, Span, TraceContext, Window, hex,
    history::{
        Entry, History, Limits, MAX_BYTES, MAX_EVENT_BYTES, MAX_EVENTS, RETENTION, bound, matches,
    },
    wall_ms,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::{sync::watch, time::Instant};

#[derive(Debug)]
pub(super) struct Hub {
    run_id: String,
    ids: AtomicU64,
    history: Mutex<History>,
    changed: watch::Sender<u64>,
    limits: Limits,
}

/// 空句柄是关闭状态；child/emit/state 均在构造数据前短路。
#[derive(Debug, Clone, Default)]
pub(crate) struct Observer {
    hub: Option<Arc<Hub>>,
    pub(crate) context: TraceContext,
}

impl Observer {
    pub(crate) fn new(run_id: String) -> Self {
        Self::with_limits(
            run_id,
            Limits {
                count: MAX_EVENTS,
                bytes: MAX_BYTES,
                age: RETENTION,
            },
        )
    }

    pub(super) fn with_limits(run_id: String, limits: Limits) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            hub: Some(Arc::new(Hub {
                run_id,
                ids: AtomicU64::new(1),
                history: Mutex::new(History::new()),
                changed,
                limits,
            })),
            context: TraceContext::default(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.hub.is_some()
    }

    pub(crate) fn child(&self, kind: Kind) -> Self {
        let Some(hub) = &self.hub else {
            return Self::default();
        };
        let id = hub.ids.fetch_add(1, Ordering::Relaxed).to_string();
        let mut context = self.context.clone();
        context.parent_span_id = context.span_id.take();
        context.span_id = Some(id.clone());
        match kind {
            Kind::Rpc if context.rpc_id.is_none() => context.rpc_id = Some(id),
            Kind::Peer if context.peer_attempt_id.is_none() => context.peer_attempt_id = Some(id),
            Kind::Discovery if context.observation_id.is_none() => {
                context.observation_id = Some(id)
            }
            Kind::Sampling if context.batch_id.is_none() => context.batch_id = Some(id),
            _ => {}
        }
        Self {
            hub: self.hub.clone(),
            context,
        }
    }

    pub(crate) fn for_hash(&self, hash: &[u8; 20]) -> Self {
        if !self.enabled() {
            return Self::default();
        }
        let mut next = self.clone();
        next.context.swarm_key = Some(hex(hash));
        next
    }

    pub(crate) fn for_job(&self, hash: &[u8; 20], generation: i64) -> Self {
        let mut next = self.for_hash(hash);
        if next.enabled() {
            next.context.generation = Some(generation);
            next.context.span_id = Some(format!("job:{}:{generation}", hex(hash)));
        }
        next
    }

    /// 长字段有明确截断标记；绝不保存正文或依赖消费者速度保留缓存引用。
    pub(crate) fn emit(
        &self,
        kind: Kind,
        step: &'static str,
        result: &'static str,
        data: impl FnOnce() -> Value,
    ) {
        let Some(hub) = &self.hub else { return };
        let mut data = data();
        let mut truncated = false;
        bound(&mut data, &mut truncated, 0);
        let mut history = hub.history.lock().expect("观测锁");
        history.evict(hub.limits);
        history.sequence += 1;
        let sequence = history.sequence;
        let at_ms = wall_ms();
        let mut event = json!({"schema_version":2,"run_id":hub.run_id,"sequence":sequence.to_string(),"at_ms":at_ms,"kind":kind,"step":step,"result":result,"context":self.context,"data":data,"truncated":truncated});
        let mut encoded = event.to_string();
        if encoded.len() > MAX_EVENT_BYTES {
            event["data"] = Value::Null;
            event["truncated"] = json!(true);
            truncated = true;
            encoded = event.to_string();
        }
        history.truncated += u64::from(truncated);
        let retained_bytes = encoded.capacity()
            + [
                self.context.swarm_key.as_ref(),
                self.context.node_id.as_ref(),
                self.context.parent_span_id.as_ref(),
                self.context.span_id.as_ref(),
                self.context.peer_attempt_id.as_ref(),
                self.context.rpc_id.as_ref(),
                self.context.observation_id.as_ref(),
                self.context.batch_id.as_ref(),
            ]
            .into_iter()
            .flatten()
            .map(String::len)
            .sum::<usize>();
        history.bytes += retained_bytes;
        history.events.push_back(Entry {
            sequence,
            at: Instant::now(),
            context: self.context.clone(),
            kind,
            encoded,
            retained_bytes,
        });
        history.evict(hub.limits);
        drop(history);
        hub.changed.send_replace(sequence);
    }

    pub(crate) fn span(&self, kind: Kind, step: &'static str) -> Span {
        Span::new(self.child(kind), kind, step)
    }

    pub(super) fn active(&self, step: &'static str) {
        if let (Some(hub), Some(id)) = (&self.hub, &self.context.span_id) {
            let mut history = hub.history.lock().expect("观测锁");
            if history.active.len() < 2048 || history.active.contains_key(id) {
                history.active.insert(
                    id.clone(),
                    json!({"context":self.context,"step":step,"since_ms":wall_ms()}),
                );
            }
        }
    }

    pub(super) fn finish_active(&self) {
        if let (Some(hub), Some(id)) = (&self.hub, &self.context.span_id) {
            hub.history.lock().expect("观测锁").active.remove(id);
        }
    }

    pub(crate) fn progress(&self, data: impl FnOnce() -> Value) {
        if let (Some(hub), Some(id)) = (&self.hub, &self.context.span_id) {
            let data = data();
            if data.to_string().len() > MAX_EVENT_BYTES {
                return;
            }
            if let Some(active) = hub.history.lock().expect("观测锁").active.get_mut(id) {
                active["data"] = data;
                active["observed_at_ms"] = json!(wall_ms());
            }
        }
    }

    pub(crate) fn state(&self, key: &'static str, data: impl FnOnce() -> Value) {
        if let Some(hub) = &self.hub {
            let data = json!({"observed_at_ms":wall_ms(),"value":data()});
            if data.to_string().len() <= 256 * 1024 {
                let mut history = hub.history.lock().expect("观测锁");
                if history.states.len() < 16 || history.states.contains_key(key) {
                    history.states.insert(key, data);
                }
            }
        }
    }

    pub(crate) fn get_state(&self, key: &str) -> Option<Value> {
        self.hub
            .as_ref()?
            .history
            .lock()
            .expect("观测锁")
            .states
            .get(key)
            .cloned()
    }

    pub(crate) fn states(&self) -> Value {
        let Some(hub) = &self.hub else {
            return json!({});
        };
        let history = hub.history.lock().expect("观测锁");
        json!({"sources":history.states,"active":history.active.values().collect::<Vec<_>>()})
    }

    pub(crate) fn window(&self) -> Window {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut history = hub.history.lock().expect("观测锁");
        history.evict(hub.limits);
        history.window(&hub.run_id, hub.limits)
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.hub.as_ref().expect("已启用监控").changed.subscribe()
    }

    /// after 为全局序号；过滤掉的记录也推进游标，避免消费者在空页重复扫描。
    pub(crate) fn page(&self, after: u64, limit: usize, filter: &Filter) -> Page {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut history = hub.history.lock().expect("观测锁");
        history.evict(hub.limits);
        let mut next = after;
        let mut events = Vec::new();
        for entry in history.events.iter().filter(|entry| entry.sequence > after) {
            next = entry.sequence;
            if matches(entry, filter) {
                events.push(serde_json::from_str(&entry.encoded).expect("自产事件 JSON"));
                if events.len() >= limit.clamp(1, 100) {
                    break;
                }
            }
        }
        let completeness = if history.evicted > 0 {
            "partial"
        } else {
            "complete"
        };
        Page {
            events,
            next: next.to_string(),
            window: history.window(&hub.run_id, hub.limits),
            completeness,
        }
    }
}
