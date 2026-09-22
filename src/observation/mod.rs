//! 当前进程的只读观测：有界事件历史与独立当前状态，不参与业务决策。
//! 生产者同步追加小事件，不执行 I/O；序号通知可合并，消费者按游标读取历史。
mod tests;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, time::Instant};

pub(crate) const MAX_EVENT_BYTES: usize = 8192;
const MAX_EVENTS: usize = 65_536;
const MAX_BYTES: usize = 32 * 1024 * 1024;
const RETENTION: Duration = Duration::from_secs(900);

/// 分类为稳定接口标签；步骤和结果由事实所有者给出，不使用 Debug 格式分类。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Lifecycle,
    Bootstrap,
    Routing,
    Rpc,
    Sampling,
    Discovery,
    Admission,
    Job,
    Lookup,
    Peer,
    Piece,
    Validation,
    Commit,
    Retry,
    Backpressure,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Context {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) generation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) peer_attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rpc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) observation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) batch_id: Option<String>,
}
/// 上下文只在启用且首次修改时分配；克隆句柄共享不可变关联字段。
#[derive(Debug, Clone, Default)]
pub(crate) struct TraceContext(Option<Arc<Context>>);
impl std::ops::Deref for TraceContext {
    type Target = Context;
    fn deref(&self) -> &Context {
        static EMPTY: Context = Context {
            node_id: None,
            hash: None,
            generation: None,
            parent_span_id: None,
            span_id: None,
            peer_attempt_id: None,
            rpc_id: None,
            observation_id: None,
            batch_id: None,
        };
        self.0.as_deref().unwrap_or(&EMPTY)
    }
}
impl std::ops::DerefMut for TraceContext {
    fn deref_mut(&mut self) -> &mut Context {
        Arc::make_mut(self.0.get_or_insert_with(|| Arc::new(Context::default())))
    }
}
impl Serialize for TraceContext {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        std::ops::Deref::deref(self).serialize(serializer)
    }
}

impl Context {
    fn matches(&self, filter: &Filter) -> bool {
        filter
            .hash
            .as_ref()
            .is_none_or(|h| self.hash.as_ref() == Some(h))
            && filter.object.as_ref().is_none_or(|id| {
                [
                    self.span_id.as_ref(),
                    self.parent_span_id.as_ref(),
                    self.peer_attempt_id.as_ref(),
                    self.rpc_id.as_ref(),
                    self.observation_id.as_ref(),
                    self.batch_id.as_ref(),
                ]
                .contains(&Some(id))
            })
    }
}
#[derive(Debug, Default)]
pub(crate) struct Filter {
    pub(crate) hash: Option<String>,
    pub(crate) object: Option<String>,
    pub(crate) kind: Option<Kind>,
}
#[derive(Debug)]
struct Entry {
    sequence: u64,
    at: Instant,
    context: TraceContext,
    kind: Kind,
    encoded: String,
    retained_bytes: usize,
}
#[derive(Debug)]
struct History {
    events: VecDeque<Entry>,
    bytes: usize,
    sequence: u64,
    evicted: u64,
    truncated: u64,
    active: BTreeMap<String, Value>,
    states: BTreeMap<&'static str, Value>,
}
#[derive(Debug)]
struct Hub {
    run_id: String,
    ids: AtomicU64,
    history: Mutex<History>,
    changed: watch::Sender<u64>,
    limits: Limits,
}
#[derive(Debug, Clone, Copy)]
struct Limits {
    count: usize,
    bytes: usize,
    age: Duration,
}

/// 空句柄是关闭状态；child/emit/state 均在构造数据前短路。
#[derive(Debug, Clone, Default)]
pub(crate) struct Observer {
    hub: Option<Arc<Hub>>,
    pub(crate) context: TraceContext,
}
#[derive(Debug, Serialize)]
pub(crate) struct Window {
    pub(crate) run_id: String,
    pub(crate) oldest: String,
    pub(crate) latest: String,
    pub(crate) retained: usize,
    pub(crate) bytes: usize,
    pub(crate) evicted: u64,
    pub(crate) truncated: u64,
    max_events: usize,
    max_bytes: usize,
    retention_ms: u64,
}
#[derive(Debug, Serialize)]
pub(crate) struct Page {
    pub(crate) events: Vec<Value>,
    pub(crate) next: String,
    pub(crate) window: Window,
    pub(crate) completeness: &'static str,
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
    fn with_limits(run_id: String, limits: Limits) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            hub: Some(Arc::new(Hub {
                run_id,
                ids: AtomicU64::new(1),
                history: Mutex::new(History {
                    events: VecDeque::new(),
                    bytes: 0,
                    sequence: 0,
                    evicted: 0,
                    truncated: 0,
                    active: BTreeMap::new(),
                    states: BTreeMap::new(),
                }),
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
        next.context.hash = Some(hex(hash));
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
        let mut event = json!({"schema_version":1,"run_id":hub.run_id,"sequence":sequence.to_string(),"at_ms":at_ms,"kind":kind,"step":step,"result":result,"context":self.context,"data":data,"truncated":truncated});
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
                self.context.hash.as_ref(),
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
        let observer = self.child(kind);
        observer.emit(kind, step, "started", || json!({}));
        observer.active(step);
        Span {
            observer,
            kind,
            step,
            start: Instant::now(),
            finished: false,
            dropped_result: "cancelled",
        }
    }
    fn active(&self, step: &'static str) {
        if let (Some(hub), Some(id)) = (&self.hub, &self.context.span_id) {
            let mut h = hub.history.lock().expect("观测锁");
            if h.active.len() < 2048 || h.active.contains_key(id) {
                h.active.insert(
                    id.clone(),
                    json!({"context":self.context,"step":step,"since_ms":wall_ms()}),
                );
            }
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
                let mut h = hub.history.lock().expect("观测锁");
                if h.states.len() < 16 || h.states.contains_key(key) {
                    h.states.insert(key, data);
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
        let h = hub.history.lock().expect("观测锁");
        json!({"sources":h.states,"active":h.active.values().collect::<Vec<_>>()})
    }
    pub(crate) fn window(&self) -> Window {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut h = hub.history.lock().expect("观测锁");
        h.evict(hub.limits);
        h.window(hub)
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.hub.as_ref().expect("已启用监控").changed.subscribe()
    }
    /// after 为全局序号；过滤掉的记录也推进游标，避免消费者在空页重复扫描。
    pub(crate) fn page(&self, after: u64, limit: usize, filter: &Filter) -> Page {
        let hub = self.hub.as_ref().expect("已启用监控");
        let mut h = hub.history.lock().expect("观测锁");
        h.evict(hub.limits);
        let mut next = after;
        let mut events = Vec::new();
        for entry in h.events.iter().filter(|e| e.sequence > after) {
            next = entry.sequence;
            if entry.context.matches(filter) && filter.kind.is_none_or(|k| k == entry.kind) {
                events.push(serde_json::from_str(&entry.encoded).expect("自产事件 JSON"));
                if events.len() >= limit.clamp(1, 100) {
                    break;
                }
            }
        }
        let completeness = if h.evicted > 0 { "partial" } else { "complete" };
        Page {
            events,
            next: next.to_string(),
            window: h.window(hub),
            completeness,
        }
    }
}
impl History {
    fn evict(&mut self, limits: Limits) {
        while self.events.front().is_some_and(|e| {
            self.events.len() > limits.count
                || self.bytes > limits.bytes
                || e.at.elapsed() >= limits.age
        }) {
            let entry = self.events.pop_front().expect("已有首项");
            self.bytes -= entry.retained_bytes;
            self.evicted += 1;
        }
    }
    fn window(&self, hub: &Hub) -> Window {
        Window {
            run_id: hub.run_id.clone(),
            oldest: self
                .events
                .front()
                .map_or(self.sequence + 1, |e| e.sequence)
                .to_string(),
            latest: self.sequence.to_string(),
            retained: self.events.len(),
            bytes: self.bytes,
            evicted: self.evicted,
            truncated: self.truncated,
            max_events: hub.limits.count,
            max_bytes: hub.limits.bytes,
            retention_ms: hub.limits.age.as_millis() as u64,
        }
    }
}
/// 阶段 guard 在普通丢弃时记录取消；事务闭包可将它一起移入，提交后才 finish。
#[derive(Debug)]
pub(crate) struct Span {
    pub(crate) observer: Observer,
    kind: Kind,
    step: &'static str,
    start: Instant,
    finished: bool,
    dropped_result: &'static str,
}
impl Span {
    /// 调用者取消仅发出请求，资源退出由持有者的事件另行确认。
    pub(crate) fn cancellation_request_on_drop(&mut self) {
        self.dropped_result = "cancel_requested";
    }

    pub(crate) fn executing(&mut self) {
        self.dropped_result = "failed";
    }
    pub(crate) fn finish(&mut self, result: &'static str) {
        if self.finished {
            return;
        }
        self.observer.emit(
            self.kind,
            self.step,
            result,
            || json!({"elapsed_ms":self.start.elapsed().as_millis() as u64}),
        );
        if let (Some(hub), Some(id)) = (&self.observer.hub, &self.observer.context.span_id) {
            hub.history.lock().expect("观测锁").active.remove(id);
        }
        self.finished = true;
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        self.finish(self.dropped_result);
    }
}
pub(crate) fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}
fn bound(value: &mut Value, truncated: &mut bool, depth: usize) {
    if depth > 8 {
        *value = Value::Null;
        *truncated = true;
        return;
    }
    match value {
        Value::String(s) if s.len() > 1024 => {
            let mut end = 1024;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            s.truncate(end);
            *truncated = true;
        }
        Value::Array(a) => {
            if a.len() > 32 {
                a.truncate(32);
                *truncated = true;
            }
            for v in a {
                bound(v, truncated, depth + 1);
            }
        }
        Value::Object(o) => {
            for v in o.values_mut() {
                bound(v, truncated, depth + 1);
            }
        }
        _ => {}
    }
}
