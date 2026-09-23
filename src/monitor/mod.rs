//! Loopback 只读 HTTP/SSE 服务；Session 持有关闭责任，监控故障不改变采集策略。
mod api;
mod tests;
use crate::{
    collection::{
        inspection::{CollectionInspection, ReadError},
        store::CollectionStore,
    },
    dht::dispatcher::DhtHandle,
    observation::{Kind, Observer, wall_ms},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

/// 协议层读取请求头的期限，不限制 SSE 响应持续时间。
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Monitor {
    connections: Arc<Mutex<JoinSet<()>>>,
    tasks: JoinSet<()>,
    stop: CancellationToken,
    finish: CancellationToken,
    state: Arc<State>,
}
struct State {
    observer: Observer,
    store: CollectionStore,
    nodes: Vec<DhtHandle>,
    cache: Mutex<Value>,
    requests: Arc<Semaphore>,
    database: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    rate: Mutex<(Instant, f64)>,
    stop: CancellationToken,
    finish: CancellationToken,
}

/// 对外 snapshot 的稳定外层；内部来源仍由各状态所有者提供有界 JSON。
#[derive(Serialize)]
struct Snapshot {
    schema_version: u8,
    window: crate::observation::Window,
    runtime: Value,
    cached: Value,
}
impl Monitor {
    /// listener 在启动时已绑定；本对象拥有 HTTP 与刷新任务，Drop 仅是异常取消兜底。
    pub(crate) fn start(
        listener: TcpListener,
        store: CollectionStore,
        nodes: Vec<DhtHandle>,
        observer: Observer,
    ) -> Self {
        observer.state(
            "monitor",
            || json!({"phase":"running","listen":listener.local_addr().ok().map(|a|a.to_string())}),
        );
        let stop = CancellationToken::new();
        let finish = CancellationToken::new();
        let state = Arc::new(State {
            observer,
            store,
            nodes,
            cache: Mutex::new(json!({"nodes":[],"database":{"available":false}})),
            requests: Arc::new(Semaphore::new(4)),
            database: Arc::new(Semaphore::new(1)),
            streams: Arc::new(Semaphore::new(4)),
            rate: Mutex::new((Instant::now(), 10.0)),
            stop: stop.clone(),
            finish: finish.clone(),
        });
        let mut tasks = JoinSet::new();
        let connections = Arc::new(Mutex::new(JoinSet::new()));
        tasks.spawn(serve(listener, state.clone(), connections.clone()));
        tasks.spawn(refresh(state.clone()));
        tasks.spawn(backfill_catalog(state.clone()));
        Self {
            connections,
            tasks,
            stop,
            finish,
            state,
        }
    }
    pub(crate) fn begin_shutdown(&self) {
        self.stop.cancel();
        self.state
            .observer
            .state("monitor", || json!({"phase":"shutting_down"}));
    }
    pub(crate) async fn changed(&mut self) {
        if self.tasks.is_empty() {
            std::future::pending::<()>().await;
        }
        if let Some(result) = self.tasks.join_next().await {
            self.state.observer.emit(
                Kind::Lifecycle,
                "monitor",
                "failed",
                || json!({"panicked":result.is_err()}),
            );
            tracing::warn!("监控任务提前结束，停止监控；采集继续运行");
            self.stop.cancel();
            self.finish.cancel();
        }
    }
    pub(crate) async fn finish(&mut self, deadline: Instant) {
        self.begin_shutdown();
        self.finish.cancel();
        let until = deadline.min(Instant::now() + Duration::from_secs(1));
        if tokio::time::timeout_at(until, async {
            while let Some(result) = self.tasks.join_next().await {
                if let Err(error) = result {
                    tracing::warn!(%error,"监控任务退出异常");
                }
            }
        })
        .await
        .is_err()
        {
            self.tasks.abort_all();
            while self.tasks.join_next().await.is_some() {}
        }
        // 连接集合属于 Monitor，服务任务被中止也不会失去退出确认。
        let mut connections =
            std::mem::take(&mut *self.connections.lock().expect("HTTP 连接集合锁"));
        if tokio::time::timeout_at(until, async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.cancel();
        self.finish.cancel();
        self.tasks.abort_all();
        self.connections
            .lock()
            .expect("HTTP 连接集合锁")
            .abort_all();
    }
}
impl State {
    fn database_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, ReadError> {
        self.database
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReadError::Busy)
    }

    async fn stats(&self) -> Result<CollectionInspection, ReadError> {
        let permit = self.database_permit()?;
        let cancel = self.stop.child_token();
        let _guard = cancel.clone().drop_guard();
        tokio::time::timeout(Duration::from_secs(2), self.store.inspect(permit, cancel))
            .await
            .map_err(|_| ReadError::Cancelled)?
    }
    /// 概览不复制路由联系人；首页只读取缓存中的摘要统计。
    fn cached_summary(&self) -> Value {
        let mut cache = self.cache.lock().expect("监控缓存锁").clone();
        if let Some(nodes) = cache["nodes"].as_array_mut() {
            for node in nodes {
                if let Some(routing) = node["routing"].as_object_mut() {
                    let count = routing
                        .remove("contacts")
                        .and_then(|contacts| contacts.as_array().map(Vec::len));
                    routing.insert("contact_count".into(), json!(count));
                }
            }
        }
        cache
    }
    fn snapshot(&self) -> Value {
        serde_json::to_value(Snapshot {
            schema_version: 1,
            window: self.observer.window(),
            runtime: self.observer.states(),
            cached: self.cached_summary(),
        })
        .expect("监控 snapshot 只包含可序列化状态")
    }
}

/// 旧 metadata 每轮只补一条；HTTP 查询占用数据库许可时直接让出本轮。
async fn backfill_catalog(state: Arc<State>) {
    let mut cursor = None;
    loop {
        if state.stop.is_cancelled() {
            break;
        }
        let Ok(permit) = state.database_permit() else {
            tokio::select! {
                _ = state.stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
            }
        };
        let cancel = state.stop.child_token();
        let _guard = cancel.clone().drop_guard();
        match state
            .store
            .backfill_catalog_one(cursor, permit, cancel)
            .await
        {
            Ok(step) => {
                cursor = step.cursor;
                if step.complete {
                    state.stop.cancelled().await;
                    break;
                }
            }
            Err(ReadError::Cancelled) if state.stop.is_cancelled() => break,
            Err(_) => {}
        }
        tokio::select! {
            _ = state.stop.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

fn cache_database(cache: &mut Value, observed_at_ms: u64, value: impl Serialize) {
    cache["database"] = json!({
        "available": true,
        "stale": false,
        "observed_at_ms": observed_at_ms,
        "value": value,
    });
}

fn mark_database_stale(cache: &mut Value) {
    cache["database"]["stale"] = json!(true);
}

async fn refresh(state: Arc<State>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cycle = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = state.stop.cancelled() => break,
            _ = interval.tick() => {}
        }
        let update = async {
            let mut nodes = Vec::new();
            for (id, node) in state.nodes.iter().enumerate() {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    node.inspect(cycle.is_multiple_of(5)),
                )
                .await
                {
                    Ok(Ok(mut snapshot)) => {
                        snapshot["id"] = json!(id.to_string());
                        snapshot["observed_at_ms"] = json!(wall_ms());
                        nodes.push(snapshot);
                    }
                    _ => nodes.push(
                        json!({"id":id.to_string(),"available":false,"observed_at_ms":wall_ms()}),
                    ),
                }
            }
            {
                let mut cache = state.cache.lock().expect("监控缓存锁");
                for (id, node) in nodes.iter_mut().enumerate() {
                    if node.get("routing").is_none() {
                        node["routing"] = cache["nodes"][id]["routing"].clone();
                    }
                }
                if let Some(first) = nodes.first_mut().and_then(Value::as_object_mut)
                    && let Some(traffic) = first.remove("traffic")
                {
                    cache["traffic"] = traffic;
                }
                for node in &mut nodes {
                    if let Some(node) = node.as_object_mut() {
                        node.remove("traffic");
                    }
                }
                cache["nodes"] = json!(nodes);
            }
            if cycle.is_multiple_of(30) {
                if let Some(database) = state.observer.get_state("database").filter(|v| {
                    v["observed_at_ms"]
                        .as_u64()
                        .is_some_and(|at| wall_ms().saturating_sub(at) < 30_000)
                }) {
                    cache_database(
                        &mut state.cache.lock().expect("监控缓存锁"),
                        database["observed_at_ms"].as_u64().expect("自产观察时间"),
                        &database["value"],
                    );
                    return;
                }
                let result = state.stats().await;
                let mut cache = state.cache.lock().expect("监控缓存锁");
                match result {
                    Ok(value) => cache_database(&mut cache, wall_ms(), value),
                    Err(_) => mark_database_stale(&mut cache),
                }
            }
        };
        tokio::select! {
            biased;
            _ = state.stop.cancelled() => break,
            _ = update => {}
        }
        cycle = cycle.wrapping_add(1);
    }
}
async fn serve(listener: TcpListener, state: Arc<State>, connections: Arc<Mutex<JoinSet<()>>>) {
    let router = api::router(state.clone());
    loop {
        tokio::select! {
            biased;
            _=state.finish.cancelled()=>break,
            result=std::future::poll_fn(|cx|connections.lock().expect("HTTP 连接集合锁").poll_join_next(cx)),if !connections.lock().expect("HTTP 连接集合锁").is_empty()=>{if let Some(Err(error))=result{tracing::warn!(%error,"HTTP 连接任务退出异常");}},
            accepted=listener.accept(),if !state.stop.is_cancelled()&&connections.lock().expect("HTTP 连接集合锁").len()<16=>{
                match accepted{
                    Ok((socket,_))=>{
                        let service=hyper_util::service::TowerToHyperService::new(router.clone());
                        let finish=state.finish.clone();
                        connections.lock().expect("HTTP 连接集合锁").spawn(async move{
                            let io=hyper_util::rt::TokioIo::new(socket);
                            let mut builder=hyper::server::conn::http1::Builder::new();
                            builder.timer(hyper_util::rt::TokioTimer::new()).header_read_timeout(HEADER_READ_TIMEOUT);
                            let connection=builder.serve_connection(io,service);tokio::pin!(connection);
                            tokio::select!{_=finish.cancelled()=>{connection.as_mut().graceful_shutdown();let _=tokio::time::timeout(Duration::from_millis(900),&mut connection).await;},_= &mut connection=>{}}
                        });
                    }
                    Err(error)=>{state.observer.emit(Kind::Lifecycle,"monitor","failed",||json!({"error":error.to_string()}));tracing::warn!(%error,"HTTP 监听失败，停止监控服务");break;}
                }
            }
        }
    }
    drop(listener);
}

pub(crate) async fn bind(address: SocketAddr) -> std::io::Result<TcpListener> {
    if !address.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "监控只能监听 loopback",
        ));
    }
    TcpListener::bind(address).await
}
