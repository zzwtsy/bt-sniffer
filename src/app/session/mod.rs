//! 应用运行会话；不负责绑定公网 socket，也不隐式配置 bootstrap。
//!
//! 使用顺序是 open → add_node → 按需 start_fetch/start_sampling → shutdown。
//! add_node 接收调用者已经创建的 socket，IPv4 和 IPv6 可共享一个 session。
//! 强类型故障状态区分可降级的写入错误和必须退出的任务故障。
//! 存储故障后暂停自动采集；通过显式关闭并重新打开会话恢复，不自动重试发包。
//! 尚未提交的结果可能随进程崩溃丢失；已经提交的 hash 写入可以安全重复执行。
//!
//! app 持有会话；会话拥有采集器、节点及数据库的关闭责任，各清理阶段共用同一个期限。
mod faults;
use crate::address::AddressPolicy;
use crate::clock::unix_millis;
use crate::collection::ingest::SampleIngest;
use crate::collection::store::CollectionStore;
use crate::dht::dispatcher::DhtDispatcher;
use crate::dht::dispatcher::DhtDispatcherConfig;
use crate::dht::dispatcher::DhtHandle;
use crate::dht::dispatcher::DispatcherExit;
#[cfg(test)]
use crate::dht::dispatcher::SampleBatch;
use crate::dht::dispatcher::SamplerConfig;
use crate::dht::persistence::DhtStore;
use crate::dht::persistence::identity;
use crate::dht::persistence::identity::LocalIdentity;
use crate::dht::routing::AddressFamily;
use crate::dht::routing::RoutingTable;
use crate::dht::transaction::TransactionManager;
use crate::dht::udp::UdpTransport;
use crate::storage::Storage;
use crate::storage::StorageConfig;
use crate::storage::StorageError;
use faults::classify_collector_error;
pub(crate) use faults::{FaultLog, FaultReporter, SessionFault};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
#[cfg(test)]
use tokio::sync::mpsc;
use tokio::{
    sync::watch,
    task::{Id, JoinSet},
};
use tokio_util::sync::CancellationToken;

struct Node {
    identity: LocalIdentity,
    handle: DhtHandle,
    exit: Option<DispatcherExit>,
    collection: Option<SampleIngest>,
    collecting: bool,
}
/// 会话按 task ID 保存的任务角色；节点任务携带 nodes 下标，全局采集协调器没有节点下标。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskRole {
    Dispatcher(usize),
    Snapshot(usize),
    SampleCollector(usize),
    FetchCoordinator,
}
impl TaskRole {
    fn node(self) -> usize {
        match self {
            Self::Dispatcher(index) | Self::Snapshot(index) | Self::SampleCollector(index) => index,
            Self::FetchCoordinator => unreachable!("全局采集协调器没有节点索引"),
        }
    }
}
impl std::fmt::Display for TaskRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dispatcher(node) => write!(f, "节点 {node} dispatcher"),
            Self::Snapshot(node) => write!(f, "节点 {node} snapshot"),
            Self::SampleCollector(node) => write!(f, "节点 {node} sample collector"),
            Self::FetchCoordinator => f.write_str("全局 metadata 采集协调器"),
        }
    }
}
/// 同一个任务返回结果，在运行期可能是异常退出，在关闭期可能是预期完成。
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskPhase {
    Running,
    ShuttingDown,
}

enum TaskOutput {
    Dispatcher(DispatcherExit),
    Snapshot(Result<(), StorageError>),
    Collector(SampleIngest),
    Fetch(Result<(), Vec<crate::collection::CollectorError>>),
}
/// 会话是运行资源的唯一关闭入口；Drop 只中止任务，完成落盘必须显式 await shutdown。
pub(crate) struct Session {
    budget: std::sync::Arc<crate::dht::traffic::Budget>,
    storage: Option<Storage>,
    collection_store: CollectionStore,
    dht_store: DhtStore,
    nodes: Vec<Node>,
    stop_snapshots: CancellationToken,
    stop_fetch: CancellationToken,
    tasks: JoinSet<TaskOutput>,
    roles: HashMap<Id, TaskRole>,
    report: FaultReporter,
    errors: watch::Receiver<Option<SessionFault>>,
    faults: FaultLog,
    shutdown_stage: String,
}
impl Drop for Session {
    fn drop(&mut self) {
        // 超时或调用者直接丢弃 session 时，不留下继续联网的孤儿任务。
        self.stop_snapshots.cancel();
        self.stop_fetch.cancel();
        self.tasks.abort_all();
    }
}
impl Session {
    #[cfg(test)]
    pub(crate) fn test_budget(&self) -> std::sync::Arc<crate::dht::traffic::Budget> {
        self.budget.clone()
    }
    pub(crate) fn log_traffic(&self) {
        self.budget.log();
    }
    #[cfg(test)]
    pub(crate) fn test_store(&self) -> CollectionStore {
        self.collection_store.clone()
    }
    /// 测试使用默认流量配置；生产由应用注入共享配额。
    #[cfg(test)]
    pub(crate) async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        Self::open_with_traffic(config, crate::dht::traffic::Config::default()).await
    }
    /// 创建共享配额、故障通道并等待数据库打开；尚未接收 socket 或启动采集任务。
    /// 返回的 Session 是关闭所有资源的唯一入口，后续 add_node/start_fetch 仍可能失败。
    #[cfg(test)]
    pub(crate) async fn open_with_traffic(
        config: StorageConfig,
        traffic: crate::dht::traffic::Config,
    ) -> Result<Self, StorageError> {
        let budget = std::sync::Arc::new(
            crate::dht::traffic::Budget::new(traffic).map_err(StorageError::Invalid)?,
        );
        Self::open_with_budget(config, budget).await
    }
    /// 所有节点使用应用已经创建的同一配额，构造过程不创建临时预算。
    pub(crate) async fn open_with_budget(
        config: StorageConfig,
        budget: std::sync::Arc<crate::dht::traffic::Budget>,
    ) -> Result<Self, StorageError> {
        let (report, errors) = FaultReporter::new();
        let storage = Storage::open(config).await?;
        let collection_store = CollectionStore::new(storage.handle.clone());
        let dht_store = DhtStore::new(storage.handle.clone());
        Ok(Self {
            budget,
            storage: Some(storage),
            collection_store,
            dht_store,
            nodes: Vec::new(),
            stop_snapshots: CancellationToken::new(),
            stop_fetch: CancellationToken::new(),
            tasks: JoinSet::new(),
            roles: HashMap::new(),
            report,
            errors,
            faults: FaultLog::default(),
            shutdown_stage: "尚未开始".into(),
        })
    }
    /// 同一个 session 可容纳 IPv4 和 IPv6，二者共用数据库线程但不共用路由表。
    pub(crate) async fn add_node(
        &mut self,
        instance: &str,
        transport: UdpTransport,
        transactions: TransactionManager,
        config: DhtDispatcherConfig,
        policy: AddressPolicy,
    ) -> Result<DhtHandle, StorageError> {
        let store = self.dht_store.clone();
        let address = transport
            .local_addr()
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let family = if address.is_ipv4() {
            AddressFamily::Ipv4
        } else {
            AddressFamily::Ipv6
        };
        let identity =
            identity::load_or_create(&store, instance, family, unix_millis(SystemTime::now())?)
                .await?;
        if self
            .nodes
            .iter()
            .any(|node| node.identity.key == identity.key)
        {
            return Err(StorageError::Conflict);
        }
        let contacts = store.load_contacts(identity).await?;
        let cooldowns = store
            .restore_cooldowns(identity, unix_millis(SystemTime::now())?)
            .await?;
        let table = RoutingTable::new(
            identity.node_id,
            family,
            tokio::time::Instant::now().into_std(),
        );
        let (mut dispatcher, handle) =
            DhtDispatcher::with_budget(transport, table, transactions, config, self.budget.clone())
                .map_err(|e| StorageError::Database(e.to_string()))?;
        dispatcher.attach_storage(store.clone(), identity, contacts, cooldowns, policy)?;
        let report = self.report.clone();
        dispatcher.report_storage_errors_to(report.storage_callback());
        let index = self.nodes.len();
        let task = self
            .tasks
            .spawn(async move { TaskOutput::Dispatcher(dispatcher.run_persistent().await) });
        self.roles.insert(task.id(), TaskRole::Dispatcher(index));
        let stop = self.stop_snapshots.clone();
        let snapshot_handle = handle.clone();
        let snapshot = self.tasks.spawn(async move {
            let mut last = Vec::new();
            let mut timer = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(60),
                Duration::from_secs(60),
            );
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = timer.tick() => {} }
                let operation = async {
                    let contacts = snapshot_handle.routing_snapshot().await?;
                    if contacts != last {
                        store.save_contacts(identity, &contacts).await?;
                        last = contacts;
                    }
                    Ok::<_, StorageError>(())
                };
                let result =
                    tokio::select! { _ = stop.cancelled() => break, result = operation => result };
                if let Err(error) = result {
                    return TaskOutput::Snapshot(Err(error));
                }
            }
            TaskOutput::Snapshot(Ok(()))
        });
        self.roles.insert(snapshot.id(), TaskRole::Snapshot(index));
        self.nodes.push(Node {
            identity,
            handle: handle.clone(),
            exit: None,
            collection: None,
            collecting: false,
        });
        Ok(handle)
    }
    /// 在已有节点上启动唯一的采集协调器；返回前恢复任务并安装宣布入口。
    /// 至少需要一个节点且不能重复启动；部分初始化失败不会自动撤销已提交的数据库更新。
    pub(crate) async fn start_fetch(
        &mut self,
        config: crate::collection::Config,
    ) -> Result<(), crate::collection::CollectorError> {
        if self.nodes.is_empty()
            || self
                .roles
                .values()
                .any(|role| *role == TaskRole::FetchCoordinator)
        {
            return Err(StorageError::Conflict.into());
        }
        let collector = crate::collection::Collector::new(
            self.collection_store.clone(),
            self.nodes.iter().map(|n| n.handle.clone()).collect(),
            config,
            crate::clock::Clock::default(),
            self.stop_fetch.clone(),
            self.report.pause_notifications(),
            self.report.collector_callback(self.faults.clone()),
        )
        .await?;
        let task = self
            .tasks
            .spawn(async move { TaskOutput::Fetch(collector.run().await) });
        self.roles.insert(task.id(), TaskRole::FetchCoordinator);
        Ok(())
    }
    /// 调用者显式启动采样；自动下载由独立的 start_fetch 启动。
    pub(crate) async fn start_sampling(
        &mut self,
        node: usize,
        config: SamplerConfig,
    ) -> Result<(), StorageError> {
        let store = self.collection_store.clone();
        let index = node;
        let node = self
            .nodes
            .get_mut(node)
            .ok_or(StorageError::Invalid("节点索引无效"))?;
        if node.collecting {
            return Err(StorageError::Conflict);
        }
        let receiver = node
            .handle
            .start_sampling(config)
            .await
            .map_err(|e| StorageError::Database(e.to_string()))?;
        let handle = node.handle.clone();
        let report = self.report.clone();
        let task = self.tasks.spawn(async move {
            let mut state = SampleIngest::new(receiver, crate::clock::Clock::default());
            if let Err(error) = state.run(&store).await {
                report.publish(SessionFault::StorageWrite(error.clone()));
                let _ = handle.pause_for_storage(error).await;
            }
            TaskOutput::Collector(state)
        });
        node.collecting = true;
        self.roles
            .insert(task.id(), TaskRole::SampleCollector(index));
        Ok(())
    }
    /// 返回所有遇到的错误，不用保存失败掩盖最初的 socket 故障。
    pub(crate) async fn shutdown(mut self) -> Result<(), Vec<String>> {
        match tokio::time::timeout(Duration::from_secs(30), self.shutdown_inner()).await {
            Ok(result) => result,
            Err(_) => {
                let mut errors = self.faults.take();
                if let Some(fault) = self.errors.borrow().as_ref() {
                    errors.push(fault.to_string());
                }
                errors.push(format!(
                    "持久化关闭超过 30 秒（阶段：{}）；未确认的结果可能尚未落盘",
                    self.shutdown_stage
                ));
                Err(errors)
            }
        }
    }
    /// 与应用的信号和引导任务一起等待，不轮询 JoinHandle，也不重复消费任务结果。
    pub(crate) async fn next_fault(&mut self) -> SessionFault {
        loop {
            if self.errors.has_changed().unwrap_or(false) {
                let fault = self.errors.borrow().clone();
                if let Some(fault) = fault {
                    if let SessionFault::StorageWrite(error) = &fault {
                        // 完成全体停产后才标记已读；future 被 select 取消时，下次继续处理。
                        futures_util::future::join_all(
                            self.nodes
                                .iter()
                                .map(|node| node.handle.pause_for_storage(error.clone())),
                        )
                        .await;
                    }
                    self.errors.borrow_and_update();
                    return fault;
                }
            }
            tokio::select! {
                _ = self.storage.as_ref().unwrap().handle.closed() => {
                    self.report.publish(SessionFault::DatabaseExited);
                }
                result = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    self.accept_task(result.expect("仍有受监督任务"), TaskPhase::Running);
                }
                // changed 会标记已读，因此这里只等待一个临时接收者。
                _ = async {
                    let mut receiver = self.errors.clone();
                    let _ = receiver.changed().await;
                } => {}
            }
        }
    }

    /// 消费一个任务结果并移除角色映射；同样的退出在运行期和关闭期有不同故障含义。
    fn accept_task(
        &mut self,
        result: Result<(Id, TaskOutput), tokio::task::JoinError>,
        phase: TaskPhase,
    ) {
        let closing = phase == TaskPhase::ShuttingDown;
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let role = self.roles.remove(&id).expect("每个任务都有角色");
        let fault = match result {
            Ok((_, TaskOutput::Fetch(result))) => match result {
                Err(errors) => {
                    for error in errors {
                        let fault = classify_collector_error(&error);
                        self.report.publish(fault);
                    }
                    None
                }
                Ok(()) if !closing => Some(SessionFault::TaskExited {
                    role,
                    detail: "采集协调器提前结束".into(),
                }),
                Ok(()) => None,
            },
            Ok((_, TaskOutput::Dispatcher(exit))) => {
                let detail = match &exit.network_result {
                    Ok(()) => "正常返回但没有收到关闭请求".into(),
                    Err(error) => error.to_string(),
                };
                self.nodes[role.node()].exit = Some(exit);
                (!closing).then_some(SessionFault::TaskExited { role, detail })
            }
            Ok((_, TaskOutput::Snapshot(result))) => match result {
                Err(error) => Some(SessionFault::StorageWrite(error)),
                Ok(()) if !closing => Some(SessionFault::TaskExited {
                    role,
                    detail: "快照任务提前结束".into(),
                }),
                Ok(()) => None,
            },
            Ok((_, TaskOutput::Collector(state))) => {
                let fault = state
                    .error()
                    .cloned()
                    .map(SessionFault::StorageWrite)
                    .or_else(|| {
                        (!closing
                            && !matches!(
                                self.errors.borrow().as_ref(),
                                Some(SessionFault::StorageWrite(_))
                            ))
                        .then_some(SessionFault::TaskExited {
                            role,
                            detail: "采集通道提前关闭".into(),
                        })
                    });
                self.nodes[role.node()].collection = Some(state);
                fault
            }
            Err(error) => Some(SessionFault::TaskFailed {
                cancelled: error.is_cancelled(),
                role,
                detail: error.to_string(),
            }),
        };
        if let Some(fault) = fault {
            self.faults.push(fault.to_string());
            self.report.publish(fault);
        }
    }

    /// 按依赖顺序停止生产、回收结果、保存快照，最后关闭数据库；外层施加共同期限。
    async fn shutdown_inner(&mut self) -> Result<(), Vec<String>> {
        self.shutdown_stage = "回收采集协调器".into();
        self.stop_snapshots.cancel();
        let store = self.collection_store.clone();
        self.stop_fetch.cancel();
        // 协调器需要仍然存活的 dispatcher 来关闭发现入口和取消查询。
        while self
            .roles
            .values()
            .any(|role| *role == TaskRole::FetchCoordinator)
        {
            if let Some(result) = self.tasks.join_next_with_id().await {
                self.accept_task(result, TaskPhase::ShuttingDown);
            }
        }
        // 先向所有节点发出停产请求，再等待任务排空，避免另一地址族一直继续采集。
        self.shutdown_stage = "停止节点采样".into();
        let faults = &self.faults;
        futures_util::future::join_all(self.nodes.iter().enumerate().map(
            |(index, node)| async move {
                if let Err(error) = node.handle.stop_sampling().await {
                    if matches!(
                        error,
                        crate::dht::dispatcher::SamplerError::DispatcherClosed
                    ) {
                        return;
                    }
                    faults.push(format!("节点 {index} 停止采样失败：{error}"));
                }
            },
        ))
        .await;
        self.shutdown_stage = "关闭节点".into();
        futures_util::future::join_all(self.nodes.iter().enumerate().map(
            |(index, node)| async move {
                if let Err(error) = node.handle.shutdown().await {
                    use crate::dht::dispatcher::QueryError;
                    if matches!(
                        error,
                        QueryError::DispatcherClosed | QueryError::ShuttingDown
                    ) {
                        return;
                    }
                    faults.push(format!("节点 {index} 关闭失败：{error}"));
                }
            },
        ))
        .await;
        self.shutdown_stage = "回收节点任务".into();
        while let Some(result) = self.tasks.join_next_with_id().await {
            self.accept_task(result, TaskPhase::ShuttingDown);
        }
        for (index, node) in self.nodes.iter_mut().enumerate() {
            self.shutdown_stage = format!("保存节点 {index} 状态");
            if let Some(exit) = node.exit.take() {
                if let Err(error) = exit.network_result {
                    self.faults.push(error.to_string());
                }
                if let Err(error) = exit.storage_flush_result {
                    self.faults.push(error.to_string());
                }
                match exit.routing_snapshot {
                    Ok(contacts) => {
                        if let Err(error) =
                            self.dht_store.save_contacts(node.identity, &contacts).await
                        {
                            self.faults.push(error.to_string());
                        }
                    }
                    Err(error) => self.faults.push(error.to_string()),
                }
            }
            if let Some(mut state) = node.collection.take() {
                // 关闭时仅重试尚未确认的分段；UPSERT 允许安全重复保存。
                if let Err(error) = state.run(&store).await {
                    self.faults.push(error.to_string());
                }
            }
        }
        if let Some(error) = self.errors.borrow().as_ref() {
            self.faults.push(error.to_string());
        }
        self.shutdown_stage = "关闭数据库（读取最终统计）".into();
        match store.fetch_stats().await {
            Ok(stats) => stats.log(true),
            Err(error) => self.faults.push(error.to_string()),
        }
        self.budget.log();
        self.shutdown_stage = "关闭数据库".into();
        if let Some(storage) = self.storage.take()
            && let Err(error) = storage.shutdown().await
        {
            self.faults.push(error.to_string());
        }
        let errors = self.faults.take();
        tracing::info!(
            event = "session_shutdown",
            schema_version = 1u64,
            success = errors.is_empty(),
            error_count = errors.len(),
            "会话关闭完成"
        );
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests;
