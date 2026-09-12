//! SQLite 的唯一所有者。阻塞的磁盘操作只在线程中进行，不占用 UDP 事件循环。
//!
//! 应用持有 Storage 并负责 shutdown；其他模块克隆 StorageHandle 提交操作。
//! open → handle 提交命令 → 数据库线程执行 → oneshot 返回结果 → shutdown。
//! 命令数和载荷字节分别限制；关闭命令按队列顺序执行，连接和目录锁释放后才确认完成。
//!
//! Storage 持有线程生命周期，StorageHandle 只提交有界命令；等待者取消不会提前释放命令的载荷预算。
mod cooldown;
pub(crate) mod jobs;
mod records;
mod schema;
#[cfg(test)]
mod tests;

pub(crate) use cooldown::{CooldownLease, RestoredCooldown};
pub(crate) use records::SavedContact;
use rusqlite::Connection;
use std::{
    fmt,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StorageError {
    /// 数据库线程已经退出，或者关闭命令已经处理。
    Closed,
    /// 另一个实例正在使用这个状态目录。
    Locked,
    /// 配置或磁盘记录不满足约束，不能自行猜测并修复。
    Invalid(&'static str),
    /// 文件、目录或操作系统层面的错误。
    Io(String),
    /// SQLite 返回的错误，例如磁盘满、只读或事务冲突。
    Database(String),
    /// 同一 hash 对应不同字节，或同一实例被重复创建。
    Conflict,
    /// 目标节点或 IP 仍然处于冷却中。
    Cooldown,
    /// 队列字节预算或记录数已经达到上限。
    Capacity,
    /// 操作系统没有提供可靠随机数，不能生成身份或预约标识。
    Entropy,
}
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "持久化错误：{self:?}")
    }
}
impl std::error::Error for StorageError {}
impl From<rusqlite::Error> for StorageError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Database(e.to_string())
    }
}
impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StorageConfig {
    /// 调用者指定的本地状态目录；不在模块中硬编码生产路径。
    pub(crate) directory: PathBuf,
    /// 等待数据库线程处理的命令数量，不等于记录总数。
    pub(crate) command_capacity: usize,
    /// 尚未处理完成的变长载荷总预算，默认 32 MiB。
    pub(crate) byte_capacity: usize,
}
impl StorageConfig {
    pub(crate) fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            command_capacity: 128,
            byte_capacity: 32 * 1024 * 1024,
        }
    }
}
// FnOnce 允许操作消费捕获的数据；Send 允许把操作交给数据库线程。
// dyn 统一不同闭包的类型，Box 使它们能够存入同一个队列。
type DbOperation = Box<dyn FnOnce(&mut Connection) + Send>;

enum StorageCommand {
    Execute(DbOperation),
    /// 排在之前已接纳操作之后的关闭屏障。
    Shutdown,
}

/// 可克隆的数据库命令入口，不拥有关闭完成确认；克隆不会创建新连接或新线程。
#[derive(Clone)]
pub(crate) struct StorageHandle {
    /// 有界队列限制排队命令数；bytes 单独限制载荷大小。
    sender: mpsc::Sender<StorageCommand>,
    bytes: Arc<Semaphore>,
    byte_capacity: usize,
    /// 进程内采集接纳上限，0 表示尚未启用；不是数据库中当前任务数量。
    fetch_limit: Arc<std::sync::atomic::AtomicUsize>,
    /// 已观察采样 hash 数，可能重复；不代表新增任务或成功下载数。
    sample_observations: Arc<std::sync::atomic::AtomicU64>,
}
impl fmt::Debug for StorageHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageHandle").finish_non_exhaustive()
    }
}
/// 应用持有所有者以正常关闭；各模块只拿可克隆的 handle。
pub(crate) struct Storage {
    pub(crate) handle: StorageHandle,
    /// 数据库连接关闭并释放目录锁后发送结果；由唯一 Storage 所有者消费。
    finished: oneshot::Receiver<Result<(), StorageError>>,
}
impl Storage {
    /// 校验容量并启动独占 SQLite 的线程；返回前等待连接、锁和迁移准备完成。
    /// 初始化失败返回具体错误；取消等待会丢弃接收端，线程发现已无控制者后退出。
    pub(crate) async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        if config.command_capacity == 0
            || config.command_capacity > Semaphore::MAX_PERMITS
            || config.byte_capacity == 0
            || config.byte_capacity > u32::MAX as usize
            || config.byte_capacity > Semaphore::MAX_PERMITS
        {
            return Err(StorageError::Invalid("存储队列容量无效"));
        }
        let (sender, mut receiver) = mpsc::channel::<StorageCommand>(config.command_capacity);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (done_tx, finished) = oneshot::channel();
        std::thread::Builder::new()
            .name("sqlite-storage".into())
            .spawn(move || {
                let opened = open_database(&config.directory);
                let (mut connection, lock) = match opened {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                // open future 被取消时，不留下没有控制者的数据库线程。
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        StorageCommand::Execute(operation) => operation(&mut connection),
                        StorageCommand::Shutdown => break,
                    }
                }
                // 关闭连接与锁之后才确认完成。排在关闭命令后的调用会收到 Closed。
                drop(receiver);
                let result = connection.close().map_err(|(_, e)| StorageError::from(e));
                drop(lock);
                let _ = done_tx.send(result);
            })?;
        ready_rx.await.map_err(|_| StorageError::Closed)??;
        Ok(Self {
            handle: StorageHandle {
                sender,
                bytes: Arc::new(Semaphore::new(config.byte_capacity)),
                byte_capacity: config.byte_capacity,
                fetch_limit: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                sample_observations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            },
            finished,
        })
    }
    /// 排入关闭屏障并等待连接关闭、目录锁释放；成功不等于所有 handle 已被销毁。
    /// 屏障前已接纳操作先执行，屏障后的操作收到 Closed；调用方应先停止生产者。
    /// 取消本次等待不撤回已入队屏障，也不提供关闭完成证明。
    pub(crate) async fn shutdown(self) -> Result<(), StorageError> {
        self.handle
            .sender
            .send(StorageCommand::Shutdown)
            .await
            .map_err(|_| StorageError::Closed)?;
        self.finished.await.map_err(|_| StorageError::Closed)?
    }
}
fn open_database(directory: &Path) -> Result<(Connection, File), StorageError> {
    std::fs::create_dir_all(directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("instance.lock"))?;
    lock.try_lock().map_err(|_| StorageError::Locked)?;
    if rusqlite::version_number() < 3_051_003 {
        return Err(StorageError::Invalid("SQLite 必须包含 WAL-reset 修复"));
    }
    let mut connection = Connection::open(directory.join("state.sqlite3"))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version > 2 {
        return Err(StorageError::Invalid("数据库由更新版本程序创建"));
    }
    connection.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
    )?;
    schema::migrate(&mut connection)?;
    Ok((connection, lock))
}
impl StorageHandle {
    /// 数据库线程退出会关闭接收端；不消费 shutdown 所需的最终结果。
    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }
    /// 在复制大载荷之前申请预算；预算直到线程处理完该命令才释放。
    async fn budget(&self, bytes: usize) -> Result<OwnedSemaphorePermit, StorageError> {
        if bytes > self.byte_capacity {
            return Err(StorageError::Capacity);
        }
        self.bytes
            .clone()
            .acquire_many_owned(bytes as u32)
            .await
            .map_err(|_| StorageError::Closed)
    }
    // 'static 要求操作不借用可能提前失效的栈数据，不表示操作会永久运行。
    // oneshot 返回一次执行结果；接收者取消等待后，已入队的操作仍会执行。
    async fn submit<T: Send + 'static>(
        &self,
        permit: OwnedSemaphorePermit,
        operation: impl FnOnce(&mut Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(StorageCommand::Execute(Box::new(move |connection| {
                // 预算随命令移交，不能随调用者取消而提前归还。
                let _permit = permit;
                let _ = reply.send(operation(connection));
            })))
            .await
            .map_err(|_| StorageError::Closed)?;
        result.await.map_err(|_| StorageError::Closed)?
    }
    /// 小型操作使用零字节预算，捕获的输入须有独立大小约束；仍占用有界命令队列。
    /// 携带较大复制数据的入口须先 budget 再 submit，不能经本入口绕过载荷限制。
    /// Closed 可能发生在入队前，也可能只是结果通道关闭，不能据此断定事务未提交。
    pub(super) async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        self.submit(self.budget(0).await?, operation).await
    }
    /// 备份目标必须不存在，防止误覆盖已有备份或数据库。
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "保留在线备份能力，尚无运维子命令")
    )]
    pub(crate) async fn backup(&self, destination: PathBuf) -> Result<(), StorageError> {
        self.call(move |connection| {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&destination)?;
            drop(file);
            connection.backup("main", &destination, None)?;
            Ok(())
        })
        .await
    }
}

/// UTC 毫秒只用于磁盘；进程内的 deadline 仍用 Instant。
pub(crate) fn unix_millis(time: std::time::SystemTime) -> Result<i64, StorageError> {
    let millis = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StorageError::Invalid("时间早于 Unix epoch"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| StorageError::Invalid("时间溢出"))
}

/// 一次运行内以单调时钟推进 UTC，避免系统校时改变已经安排好的协议期限。
#[derive(Debug, Clone)]
pub(crate) struct Clock {
    monotonic: std::time::Instant,
    wall: std::time::SystemTime,
}
impl Default for Clock {
    fn default() -> Self {
        Self::new(
            tokio::time::Instant::now().into_std(),
            std::time::SystemTime::now(),
        )
    }
}
impl Clock {
    pub(crate) fn new(monotonic: std::time::Instant, wall: std::time::SystemTime) -> Self {
        Self { monotonic, wall }
    }
    pub(crate) fn wall_at(
        &self,
        now: std::time::Instant,
    ) -> Result<std::time::SystemTime, StorageError> {
        let elapsed = now
            .checked_duration_since(self.monotonic)
            .ok_or(StorageError::Invalid("单调时间回退"))?;
        self.wall
            .checked_add(elapsed)
            .ok_or(StorageError::Invalid("UTC 时间溢出"))
    }
    pub(crate) fn millis_at(&self, now: std::time::Instant) -> Result<i64, StorageError> {
        // 冷却时间向上取整，不能因毫秒精度损失而比远端允许的时间早发包。
        let wall = self
            .wall_at(now)?
            .checked_add(Duration::from_nanos(999_999))
            .ok_or(StorageError::Invalid("UTC 时间溢出"))?;
        unix_millis(wall)
    }
}
