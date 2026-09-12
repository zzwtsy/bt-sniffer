# 可读性正反例

按当前任务阅读对应章节，不把所有例子变成每个项目必须实现的功能。这里的参数、目录和业务名称用于说明选择方式，不构成对任何现有仓库的迁移指令。

除日志章节给出了配套的模块与入口代码外，其余 Rust 片段均为局部示意，省略了所在项目的类型、导入或实现，不能直接当作完整程序编译。本文件的代码没有在交付环境中编译；采用时应核对目标项目版本与 feature 并实际验证。

<a id="logging"></a>

## 1. 日志：直接建立两个固定输出端

### 当前需求

一个应用需要终端和文件日志；没有运行时调级、JSON 消费者或多套部署策略。选择文本、固定级别、固定目录。长期运行时选定一种现成轮转策略，不同时提供全部选择。

不建议从这个需求推导出 `LoggerConfig`、环境变量优先级、输出端注册表、格式枚举和热更新。下面这种接口在调用者都用默认值时，通常应重新检查必要性：

```rust
// 反例：调用者只需要一个初始化动作，却必须理解多组可选策略。
logger::init("app", None, None, None);
```

应先检查参数和环境变量是否有现实使用者。共享模块中两个程序确实需要不同前缀时，可以只保留 `filename_prefix` 一个参数；不能仅凭调用者传 `None` 就认定部署没有使用环境变量。

### 直接实现

下面针对一个固定应用给出 `src/logging.rs` 的示例。依赖为 `tracing 0.1`、`tracing-subscriber 0.3`（默认 features）和提供相关 Builder API 的 `tracing-appender 0.2`。采用目标项目兼容的具体版本，不为了复制示例升级依赖。

```rust
//! 初始化终端和文件日志，不读取环境变量或配置文件。
//!
//! 日志固定为 INFO 级文本，文件按天轮转。
//! main 持有两个后台写入守卫，直到业务收尾和最后一条日志输出完成。

use std::{
    error::Error,
    io::{self, IsTerminal},
};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    filter::LevelFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

// logs 是本应用专用目录；轮转会清理匹配前缀和后缀的旧日志。
const LOG_DIRECTORY: &str = "logs";
const MAX_LOG_FILES: usize = 7;

/// 启用终端和文件输出，整个进程只调用一次。
///
/// 文件名类似 app.2026-09-13.log。保留上限按文件数量设置，
/// 不表示严格保留七天，也不是磁盘字节数上限。
///
/// 返回的守卫分别管理文件和终端写入。调用者必须持有它们至应用结束。
/// 文件创建或全局 subscriber 注册失败时返回错误，不悄悄退回单端输出。
pub(crate) fn init() -> Result<[WorkerGuard; 2], Box<dyn Error + Send + Sync>> {
    let file_appender = RollingFileAppender::builder()
        .filename_prefix("app")
        .filename_suffix("log")
        .rotation(Rotation::DAILY)
        .max_log_files(MAX_LOG_FILES)
        .build(LOG_DIRECTORY)?;

    // 两个输出端都可能变慢。使用库提供的后台写入，
    // 避免把终端或磁盘写入直接放进网络事件循环。
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let (terminal_writer, terminal_guard) = tracing_appender::non_blocking(io::stderr());

    // 文件不使用颜色，避免把 ANSI 转义字符写进日志。
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false);

    // 日志使用 stderr，为命令行的正常业务输出保留 stdout。
    // 重定向时关闭颜色；这里不增加 NO_COLOR 的解析逻辑。
    let terminal_layer = tracing_subscriber::fmt::layer()
        .with_writer(terminal_writer)
        .with_ansi(io::stderr().is_terminal());

    tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(file_layer)
        .with(terminal_layer)
        .try_init()?;

    Ok([file_guard, terminal_guard])
}
```

配套的最小 `src/main.rs`：

```rust
mod logging;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 必须是具名绑定。`let _ = logging::init()?` 会立即丢弃返回值。
    let _log_guards = logging::init()?;

    tracing::info!("应用启动");
    // 实际业务在此执行，并在作用域结束之前完成任务收尾。
    tracing::info!("应用结束");
    Ok(())
}
```

该结构没有日志管理器和泛型构造函数。两个输出层允许少量重复，因为它们放在一起便于对照。示例用标准错误 trait object 传播不同初始化错误，不要求项目换掉既有错误体系。

这个例子仍有明确边界：`non_blocking` 的默认有界队列满载时可能丢日志，格式化仍有开销；guard 在正常作用域结束时参与收尾，但不是进程被强杀、断电或底层 I/O 故障下的持久化保证。`max_log_files` 也不限制单个文件大小，轮转可能删除匹配的旧日志。需要审计级不丢失或严格磁盘预算时，应另外设计并验证，不给普通日志虚构保证。

对低频同步 CLI，不一定需要两个后台写入线程；对已有异步网络应用，不要为了少一行代码把同步慢输出重新带回事件循环。示例不要求所有项目都使用同一组常量。

如果确有第三方日志噪声，可以在模块内直接使用固定的 `Targets` 过滤策略；如果终端和文件确需不同级别，再使用每层过滤。都不自动意味着需要环境变量或新的配置接口。

API 依据：[后台写入与守卫](https://docs.rs/tracing-appender/latest/tracing_appender/non_blocking/index.html)、[轮转 Builder](https://docs.rs/tracing-appender/latest/tracing_appender/rolling/struct.Builder.html)、[try_init](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/util/trait.SubscriberInitExt.html)、[Targets](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/targets/struct.Targets.html)。

<a id="modules"></a>

## 2. 目录：按职责组织，不按模板摆造型

下面的三个名称容易让职责重叠：

```text
app/             应用启动
persistence/     数据恢复、节点创建、任务监督、退出收尾
storage/         SQLite 和数据库线程
```

问题不是存在三个目录，而是 `persistence` 的名称没有说明它还拥有整个运行会话。可以考虑把会话职责放到 `app/session/mod.rs`，同时检查依赖，而不是只搬文件。

某个确有采集任务的应用可以采用以下局部结构：

```text
src/
├── main.rs
├── app/
│   ├── mod.rs
│   └── session/
│       ├── mod.rs
│       └── tests.rs
├── collector/
│   ├── mod.rs                  # 调度与整体运行
│   ├── worker.rs               # 单个采集任务
│   ├── peer_lookup.rs          # 查找候选 peer
│   └── connection_limits.rs    # 连接许可，不建立连接
├── storage/
│   ├── mod.rs
│   └── jobs.rs
└── logging.rs
```

这不是通用脚手架。只有导出文件的小工具，可能只需要 `main.rs`、`cli.rs` 和 `export.rs`。不要为了“结构化”预建空模块或把每个函数放进单独目录。

移动前还要检查这样的关系：

```text
session 创建 collector
collector 又依赖 session 的错误处理与任务管理定义
```

把路径改成 `app/session` 不能消除双向依赖。优先让 collector 报告自身结果，由应用解释它对整个会话的影响；确需共享的数据定义采用最小的明确边界，不自动引入通用事件总线。

同一个状态所有者的私有实现有时可以分布在几个文件中，但如果所有文件都操作几十个共享字段，仅文件数量变多并没有降低理解成本。先确认职责和不变量，再决定是否需要内部小结构体。

单文件模块（包括测试辅助模块）使用 `foo.rs`；模块需要两个及以上文件时使用 `foo/mod.rs`，不保留仅含 `mod.rs` 的目录。Cargo 识别的独立入口如 `tests/smoke.rs`、`examples/demo.rs` 不因此机械改名。大型模块测试可以组织为 `tests/mod.rs` 及职责明确的子模块，小型 `#[cfg(test)] mod tests { ... }` 可保留。多文件模块使用 `mod.rs` 是本项目偏好，不是 Rust 唯一合法布局。

语言依据：[Rust 模块文件布局](https://doc.rust-lang.org/book/ch07-05-separating-modules-into-different-files.html)、[Cargo 编译目标布局](https://doc.rust-lang.org/cargo/guide/project-layout.html)。

<a id="flow"></a>

## 3. 函数：提取完整职责，不把长代码变成很多短转发

调度循环应该让读者先辨认“退出、完成结果、新发现、定时维护”等事件，而不是在每个分支里读完数据库事务、下载协议和统计实现。

下面是调度结构示意，不是可直接替换现有事件循环的补丁：

```rust
loop {
    tokio::select! {
        _ = stop.cancelled() => break,
        result = workers.next(), if !workers.is_empty() => {
            self.handle_worker_completion(result).await?;
        }
        event = discoveries.recv() => {
            let Some(event) = event else {
                break;
            };
            self.accept_discovery(event).await?;
        }
        _ = maintenance.tick() => {
            self.run_maintenance(&mut workers).await?;
        }
    }
}
```

这些函数应包含完整的业务步骤，而不是再次无意义转发。维护步骤可继续明确区分容量保护、背压和任务补充，但不要求固定拆成几个函数。

此处 `break`、错误传播以及各个 `.await` 的语义只适用于这个示意。实际重构必须保留原有通道关闭策略、分支优先级、取消安全和故障后的清理。不要机械把原先继续运行的分支改为退出，也不要在提取时增加等待而阻塞其他事件。

SQL 的整理通常先做排版，而不是先造查询构建器：

```rust
// 局部示意：SQL 放在拥有该规则的存储模块中。
const ACTIVE_JOBS_SQL: &str = r#"
    SELECT count(*)
    FROM fetch_jobs
    WHERE state IN ('pending', 'running', 'retry_wait')
"#;
```

纯格式整理不能改变字段、参数编号、条件、排序或事务范围。一次性的短 SQL 可以就地书写，不要求所有字符串都提取成常量。

<a id="names"></a>

## 4. 名称：不要靠注释纠正函数名

```rust
// 反例：connect 暗示已建立连接，实际上只获得了并发许可。
let _connection = network.connect(peer.ip()).await;
```

```rust
// 正例：申请同一 IP 的下载名额；这里尚未建立 TCP 连接。
// 本次尝试结束时释放许可，使下一个等待者可以继续。
let _permit = connection_limits.acquire_for_ip(peer.ip()).await;
let metadata = fetcher.fetch_from_peer(peer).await?;
```

`ConnectionPermit`、`acquire_for_ip` 和 `_permit` 表达同一件事。申请失败是否返回 `Result`、取消等待是否释放名额，应按实际实现说明，不能照搬上面省略的类型。

其他命名判断：把命令放进队列用 `enqueue` 或 `submit`；确认事务已提交才使用表达“已完成”的结果。若函数返回“已接受”与“已应用”的不同结果，用明确类型表达，而不是依赖调用者阅读深层实现。

<a id="contracts"></a>

## 5. 注释：明确“没有报错”不等于“本次写入生效”

以下是返回契约与调用方式的示意，`Job`、`VerifiedMetadata`、`StorageError` 属于所在应用：

```rust
/// 当前领取的结果是否真正写入。
enum UpdateResult {
    /// 结果和任务完成状态已在同一事务中提交。
    Applied,
    /// 领取已失效；本次结果被忽略，没有更新当前任务。
    Stale,
}
```

函数文档应说明真实语义，而不是复制旧签名中的 `Ok(())`：

```rust
/// 保存已校验的数据，并完成当前领取的任务。
///
/// Applied 表示本次结果已提交；Stale 表示领取失效，本次不写入。
/// 数据和任务状态在同一事务内更新，写入失败时一起回滚。
/// 调用者取消等待，不会撤销已经被数据库线程接纳的操作。
// 上述承诺只有在实现及测试确实保证时才可使用。
```

调用者也应据此更新统计：

```rust
match store.complete_job(job, metadata).await? {
    UpdateResult::Applied => stats.saved += 1,
    UpdateResult::Stale => {
        tracing::debug!("领取已失效，忽略本次结果");
    }
}
```

不要写“返回成功”便结束说明；也不要在签名变更后只修改代码、不修复函数注释、新手指南和示例。业务没有上述事务和取消语义时，应先描述真实行为，而不是从规范复制一个更强的承诺。

<a id="ownership"></a>

## 6. 所有权与异步：解释本处原因，不重复讲语法课

不好的注释只是把代码翻译一遍：

```rust
// 克隆配置。
let worker_config = config.clone();
```

只有配置确实较小、任务确实需要独立数据时，下面的解释才成立：

```rust
// worker 需要独立持有启动配置；配置只有少量参数。
// 这里复制一次，避免让任务生命周期依赖调用者的局部借用。
let worker_config = config.clone();
```

当实际对象是 `Arc<Config>` 时，应写“共享配置的所有权”，不能说“复制了一份独立配置”。不要求给每次克隆添加注释，只解释影响理解和正确性的边界。

后台任务返回的句柄要交给明确所有者；任务内部的业务 `Result` 与等待句柄可能产生的 join 错误是两层不同结果。取消 token 只是通知，退出时还需要按业务要求回收任务和资源。不要为了让借用编译通过，随意增加任务、改用全局状态或把数据泄漏成 `'static`。

这一类说明应放在创建任务、持有 guard 或跨线程提交操作的位置。底层原理可集中解释一次，后续只说明本处差异。

机制依据：[Tokio 任务与所有权](https://tokio.rs/tokio/tutorial/spawning)、[优雅关闭](https://tokio.rs/tokio/topics/shutdown)。
