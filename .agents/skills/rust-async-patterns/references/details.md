# 异步任务、取消与资源管理

按入口表格选择下面的部分。代码的运行时行为以当前依赖版本为准；本文件中的示例实现在独立 Cargo 工程中，避免复制多个无法同步验证的片段。

<a id="tasks"></a>
## 任务所有权

选择并发方式时先确定是否需要独立调度、结果顺序和退出语义：

| 需求 | 可选工具 | 必须确认 |
| --- | --- | --- |
| 少量子 future 随父调用完成或取消 | `join!`、直接 `.await` | 普通 `join!` 等待全部；任一分支不结束就不会返回 |
| 有界集合并发，无需独立任务 | `buffer_unordered` 等 stream 工具 | 并发值大于零、结果顺序、逐项错误 |
| 独立调度且由统一所有者管理 | `JoinSet` 或被持有的 `JoinHandle` | 任务上限、回收结果、取消后的 join |

`tokio::spawn` 即使在单线程 runtime 上也要求 future 和输出为 `Send + 'static`。`'static` 不表示任务必须永久存在，而是它不能借用在任务之前失效的栈数据。按需移动拥有的数据或克隆 `Arc`；不要仅为满足该边界泄漏内存。

丢弃 `JoinHandle` 会分离任务，丢弃 `JoinSet` 会请求中止所管理的异步任务；两者都不能代替成功完成确认。`abort_all` 后还需要回收结果。

有界并发须覆盖整个接纳路径：

- 可在 spawn 前申请许可，或根据 `JoinSet::len()` 等限制任务集合。
- 在无限 spawn 的任务内部等待 semaphore，只限制正在工作的数量，仍会积累等待任务。
- 及时消费已完成结果，防止 `JoinSet` 或结果容器无界增长。
- 条目上限不能代替载荷字节预算；队列外持有大消息的等待者也消耗内存。

批处理明确是全部成功、遇错停止，还是允许部分成功。不能仅日志记录失败，再用不带说明的 `Ok(成功结果列表)` 表示完成。

<a id="channels"></a>
## 通道与背压

| 通道 | 合适的语义 | 关闭或落后时的处理 |
| --- | --- | --- |
| `mpsc` | 多生产者、单消费者的工作队列 | 全部强 sender 释放且缓冲排空后 recv 返回 None；也可由 receiver 主动 close 后排空 |
| `oneshot` | 一次请求的结果确认 | 发送/接收失败通常表示另一方退出，按契约处理 |
| `watch` | 多生产者、多消费者共享最新值 | 可合并中间更新；用 `changed()` 配合 `borrow_and_update()`，处理关闭 |
| `broadcast` | 活跃订阅者接收事件 | 处理 `Lagged` 与 `Closed`；它不保证慢消费者收到每条消息 |

[collect_messages 示例](../examples/src/lib.rs)将唯一 sender 移入生产者并明确释放，使接收循环能够终止；生产和消费同时驱动，避免填满有界通道后才启动消费者。

关闭流程要确认谁还持有 sender 克隆，以及是否有未释放的发送许可。满载可等待、拒绝或丢弃，选择必须符合该入口的契约并可观察；持久化结果不能静默丢弃。

<a id="cancellation"></a>
## 取消安全

审查每个 `.await`：在此处丢弃 future，已经发生了什么，剩余状态归谁，重新调用是否重复执行？

- `select!` 选中一个分支后会丢弃其他分支的 future；这不会回滚文件写入、远端请求或已入队的数据库操作。
- `timeout` 同样只结束对内部 future 的等待。非让出执行权的代码可能超过期限仍继续运行，已分离的任务也可能继续。
- `mpsc::Receiver::recv`、`JoinSet::join_next` 可安全地在 `select!` 中取消等待；但必须继续处理已接纳工作的最终责任。
- `read_exact`、`write_all` 等组合 I/O 可能已处理部分字节。取消后复用同一流时必须保留进度或重新设计帧状态；若整个连接会被关闭，也要明确这是放弃该会话。
- 在 `select!` 中取消 `sender.send(value)` 会丢弃它拥有的 `value`。必须保留消息时，将值留在外部，竞争 `reserve()` 与取消信号；取得许可后再同步发送。取消预约会失去排队位置。
- 请求被存储线程接纳后，即使 oneshot 接收端被丢弃，命令仍可能提交。通过幂等键、领取 generation、事务或可查询状态处理未知结果。
- `CancellationToken` 可以被多个任务观察；子 token 可单独取消，子取消不反向取消父 token。通知本身不释放 transaction、许可或业务记录。

稳定的截止时间应在整个操作开始时建立并传递。分阶段和重试各自重新创建完整 timeout，可能让总耗时远超预期。

<a id="shutdown"></a>
## 退出与完成确认

通常按依赖关系完成以下动作；具体顺序由谁继续生产、谁负责消费决定：

1. 停止接纳新工作，终止会重新补充任务的调度源。
2. 发出取消通知或关闭接纳通道，并明确哪些已接纳工作要排空、保存或回到可恢复状态。
3. 在共同截止时间内等待任务结果，同时处理业务错误与 `JoinError`。
4. 关闭仍承担排空/保存责任的存储消费者，确认连接及文件资源已释放。
5. 超时则记录失败；可中止的异步任务发出 abort 后继续 join，不能声称业务清理已完成。

[shutdown 示例](../examples/src/lib.rs)只演示任务集合的取消、共同期限、错误聚合及 abort 后回收；调用前须停止任务接纳，它不包含数据库持久化或进程信号处理。

这个例子的截止时间依赖 Tokio 的协作调度。任务长时间不让出执行权、同步析构阻塞、系统调用无法返回时，异步 timeout 不能提供进程级硬期限。已启动的 `spawn_blocking` 不能被 abort 停止；`shutdown_timeout` 只限制 runtime 等待时间，不代表底层线程操作已结束。

<a id="resources"></a>
## 资源与阻塞边界

- 短暂访问普通数据、且不跨 `.await` 的临界区可使用 `std::sync::Mutex`。把 guard 限制在词法作用域内，避免它意外进入 suspended future。
- 确实需要跨 `.await` 串行访问资源时，可以使用 Tokio `Mutex`；仍然检查锁顺序、递归获取和持锁耗时。异步锁并不消除死锁。
- `RwLock` 需要根据竞争测量选择，不能仅因读取多就认定更快。
- 短期阻塞工作可用 `spawn_blocking` 并限制并发；长期独占的同步连接可用专用线程和有界命令队列。大量 CPU 工作需要额外并发控制，不能依赖阻塞线程池默认上限。
- RAII 负责同步释放拥有的资源，例如 `OwnedSemaphorePermit`。取消工作 future 后，必须等待其销毁才能确认许可已经归还。
- `Drop` 不能 `.await`。不要在里面 spawn 一个借用 `self` 的异步归还任务：除了 `'static` 问题，还可能先释放许可、后归还资源，让下一次申请取得不存在的连接。
- 真正需要异步清理时，设计显式 `close`/`shutdown` 与 owner 的完成确认；`Drop` 只提供有明确限制的兜底。连接池优先使用满足协议与取消要求的成熟实现。

<a id="tools"></a>
## Trait、Pin、日志与测试

- 不为普通异步逻辑默认加入 `async-trait`。Rust 1.75 起原生 async trait 方法可用于静态分发；动态分发时再评估宏库或显式装箱 future，核对 `Send` 和分配成本。
- 某些 stream 不是 `Unpin`。调用要求 `Unpin` 的 `next()` 前，按类型需要使用 `pin!` 或 `Box::pin`；不要假设所有生成式 stream 都能直接 `.next().await`。
- 使用 tracing 的 `.instrument(span)` 跨异步 poll 关联任务，避免把 `span.enter()` guard 持有到 `.await` 之后导致上下文错误。tokio-console 需要相应依赖、subscriber 初始化、features 与配置，单开 `tokio_unstable` 不足以接入。
- 观察任务数、队列/字节占用、失败类别、丢弃量和清理结果；日志不能替代行为确认。
- 使用事件同步确认任务已经进入目标阶段；暂停时钟适合 Tokio 定时，但不能驱动数据库线程或系统 I/O。覆盖取消前后资源和持久化状态，不仅检查 token。

<a id="examples"></a>
## 可运行示例与验证

[最小 Cargo 工程](../examples/Cargo.toml)使用 current_thread 测试 runtime、Tokio 和 tokio-util；没有公网、数据库或 HTTP 依赖。测试覆盖 sender 关闭、排空、取消后的清理与许可释放、超时、共同期限，以及业务失败和 panic 的报告。

从仓库根目录运行；已缓存依赖时可加 `--offline`：

```sh
cargo fmt --manifest-path .agents/skills/rust-async-patterns/examples/Cargo.toml -- --check
CARGO_TARGET_DIR="${TMPDIR:-/tmp}/bt-sniffer-skill-examples-target" cargo test --locked --manifest-path .agents/skills/rust-async-patterns/examples/Cargo.toml
CARGO_TARGET_DIR="${TMPDIR:-/tmp}/bt-sniffer-skill-examples-target" cargo clippy --locked --all-targets --manifest-path .agents/skills/rust-async-patterns/examples/Cargo.toml -- -D warnings
```

修改示例后运行这些检查，保持源码作为示例的唯一版本。本工程的少量有限输入只演示生命周期，`collect_messages` 收集全部输出，不宣称整个流水线的内存有界；无限数据流应逐项消费并单独限制载荷。

官方参考：[Tokio select 取消安全](https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety)、[JoinSet](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html)、[mpsc](https://docs.rs/tokio/latest/tokio/sync/mpsc/index.html)、[Mutex](https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html)、[spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)、[CancellationToken](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)。检查具体 API 时切换到项目锁定版本。
