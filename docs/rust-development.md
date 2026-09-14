# Rust 开发约束

本文件记录 `bt-sniffer` 的项目实现约束。[AGENTS.md](../AGENTS.md) 与 [rust-readable-apps](../.agents/skills/rust-readable-apps/SKILL.md) 是主工作流；[rust-best-practices](../.agents/skills/rust-best-practices/SKILL.md) 作为通用参考，不默认重复加载，[rust-async-patterns](../.agents/skills/rust-async-patterns/SKILL.md) 按异步问题查阅相关章节。运行参数、数值预算和验收命令以 [README](../README.md) 与[采集说明](metadata-collection.md)为准；变更架构时同步修订相关约束。规范统一不授权删除或改变现有协议、数据库、安全、故障恢复及运行行为。

## 可读性与所有权

- 优先使用具体类型、普通函数和有业务含义的 `struct` / `enum`。确有复用或替换需求时使用 trait；不为消除局部动态分发把泛型扩散到整个应用。
- 用 `Result`、`Option`、`?`、`match`、`let … else` 表达失败和正常缺失。多步骤、带副作用或提前退出的流程优先写清楚的循环；简单转换可以使用简短迭代器链。
- 命名说明用途和单位，例如 `observed_at_ms`、`delay_ms`、`state_bytes`。SQL 分行展示字段、条件、排序和限制；嵌套查询保持可辨认的层次。宏内也避免把多条状态更新挤在一行。
- 只读时借用，需要保留或跨线程传递时明确转移所有权。移动 `Vec` 通常不复制缓冲区；`Arc::clone` 与 `Vec::clone` 的成本不同，不能统一禁止 clone。
- 提取函数的依据是完整职责或需要集中维护的规则，不设机械行数门槛。采样的候选筛选、轮次重置、请求登记，以及 metadata 的握手、分片窗口和最终校验，是本项目的具体示例。保留能防止无效输入流入后续流程的领域类型，例如 `VerifiedMetadata`。

## 中文注释

- `//!` 说明模块职责、调用顺序、数据和任务由谁持有；`///` 说明类型或接口的用途、参数单位、返回值与失败语义；`//` 说明不明显的原因、事务边界、资源释放及性能选择。
- 跨模块接口与关键私有边界应有契约说明。简单 getter 无需重复翻译代码，不按注释数量或覆盖百分比验收。
- 面向新手，首次出现 `FnOnce`、`Send + 'static`、oneshot、许可释放等机制时，可以解释它们在此处的作用；常见知识集中解释一次，后续引用。
- 特别说明“未发生写入但返回成功”“取消等待但命令仍会执行”等容易误判的行为。错误、取消和重试规则随代码一起更新。
- 可运行示例引用真实测试，独立片段明确标注上下文。当前是 binary，生成 rustdoc 不代表内部示例已被 doctest 执行。
- 新手从[上手指南](getting-started.md)进入；参数和运行边界仍引用 README 与采集说明，不在多处复制维护。

- 内部结果优先用字段说明含义，例如 `HandshakeInfo`、`LookupResult` 和预约结果；正常的能力标志可以保留 bool，有多种业务用途的参数采用枚举。不要把所有 Option 都改成自定义状态机。
- 全链路审查时逐项核对入口、输入可信度、状态所有者、成功条件、错误与取消路径；已有清晰说明无需重写，不以注释比例作为质量标准。

## 性能取舍

正确性、资源上限和明确的性能目标是底线；在达标实现中优先选择容易维护的方案。

- 基础实现保留有界并发、分页、合理查询和事务边界，避免无界积压与重复工作。
- 会增加理解成本的优化必须记录：代表性工作负载、相同 Release 配置下的基线、优化后的指标、测量波动，以及行为一致性的证据。根据问题选择吞吐、延迟、CPU 或内存指标。
- 先定位热点，再考虑算法、数据结构及减少重复 I/O。缓存、复杂借用、手写内存操作和内联调整需要具体收益支持，不凭语法判断性能。
- 微基准改善不能直接等同于整个服务改善；格式、编译和行为测试通过也不证明性能改善。可读性改造若未进行性能测量，不声明性能提升。

参考：[Rust Book 的循环与迭代器](https://doc.rust-lang.org/book/ch13-04-performance.html)、[rustdoc 写作指南](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html)、[Rust Performance Book 的基准测试](https://nnethercote.github.io/perf-book/benchmarking.html)。

## 运行时与模块

- 单文件模块使用 `模块名.rs`，包括叶子模块和独立测试辅助模块，不创建仅含 `mod.rs` 的目录。模块需要两个及以上文件（包括入口和独立测试文件，可位于子目录）时，使用 `模块名/mod.rs` 作为目录入口；小型内联测试可以保留。`main.rs`、`lib.rs`、`build.rs`、独立集成测试和示例等编译目标入口保留 Cargo 所需布局。文件布局不改变逻辑模块路径与可见性；现有模块按批准范围渐进迁移，不因小修复全仓搬迁。

- 这是一个可执行程序；[入口](../src/main.rs)明确内部模块不对其他 crate 提供 API。测试可放在模块内部访问 `pub(crate)` 能力，无需仅为测试拆出公共 library。
- [Cargo.toml](../Cargo.toml)使用 edition 2024，未单独声明 `rust-version`。先核对现有工具链及依赖要求，不从 skill 的历史版本描述推定 MSRV。
- 入口使用 Tokio `current_thread` runtime。网络 future 必须及时让出执行权；不要为套用示例添加 `full` features、更换 runtime 或引入无关数据库/HTTP 库。
- Bencode、帧编解码及取消通知使用现有库；DHT transaction、路由、BEP 9/10 协商、原始 metadata 校验和恢复语义由项目维护。
- KRPC 的未排序字典兼容仅位于 `dht/udp/ordering.rs`：bendy 校验原子编码，适配层有界组织并排序容器，拒绝所有层级的重复键，再由 bendy 解码为 KRPC 类型。容器深度最多 64；原子字节不改写。不能把该适配层用于 peer-wire 或原始 info 校验，也不能因引导服务器改端口回包而放宽 transaction 来源匹配。 扩展握手版本 2 使用独立的 `collection/peer/wire/extension_ordering.rs`：只在严格 InvalidDictionary 后尝试，最多 4096 字节／64 层，拒绝所有层级重复键，规范化后再次严格解析。metadata 消息头和原始 info 绝不使用规范化；诊断 inspection 不能充当接纳依据。

## 取消、背压与存储

- [Storage](../src/storage/mod.rs)由专用线程独占 SQLite。阻塞的连接操作和文件 I/O 不进入 UDP 事件循环；异步调用通过有界命令通道及载荷预算提交。
- 命令被接纳、调用者收到响应、数据库事务提交是不同事件。调用方 future 被取消，不意味着已经提交给数据库线程的命令被撤销；重试前考虑幂等性和结果是否已生效。
- [持久化任务](../src/collection/jobs/mod.rs)通过领取 generation 拒绝旧任务结果。维护任务状态时同时检查 metadata 保存和状态变更的事务边界，不能让取消或重试绕过退避及领取规则。
- [采集器](../src/collection/mod.rs)使用 task ID 关联领取记录，统一处理正常退出、worker panic 和意外取消，完成在途任务及结果收尾；[Dispatcher](../src/dht/dispatcher/mod.rs)负责 DHT 查询资源。取消测试应观察 transaction、连接和许可最终释放，以及未完成任务可恢复。
- 满载行为按入口契约区分：协议通知允许按既有规则丢弃并计数；数据库结果不能靠丢弃换取吞吐。DHT 成功 ACK 不表示采集任务已持久化。
- [会话退出](../src/app/session/mod.rs)停止采集、关闭节点、保存状态、等待任务并关闭数据库；沿用共同清理期限，聚合失败。已报告错误保留在会话共享记录中，不能只放在可能被超时取消的局部 future 内；超时报告当前清理阶段。超时不等于成功，runtime 的 DNS 等待期限也不保证系统 DNS 已停止。

- 采集器的领取、退避和完成时间使用注入的 `clock::Clock`：单次运行内从 UTC 锚点按单调时间推进，重启继续使用磁盘上的 UTC 期限。采样记录区分接收观察时间与任务处理时间，不用处理时间覆盖首次发现。
- DHT 控制错误、worker 异常与配置错误属于致命故障，不能转成可降级的存储错误。正常取消不增加远端失败次数；同 IP TCP 等待由释放通知唤醒，取消等待不能遗留占用。

## 验证选择

- 从受影响模块的行为检查开始，再运行 README 指定的必要检查。文档和 skill 示例修改不要求重跑公共 DHT 验收。
- 默认测试使用 loopback、临时数据库与注入的 DNS 结果；异步定时用受控时钟或事件同步。数据库线程的完成依靠响应确认，推进 Tokio 时钟不能代替等待磁盘工作。
- 测试名称表达场景和预期，中文说明解释所保护的规则；复杂测试按准备、触发、断言和收尾组织。大型 fixture 用本模块具名结构体，模拟地址族和 peer 行为用明确类型，保留真实协议位的布尔表达。
- 修改 SQL 排版时保持字段、参数、条件和事务顺序，审查应区分空白变化与语义变化；重命名测试必须同步更新精确选择命令和子进程入口。
- 长时间、本机真实时间与公网测试是显式验收项目。分别报告编译、单元测试、真实网络观察的证据，不用一种结果代替另一种。
- 沙箱无法创建 socket、访问缓存或运行工具时，记录具体阻塞；不能据此修改协议行为或把检查记为通过。

## Skill 维护

主工作流、通用参考和异步专项保持上述职责，入口按需指向参考章节。[skills-lock.json](../skills-lock.json)保留安装来源记录；本地修订由 skill 元数据标记，不手工伪造安装 hash。升级上游时逐项核对本地纠错、项目边界和示例验证。

结构参考 [OpenAI 官方 Skills 文档](https://developers.openai.com/codex/skills/)：名称和描述用于发现，选中后加载入口，详细参考内容按任务读取。

## DHT 共享预算与采集调度

- governor 管理速率语义，Tokio 单调时钟适配支持暂停时间测试。会话持有唯一预算，双栈和全部主动来源共用；组合配额先探测、全部满足才提交，不能因受限 IP 耗尽其他 IP 的可用额度。
- dispatcher 拥有待发意图和在途 transaction，合计有界且保留用户名额。限流通过事件循环期限推进，不在 UDP 分支睡眠，不为每个等待创建任务；在实际发送前登记 transaction，取消时同步回收业务上下文。
- 原始 UDP 数据报先限流再 Bencode 解码。响应预留只豁免普通解码入口，不能豁免 transaction/来源/身份校验，也不能让 Query 绕过普通配额。回复不积压。
- 未发送的本地错误不得调用远端失败路径；采样只有确认未发出时才按租约撤销，发送不确定与异常退出保守恢复。存储确认仍由事件循环的持有 future 推进。
- collector 的 TCP IP 表覆盖持有者和等待者，由公平 semaphore 与 RAII 管理；取消后要验证最后一个条目释放。总期限包含本地等待，本地延期与远端失败分别记录。
- 领取偏好只影响到期任务，提示首试/近期首试/历史首试/全部重复任务的 3∶2∶1∶2 八步轮转保存在内存，SQL 事务是接纳容量和 generation 的事实来源。任务容量暂停仍接纳已存在 hash 的 hint 刷新；磁盘/存储暂停停止全部发现接纳。
- 指标仅保留固定聚合，不按 hash/IP 增加长期标签。提交计数必须以事务 Applied 为依据；Stale 不能计成功。固定输入 Release 调度模拟与真实网络性能证据明确区分。
- 30 分钟本机与两小时公网验收独立手动运行，不作为默认回归或实施收尾步骤。报告记录源码清单 SHA-256、工具链、配置、时长、统计、实际验证范围及状态；提前停止不能标为通过。

## 流式采集与积压控制

- worker 拥有并交替驱动 lookup 与单个 peer future；每 hash 最多一个连接，不创建脱离 worker 的查找。流式通道容量与双栈去重结果均为 32；慢消费者不能阻塞 dispatcher。成功后取消须观察 transaction、socket 与许可实际释放。
- 并行阶段分别跟踪 DHT 发送进度和 peer 状态，不能用后写的单一阶段掩盖本地等待。真实连接、握手、传输和校验位置生成阶段报告；普通取消记 Cancelled/None，外层任务超时截断记 Timeout/Task。
- 反向验证的每 IP 60 秒冷却复用共享有界表；双栈共享待发许可由 RAII 回收，重复事件不延长冷却。验证接纳拒绝不得走远端失败处理，也不得阻止合法回复。
- freshness 每 5 秒在有界补建与领取后读取 Q：最近 30 分钟首次发现、generation=0、pending/retry_wait 且无有效合法提示（含未到期首试）；地址策略与领取共用，不能在存储硬编码 PublicOnly。领取立即释放 Q，运行和重试仍占 M。高水位 min(M,4C)、低水位 floor(B/4) 持续 30 秒恢复，与容量、存储暂停做 OR。仅 sample/fetch 同时启用时生效；capacity 仅按总容量接纳。所有采样 hash 先落库，新任务统一游标补建；近期/历史各最多 128 行/秒，历史只使用 M−min(B,M−1)。提示失效可使已有 Q 超限，不驱逐。保持 schema v2、事务/generation、休眠再激活与收尾边界。
- 直方图保留固定桶，领取等待与网络耗时使用不同上界；溢出用显式分位数结果表达。DHT 等待原因按意图去重，不能把事件循环探测次数算请求次数。
- 固定输入比较把同等任务的真实 loopback 时序与减少主动接纳的模拟分开报告；模拟不代表公网吞吐。短比较必须用明确测试名运行，ignored 长测不自动启动。

## 日志输出边界

日志固定输出 stderr 文本和工作目录 logs/ 下的 JSONL；默认本程序 INFO、第三方 WARN。
仅通过 RUST_LOG 使用标准 EnvFilter：未设置或空值为 warn,bt_sniffer=info，非空指令完整替换默认规则，非法/非 Unicode 值在创建日志文件前失败。
不加载 .env，不提供格式 CLI、热更新或多进程共写。按 UTC 自然日轮转，最多保留 7 个匹配 .jsonl 文件，旧 .log 不自动清理；磁盘字节预算由部署层负责。

初始化集中于 app::logging::init()，在 CLI 解析后、runtime 创建前；help/version 不创建日志资源。
main 持有两个 writer guard 至 runtime 收尾、最终业务错误和队列观察事件之后。
两端各有 4,096 条有界 lossy 队列，慢 I/O 在独立线程执行；格式化仍有调用线程开销，满载丢弃，不反压网络循环。
队列诊断复用分钟摘要并在退出输出最终观察；计数不覆盖全部 I/O 故障、报告自身和最终刷新阶段，也可能被过滤或丢弃。
guard 超时不是排空或持久化证明。字段遵守 [日志契约版本 3](logging.md#日志契约版本-3)，保留类型、单位、区间和累计含义，不输出原始 metadata/报文。
日志专项工作采用项目 bt-sniffer-logging skill；测试解析字段，进程级输出和环境变量用隔离子进程验证。

## 功能切片与重构约束

`dht` 持有协议状态及 `DhtStore`；`collection` 持有 `CollectionStore`、接纳配置、调度、peer 会话及诊断。两个具体 Store 共用 `StorageHandle` 的命令通道和载荷预算，不另开数据库连接。DHT 不引用 collection；采样和宣布通过有界通道交付。共享 `clock`、`histogram` 和 `info_hash` 不依赖业务模块。

采样分段由 `collection/ingest` 的 `SampleIngest` 消费，未确认分段、偏移和队列封装在该对象中，应用接回整体状态并通过同一 `run` 入口重试保存；sample-only 同样运行该路径。`scheduler.rs` 在协调器任务内驱动维护与领取，worker 唯一编排候选，`PeerClient::fetch_one` 只管理一条 peer 会话。总并发属于调度器，同 IP 许可覆盖整个 peer 尝试。正常和退出保存共用 `commit_metadata`，只有 Applied 更新实际提交视图；正常 CompletionTotals 不包含关闭保存。

领取耗时按完整维度只存一份累计与区间，摘要在输出时合并桶后计算分位数。单个 `PeerObservation` 推进唯一物理阶段，并独立记录完整握手起点。物理阶段与期限范围分别表达，Task 截断统一记 Timeout/Task；普通取消不增加远端失败历史。日志 target 使用真实模块路径。

显式 ping/find_node、夹具 metadata 保存和读取只编入测试；引导和维护共用的 RPC 校验、上下文和收尾继续用于生产。生产构建不提供在线备份接口，也不启用 rusqlite backup feature，不以预留接口和 dead_code 豁免替代实际需求。

采集接口按实际控制者配置：`MetadataConfig` 仅含 peer 协议与期限限制，并发在采集配置校验，候选上限读取 worker 的唯一常量。应用一次确定 peer 配置和实际背压模式；启动日志直接输出实际值和单位，不为日志保存影子执行参数。`PeerInitError` 仅用于构造，`PeerFetchError` 仅用于单次调用，取消明确映射为本地延期。

任务 SQL 按 `jobs/claim`、`transitions`、`hints`、`queries` 与 `admission` 归属组织；退避函数接收事务内读取的失败次数和一次抽取的抖动，不自行访问数据库或随机数。提示清理在补建事务中执行。

`WorkerResources` 由采集侧一次组装共享，lookup 的 `LookupPacer` 只负责查询节奏，`TcpLimits` 单独负责同 IP 许可。观察 guard 位于 diagnostics 的 observations 模块；完整 `AttemptContext` 包含冻结类别，汇总视图使用省略相应维度的键，不用 false 充当忽略维度的哨兵。histogram 只计算，DHT 与采集各自输出日志，target 对应各自模块。

会话故障分类和同步发布位于 `app/session/faults`，任务所有权、共同关闭期限与重试顺序仍属于 Session；故障发布同步执行。
