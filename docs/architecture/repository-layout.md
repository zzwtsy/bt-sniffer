# 源码目录设计

本页回答功能放哪里、状态由谁持有、增加行为时从哪里修改。编译边界以 [main.rs](../../src/main.rs) 的模块声明和 [Cargo.toml](../../Cargo.toml) 为准；系统数据流见[架构概览](overview.md)。

## 实际编译布局

```text
src/
├── main.rs                 CLI → 日志 → runtime → app → 最终诊断
├── app/                    参数、资源组装、引导、会话和日志输出
│   ├── config.rs           CLI 定义、默认值和冲突校验
│   ├── collection_config.rs  CLI 到采集与采样策略的转换
│   ├── bootstrap/          DNS 引导与启动查询
│   ├── session/            任务监督、故障分类和有序关闭
│   ├── logging/            过滤、格式、writer 和队列诊断
│   └── sockets.rs          地址族与 socket 绑定策略
├── dht/                    DHT 协议与节点状态
│   ├── dispatcher/         单节点状态与事件循环
│   │   ├── runtime/        接收、命令、计时器和任务推进
│   │   ├── peer_queries/   get_peers 与 announce 请求处理
│   │   ├── sampler/        采样接口及 durable 冷却预约
│   │   └── sampling/       采样查询与结果处理
│   ├── krpc/               消息编码、解码和 compact 地址
│   ├── udp/                UDP 边界与字典顺序兼容
│   ├── transaction/        响应匹配、截止时间和取消
│   ├── routing/            分地址族路由表
│   ├── persistence/        身份、联系人、采样冷却的 SQL
│   ├── traffic/            双栈共享的流量预算与统计
│   ├── peer_store/         有界 peer 缓存
│   ├── token/              announce token
│   └── shortlist.rs        有界查找候选
├── collection/             从发现到 metadata 提交
│   ├── scheduler.rs        领取与 worker 调度
│   ├── worker.rs           单轮采集和结果处理
│   ├── candidate_selector/ 同步候选顺序与每轮已选地址集合
│   ├── lookup.rs           DHT peer 查找
│   ├── lifecycle.rs        worker 取消、回收及故障处理
│   ├── jobs/               接纳、领取、提示、状态转换和查询
│   │   └── policy/         同步领取轮转与借用规则，游标实例由 scheduler 持有
│   ├── peer/               metadata 校验与 TCP 协议
│   │   ├── session/        peer 会话与分片获取
│   │   └── wire/           帧、扩展握手和兼容边界
│   ├── ingest/             采样批次保存与确认
│   ├── diagnostics/        采集指标及事件聚合
│   ├── status.rs           每分钟诊断组装与数据库快照输出
│   ├── store.rs            采集 SQL 入口
│   ├── backpressure.rs    采样背压策略
│   ├── tcp_limits.rs      TCP 许可
│   ├── records.rs         持久化记录类型
│   ├── failure.rs         失败分类
│   └── tests/             离线链路、恢复、边界和 ignored 验收
├── observation/            强类型事件、关联上下文、有界历史和当前状态
├── monitor/                只读 HTTP/SSE、刷新和连接所有权
├── storage/                SQLite 线程、命令预算、schema 和地址表示
├── address.rs              网络地址策略
├── clock.rs                时间与可控测试时钟
├── histogram.rs            无业务依赖的固定桶算法
├── info_hash.rs            v1 hash 类型
└── acceptance/             仅 cfg(test) 的验收身份与证据辅助
```

图中省略各目录的 `mod.rs`、单元测试及细分实现文件；完整文件以模块声明为准。观测与 HTTP 的边界见[观测接口](../domains/monitoring.md)。

## 划分依据与依赖方向

`app` 组装具体组件，不承载 KRPC 或领取 SQL。`Session` 持有任务关闭责任；每个 dispatcher 持有本地址族路由、transaction 和采样状态。采集规则放在 `collection`，不会让 DHT 解释重试状态或接纳策略。

`observation` 不依赖 HTTP、SQLite 或调度；`monitor` 通过 DHT handle 和 CollectionStore 读取，app 负责注入并由 Session 收尾。

依赖主方向为 `main → app → collection / dht → storage 与共享基础模块`；采集通过 DHT handle 查询 peer。`DhtStore` 与 `CollectionStore` 是业务侧的两个具体 SQL 入口，复用一个 `StorageHandle`，并非两条数据库线程。`storage` 不反向调用业务调度；`histogram` 只计算桶值，由业务模块命名日志字段。

复杂目录按独立变化原因分文件：`jobs` 将接纳、领取、转换、提示和统计查询分开，以事务边界维持一致性；`peer` 将会话推进与不可信字节解析分开；`diagnostics` 按观测对象聚合；dispatcher 的文件共同实现节点状态，不意味着每个文件都是独立服务。新增状态应先确定唯一所有者，再决定是否拆目录。

单 crate 使内部契约可以使用私有或 `pub(crate)` 可见性，减少公共 API 负担。当前不需要通用 repository trait 或插件层；真实出现独立复用、部署或替换需求时才重访 crate 和抽象边界，承担的代价包括跨 crate API 维护及测试迁移。

## 仓库辅助目录

| 路径 | 归属与变化原因 |
| --- | --- |
| `tests/logging_cli.rs` | Cargo 独立集成测试目标，通过真实 CLI 子进程验证日志与退出；不属于 src 的 cfg(test) 模块 |
| `scripts/`、`scripts/tests/` | 开发检查、产品诊断及 Python 离线回归；公开检查入口固定为 check.py |
| `tools/docs/` | Markdown 工具版本、锁文件与配置，不承载产品逻辑 |
| `.github/workflows/` | CI 准备环境并调用同一检查入口 |
| `.agents/skills/` | 专项方法、按需参考和独立异步示例 |
| `docs/` | 任务导航和当前契约，布局见[文档目录设计](documentation-layout.md) |
| `target/` | 编译产物与开发检查证据，不作为源码或文档事实来源 |

## 模块与测试归属

单文件模块用 `name.rs`；需要两个及以上文件（含独立测试文件）的模块用 `name/mod.rs`。入口可以直接包含实现，不强制再分 `types`、`service`、`impl`。Cargo 编译目标入口保留标准位置。新增模块同步声明与最小可见性，不用 `#[path]` 绕开归属。

局部单元测试放所属模块的 `tests.rs` 或小型内联 `tests`；跨采集链路放 `collection/tests`，应用组装放 `app/tests.rs`。SQL 调度比较放 `collection/jobs`，辅助数据与断言就近维护。`test_storage` 和 `acceptance` 仅在测试配置下使用，不为它们扩大生产 API。

[异步示例](../../.agents/skills/rust-async-patterns/examples/Cargo.toml) 是独立 workspace，用于验证通用方法，不承载产品行为。主 crate 的测试不覆盖它，检查范围 `examples` 单独执行它；选择规则见[验证指南](../development/validation.md)。

## 常见修改入口

| 任务 | 首改位置与调用链 | 测试与联动文档 |
| --- | --- | --- |
| 增加 CLI 参数 | `app/config.rs` → `app/mod.rs` 或 `collection_config.rs` → 状态所有者 | `app::config::tests`；[运行](../operations/running.md)，有启动字段时核对事件 |
| 增加 DHT 行为 | `krpc` / `udp` 校验 → dispatcher → transaction / traffic | 相邻 DHT 测试；[DHT](../domains/dht.md) |
| 调整采集策略 | `jobs/admission.rs`、`jobs/policy`、`claim.rs`、`transitions.rs`、`candidate_selector` → scheduler / worker | jobs 测试和 `collection/tests/recovery.rs`；[采集](../domains/collection.md) |
| 增加存储操作 | 所属 Store 与事务实现；改变结构才修改 `storage/schema.rs` | Store 和 storage 测试；[存储](../domains/storage.md) |
| 增加日志事件 | 业务事件所有者；输出机制才改 `app/logging` | 事件类型测试、Python 消费者；[事件参考](../domains/log-events.md) |

定位时先读本页对应行，再读专题的源码与验证入口，不需要加载全部模块和文档。

## 前端垂直切片

`web/src/app` 组装 Provider 与同步生命周期，`routes` 挂载页面，`features` 按总览、DHT、发现、任务、hash、详情、metadata、事件组织。切片不引用彼此内部代码；共享展示进入 `components/observation`，shadcn Chart 保留在 `components/ui`。HTTP 与 SSE 公共契约分别位于 `lib/api` 和 `lib/observation`。详见[前端开发说明](../../web/README.md)。
