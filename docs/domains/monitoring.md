# 发现流程观测接口

源码入口：[观测缓存](../../src/observation/mod.rs)、[HTTP/SSE](../../src/monitor/api.rs)、[服务生命周期](../../src/monitor/mod.rs)、[数据库读取](../../src/collection/inspection.rs)。验证入口：`observation::tests`、`monitor::tests`、`collection::inspection::tests`、`collection::tests::discovery::observed_sampling_preserves_result_and_links_all_stages`。

## 启用与事实边界

通过 [monitor-listen 参数](../operations/running.md#参数)显式启用，只监听 loopback。浏览器使用同源代理访问，无 CORS。所有接口只支持 GET；不提供任务操作、原始 metadata、piece hash 或 torrent 导出。省略参数时不创建观测缓存、刷新任务或 HTTP 服务；目录回填也可由 fetch 启用，事件变长数据通过惰性闭包构造。

SQLite schema v5 是任务、metadata 和可重建查询目录的持久事实。过程事件仅属于当前 `run_id`，复用日志运行标识；重启丢失过程历史。监控数据不参与领取、peer 选择和重试。来源缺失返回 `null` 或不提供关联字段，不能通过当前提示推断最初发现来源。

内存状态独立维护，不依赖重放历史。节点、collector 和数据库分别携带观察时间，快照不承诺跨节点或 SQLite 原子一致。指标读取不清空日志统计区间；固定桶分位值表示桶上界，不能跨区间相除构造成功率。

## GET 接口

统一前缀 `/api/v1`。目录列表使用页码偏移分页，文件清单使用 keyset 游标；SSE 的 `after` 是 `run_id:sequence` 游标。

| 路径 | 参数与内容 |
| --- | --- |
| `/health` | 运行阶段和缓存数据源状态；collector 返回独立暂停状态，完整指标见 snapshot 的 runtime |
| `/snapshot` | `schema_version`、`window`、`runtime` 当前状态及 `cached` 节点、共享流量和数据库统计 |
| `/stream` | SSE，after 格式为 run_id:sequence |
| `/torrents?q=&page=&limit=` | 空 q 返回最近目录；非空 q 为 3–200 字符名称/已纳入索引的完整文件路径字面子串；page 从 1 开始，默认 50、最大 100 条；响应含结果总数 `total`、回显页码 `page` 与 `index` 进度，越界页返回空 items |
| `/torrents/{hash}` | 40 位完整 v1 或 64 位完整 v2 hash 的摘要；重新核对对应完整摘要和字典 |
| `/torrents/{hash}/files?after=&limit=` | 原始顺序文件清单；默认及最大 100 条，单文件也返回一条 |

除此之外的 `/api/v1` 路径不匹配监控路由，GET 返回 404；不提供旧接口重定向。非 GET 请求仍由只读中间件返回 405。

`*_ms` 是毫秒：事件的 `at_ms`、数据源的 `observed_at_ms` 表示 UTC Unix 时间；`elapsed`、`timeout` 表示时长，阶段耗时由单调时钟计算。`bytes` 表示字节数。SQLite 中的任务与 metadata 仍由采集内部维护，首页只读取统计值。

`runtime.sources` 按 config、monitor、collector、database 保存独立的 observed_at_ms 和 value；`runtime.active` 是当前阶段及关联上下文。collector 的 `tracked_tcp_ips` 包含持有许可或等待许可的 IP 条目，不能作为 socket 数；running_workers 是当前 worker 数。

`cached.traffic` 为双栈共享累计统计，只计一次。packets、bytes、queued、inflight_cancelled、queue_wait 的数组顺序为 collector、control、sampling、verification；queue_finished 内层依次是发送、取消、超时、本地拒绝，blocked 内层依次是类别配额、目标 IP 配额、字节配额、IP 表容量。queue_wait.buckets 的毫秒上界依次为 1、5、10、50、100、500、1000、2000、5000、10000、30000、60000、180000，第 14 项是溢出桶，后续固定预留项为零。普通指标 durations 的 p50/p95/p99 直接返回 upper_bound_ms 或 exceeds_ms，空样本两者均为 null。

缓存数据源的 `available=false` 表示无可用结果；数据库刷新失败保留上一结果并设 `stale=true`。未知值不能替换为零。数据库统计首次失败时不生成假的空库统计。

错误结构为 `{"error":{"code":"invalid_query","message":"参数或游标格式无效"}}`。错误码是稳定机器标识，message 提供对应说明。

目录 `index` 含 `indexed`、`total`、`complete` 和 `search_complete`。`complete` 表示每条 metadata 的目录行都具有已确认的搜索覆盖状态；`search_complete` 表示所有文件路径都已纳入搜索文本。若后者为 false，可能仍在逐条回填、存在不可解析的目录行，或派生文本达到 12 MiB 上限；达到上限时名称仍会索引，只加入上限内的完整路径。按路径搜索及空结果可能不完整，该状态不会通过增大 metadata 接纳上限或写入半截路径来掩盖。

| HTTP 状态 | 常见 code | 含义 |
| --- | --- | --- |
| 400 | invalid_query、invalid_cursor | 非法参数或游标 |
| 404 | not_found | 路径不匹配或资源不存在 |
| 405 | read_only | 非 GET 请求 |
| 429 | rate_limit、request_capacity、database_capacity、stream_capacity | 当前资源额度耗尽 |
| 503 | source_unavailable、response_capacity、shutting_down | 数据源、响应容量或运行阶段不可服务 |
| 504 | query_timeout、request_timeout | 查询执行或请求等待超时 |

## 事件与关联

公共字段为 `schema_version`、`run_id`、字符串 `sequence`、`at_ms`、`kind`、`step`、`context`、`result`、`data` 和 `truncated`。关联标识均为稳定字符串；generation 为整数。事件 schema_version=2，snapshot 仍为 1。context 可包含 node_id、swarm_key、generation、observation_id、batch_id、peer_attempt_id、rpc_id、span_id、parent_span_id。RPC ID 只属于应用上下文，不修改 KRPC transaction 或匹配规则。

kind 为 lifecycle、bootstrap、routing、rpc、sampling、discovery、admission、job、lookup、peer、piece、validation、commit、retry、backpressure。事件分别表达负责模块已知的事实；started、waiting、sent、cancelled 与 applied 不能互换。事务内失败不能报告 applied；即使调用者取消等待，已接纳数据库闭包仍会报告实际提交结果。Stale 不表示提交成功。

同次 peer 尝试共享 peer_attempt_id，各阶段通过 span 层级关联；切换 peer 使用新的分片状态。重复分片与新增分片分别记录。事件不包含 token、原始报文或 metadata 正文。长字段截断时提供 truncated 标记，不应把截断对象解释为完整协议数据。

`window` 提供 run_id、oldest、latest、retained、bytes、evicted、truncated、max_events、max_bytes、retention_ms。发生淘汰后客户端应按窗口游标重新同步，不能据此断言某个阶段从未发生。snapshot 的窗口游标只描述事件流。

## SSE 同步

初次连接发送 `hello`，内容为当前窗口。没有游标时从连接时刻之后接收新事件；浏览器重连通过 `Last-Event-ID` 发送 run_id:sequence，同时存在 after 时以前者为准。客户端不依赖历史 HTTP 页面，连接游标由 snapshot 和 SSE 自身维护。

`events` 按全局序号补发，每批最多 100 条，SSE id 为本批最后游标。`snapshot` 每秒发送当前状态；每 15 秒提供保活。生产者仅更新共享历史和顺序通知，不为客户端复制完整队列。

运行标识改变、游标早于窗口或超前时发送 `reset` 后结束连接。客户端应重新获取 snapshot，再用其中窗口 latest 和 run_id 建立流；旧过程显示不可用。慢客户端落后也使用此流程，不阻塞生产者。单条已交给网络的有限批次可完成发送，后续读取重新检查窗口。

## 资源与退出

| 资源 | 固定上限或周期 |
| --- | --- |
| 过程历史 | 15 分钟、65,536 条、变长载荷 32 MiB，先到者淘汰 |
| 单事件编码 | 8 KiB |
| JSON 响应或 SSE 状态 | 1 MiB；事件批次最多 100 条 |
| 内存刷新 / 路由刷新 / 数据库统计 | 1 秒 / 5 秒 / 30 秒；故障等待可能延迟刷新 |
| HTTP / SSE | 最多 16 个连接，其中最多 4 个 SSE |
| HTTP 请求头读取 | 30 秒 |
| 普通请求 | 并发 4、整体每秒 10 次、突发 10 次 |
| 监控数据库操作 | 已接纳未完成最多 1 个 |
| 请求等待 / SQLite 执行预算 | 2 秒 / 100 毫秒 |

载荷预算不代表整个进程堆内存上限：固定记录、独立当前状态、有限响应及网络缓冲另有开销。事件历史只保留 SSE replay 所需的有界记录；当前状态独立保存，不为已删除页面维护发现摘要或尝试聚合索引。

HTTP 请求头读取由 Hyper 的 Tokio 计时器限制为 30 秒，超时连接退出后释放连接额度；这是协议层行为，不承诺 API JSON 错误响应。该期限不代表通用连接空闲超时，也不限制 SSE 响应持续时间。完整请求进入 API 后另受 2 秒请求等待预算约束。

监控 SQL 经过原 Store 和唯一数据库线程。进度回调每 1000 个 SQLite VM 指令检查取消及执行预算，作用域结束移除；不能硬中断阻塞磁盘 I/O。查询许可由数据库闭包持有，HTTP 超时不提前释放，不形成无界补发。目录回填每次处理一条、批次间至少等待 100 ms，HTTP 已占数据库许可时跳过本轮；提交目录行和计数后才推进，重启从缺失行恢复。监控读取中断不会触发采集写入暂停。

绑定失败是启动失败；运行期监控故障只停止监控。Session 开始关闭后停止接受请求和刷新、取消监控查询，已有 SSE 仍可读业务收尾事件。业务及数据库收尾后结束 SSE，最终推送最多占共同剩余期限中的 1 秒。HTTP 任务及连接由 Session 的 Monitor 持有并回收，不另追加 30 秒预算；整体关闭规则见[生命周期](../architecture/lifecycle.md)。

## 目录协议字段

列表按 metadata 去重，有效 hybrid 返回 v1、v2 两个完整身份，两个详情地址定位同一记录。40 位值只作为完整 v1 身份解释，不能当作完整 v2 摘要。`format`、`identities`、`semantic_status`、`semantic_reason`、`verification`、`validation_scope=info_only`、`piece_layers=not_fetched` 明确标示解析和验证范围；未知统计为 null。

文件项含 kind、hidden、executable、symlink_path 和 sha1 提示。前端只对可用 v1 身份启用第三方截图请求；纯 v2 不拼接 btih 请求。过程事件中的 swarm_key 是 20 字节查找键，提交事件 data.identities 才是完整身份列表。

回填由 Session 持有，在 fetch 或 monitor 启用时启动，不依赖浏览器访问；与 HTTP 共享单个数据库读取许可，关闭时由 Session 取消并确认结束。

详情在验证原始摘要及完整字典后，以一次即时解析生成语义状态、原因与统计，不依赖目录是否已经回填。身份与匹配依据仍来自持久关联，GET 不写目录或补别名；列表在后台回填后更新，因此回填期间可与详情的语义状态不同。页面即使无法展示文件清单也保留语义失败原因，不重复显示通用不可解析提示。
