# 发现流程观测接口

源码入口：[观测缓存](../../src/observation/mod.rs)、[历史摘要](../../src/observation/queries.rs)、[HTTP/SSE](../../src/monitor/api.rs)、[服务生命周期](../../src/monitor/mod.rs)、[数据库读取](../../src/collection/inspection.rs)。验证入口：`observation::tests`、`monitor::tests`、`collection::inspection::tests`、`collection::tests::discovery::observed_sampling_preserves_result_and_links_all_stages`。

## 启用与事实边界

通过 [monitor-listen 参数](../operations/running.md#参数)显式启用，只监听 loopback。浏览器使用同源代理访问，无 CORS。所有接口只支持 GET；不提供任务操作或 metadata 正文。省略参数时不创建观测缓存、刷新任务或 HTTP 服务，事件变长数据通过惰性闭包构造。

SQLite schema 仍为 v2，是任务和 metadata 的持久事实。过程事件仅属于当前 `run_id`，复用日志运行标识；重启丢失过程历史。监控数据不参与领取、peer 选择和重试。来源缺失返回 `null` 或不提供关联字段，不能通过当前提示推断最初发现来源。

内存状态独立维护，不依赖重放历史。节点、collector 和数据库分别携带观察时间，快照不承诺跨节点或 SQLite 原子一致。指标读取不清空日志统计区间；固定桶分位值表示桶上界，不能跨区间相除构造成功率。

## GET 接口

统一前缀 `/api/v1`，JSON 响应。列表默认 `limit=50`，范围 1 至 100，使用 `after` 续页；返回 `next` 为 `null` 时结束。游标视为不透明字符串，不保证多次请求之间数据库冻结。

| 路径 | 参数与内容 |
| --- | --- |
| `/health` | 运行阶段和缓存数据源状态；collector 返回独立暂停状态，完整指标见 snapshot 的 runtime |
| `/snapshot` | `schema_version`、`window`、`runtime` 当前状态及 `cached` 节点、共享流量和数据库统计 |
| `/dht/nodes` | dispatcher 列表，`id` 为本次运行的节点索引字符串 |
| `/dht/nodes/{id}/routing` | 路由桶及分页联系人；after 为联系人偏移，只对当前缓存有意义 |
| `/discoveries` | 当前保留的采样批次和发现观察摘要；after 为最早保留事件序号 |
| `/discoveries/{id}` | 发现对象关联事件，支持 after、limit |
| `/hashes` | 按 hash 升序列出发现时间，after 为 hash |
| `/hashes/{hash}` | 发现时间、当前 job、有效 peer_hints、metadata 摘要及 original_source |
| `/jobs` | 当前任务；可用单个 state 筛选：pending、running、retry_wait、dormant、succeeded |
| `/hashes/{hash}/attempts` | 保留的 generation 和 peer 尝试摘要，after 为 generation |
| `/metadata` | hash、字节数、获取时间及校验语义摘要，按 hash 升序 |
| `/events` | after 为序号，支持 hash、object、kind 的交集筛选 |
| `/stream` | SSE，after 格式为 run_id:sequence |

hash 输入必须为 40 个十六进制字符，输出统一小写。未筛选任务按 hash 排序；单状态任务按 due_at 和 hash 排序，以复用现有索引。数据库列表不计算每页总数，不提供任意搜索与排序。

`*_ms` 是毫秒：at、observed_at、first_seen、last_seen、fetched_at、due_at、updated_at 表示 UTC Unix 时间；elapsed、timeout 表示时长，阶段耗时由单调时钟计算。bytes 表示字节数。job 的 `remote_failures` 是已记录远端失败次数，`generation` 是领取版本。metadata 的 `verification=validated_before_commit` 表示采集提交前验证过，`content_rechecked=false` 表示查询没有重新读取并校验正文。

`runtime.sources` 按 config、monitor、collector、database 保存独立的 observed_at_ms 和 value；`runtime.active` 是当前阶段及关联上下文。collector 的 `tracked_tcp_ips` 包含持有许可或等待许可的 IP 条目，不能作为 socket 数；running_workers 是当前 worker 数。

`cached.traffic` 为双栈共享累计统计，只计一次。packets、bytes、queued、inflight_cancelled、queue_wait 的数组顺序为 collector、control、sampling、verification；queue_finished 内层依次是发送、取消、超时、本地拒绝，blocked 内层依次是类别配额、目标 IP 配额、字节配额、IP 表容量。queue_wait.buckets 的毫秒上界依次为 1、5、10、50、100、500、1000、2000、5000、10000、30000、60000、180000，第 14 项是溢出桶，后续固定预留项为零。普通指标 durations 的 p50/p95/p99 直接返回 upper_bound_ms 或 exceeds_ms，空样本两者均为 null。

缓存数据源的 `available=false` 表示无可用结果；数据库刷新失败保留上一结果并设 `stale=true`。未知值不能替换为零。数据库统计首次失败时不生成假的空库统计。

错误结构为 `{"error":{"code":"invalid_query","message":"参数或游标格式无效"}}`。错误码是稳定机器标识，message 提供对应说明。

| HTTP 状态 | 常见 code | 含义 |
| --- | --- | --- |
| 400 | invalid_query、invalid_cursor | 非法参数或游标 |
| 404 | not_found、node_not_found、history_unavailable | 对象不存在或过程已不可用 |
| 405 | read_only | 非 GET 请求 |
| 429 | rate_limit、request_capacity、database_capacity、stream_capacity | 当前资源额度耗尽 |
| 503 | source_unavailable、routing_unavailable、response_capacity、shutting_down | 数据源、响应容量或运行阶段不可服务 |
| 504 | query_timeout、request_timeout | 查询执行或请求等待超时 |

## 事件与关联

公共字段为 `schema_version`、`run_id`、字符串 `sequence`、`at_ms`、`kind`、`step`、`context`、`result`、`data` 和 `truncated`。关联标识均为稳定字符串；generation 为整数。context 可包含 node_id、hash、generation、observation_id、batch_id、peer_attempt_id、rpc_id、span_id、parent_span_id。RPC ID 只属于应用上下文，不修改 KRPC transaction 或匹配规则。

kind 为 lifecycle、bootstrap、routing、rpc、sampling、discovery、admission、job、lookup、peer、piece、validation、commit、retry、backpressure。事件分别表达负责模块已知的事实；started、waiting、sent、cancelled 与 applied 不能互换。事务内失败不能报告 applied；即使调用者取消等待，已接纳数据库闭包仍会报告实际提交结果。Stale 不表示提交成功。

同次 peer 尝试共享 peer_attempt_id，各阶段通过 span 层级关联；切换 peer 使用新的分片状态。重复分片与新增分片分别记录。事件不包含 token、原始报文或 metadata 正文。长字段截断时提供 truncated 标记，不应把截断对象解释为完整协议数据。

历史页和尝试摘要标注 `complete`、`partial` 或 `unavailable`。发生淘汰后采用保守的 partial 标记，不能据此断言某个阶段从未发生。`window` 提供 run_id、oldest、latest、retained、bytes、evicted、truncated、max_events、max_bytes、retention_ms。snapshot 的窗口游标只描述事件流。

## SSE 同步

初次连接发送 `hello`，内容为当前窗口。没有游标时从连接时刻之后接收新事件；需要历史时先获取事件页或指定游标。浏览器重连通过 `Last-Event-ID` 发送 run_id:sequence，同时存在 after 时以前者为准。

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

载荷预算不代表整个进程堆内存上限：固定记录、独立当前状态、有限响应及网络缓冲另有开销。发现摘要索引随事件追加和淘汰同步维护，每条保留事件最多对应一条轻量记录；对象 ID 在索引内部共享，其变长分配计入历史载荷预算，固定索引节点数量受事件条数限制。列表查询只复制一页摘要，在释放历史锁后构造 JSON；索引不持有被淘汰事件的共享引用。其他历史查询仍在保留窗口内汇总。

HTTP 请求头读取由 Hyper 的 Tokio 计时器限制为 30 秒，超时连接退出后释放连接额度；这是协议层行为，不承诺 API JSON 错误响应。该期限不代表通用连接空闲超时，也不限制 SSE 响应持续时间。完整请求进入 API 后另受 2 秒请求等待预算约束。

监控 SQL 经过原 Store 和唯一数据库线程。进度回调每 1000 个 SQLite VM 指令检查取消及执行预算，作用域结束移除；不能硬中断阻塞磁盘 I/O。查询许可由数据库闭包持有，HTTP 超时不提前释放，不形成无界补发。监控读取中断不会触发采集写入暂停。

绑定失败是启动失败；运行期监控故障只停止监控。Session 开始关闭后停止接受请求和刷新、取消监控查询，已有 SSE 仍可读业务收尾事件。业务及数据库收尾后结束 SSE，最终推送最多占共同剩余期限中的 1 秒。HTTP 任务及连接由 Session 的 Monitor 持有并回收，不另追加 30 秒预算；整体关闭规则见[生命周期](../architecture/lifecycle.md)。
