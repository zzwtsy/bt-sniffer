# 日志输出与字段契约

日志同时输出 stderr 文本和进程工作目录下的 `logs/bt-sniffer.YYYY-MM-DD.jsonl`，为业务 stdout 留出空间。
两端事件范围一致，默认本程序 INFO 及以上、第三方 WARN 及以上。时间使用 UTC，保留 level、target，不默认输出线程、文件和行号。

```bash
# logs/ 属于当前工作目录，不随 --state-dir 改变。
./bt-sniffer --state-dir ./state --sample --fetch
# 非空 RUST_LOG 完整替换默认规则；这里显式保留全局和本程序基线。
RUST_LOG='warn,bt_sniffer=info,bt_sniffer::collection::worker=debug' ./bt-sniffer --sample --fetch
```

`RUST_LOG` 未设置或为空时使用 `warn,bt_sniffer=info`；非空值按标准 EnvFilter 语法完整替换，不隐式合并。
非法语法和非 Unicode 值在创建日志目录前报告初始化失败。`application_start.log_filter` 记录实际指令。
`RUST_LOG=error` 或 `off` 会抑制 INFO 摘要和健康事件，不绕过过滤器补写；需要完整诊断时使用默认过滤。
不加载 `.env`，不提供日志 CLI、格式切换或热更新。文件禁用 ANSI，stderr 仅连接终端时启用颜色；显式设置覆盖依赖读取的 NO_COLOR 默认值。

文件每个完整行是一个 JSON 对象：`timestamp`、`level`、`target`、`fields`、顶层 `run_id`。
业务事件标识和版本分别位于 `fields.event`、`fields.schema_version`，数值和布尔保留类型，不将 fields 展开到顶层。
第三方事件保留其自身字段，不补造业务 event/schema。标准 formatter 保留可用的 span 上下文；本程序没有逐任务 span。
文本末尾附带同一个 run_id，包含专用数据库线程和退出事件；run_id 不依赖根 span 的传播。

## 文件、部署与消费者

文件使用 `tracing-appender 0.2.5` 的 DAILY 轮转：按 UTC 自然日命名，跨日后的首次实际写入触发轮转，不在午夜额外启动任务或生成空文件。后台队列中的日志按实际写入时刻选择文件；跨午夜积压时，事件时间与文件日期可以不同。

最多保留 7 个匹配文件，不是严格保留 7 天，不保证恰好保留 7 个，也不限制单文件大小。启动和轮转时清理，锁定依赖在打开文件前为新文件预留名额，同日重启可能少保留一个。候选文件以 `bt-sniffer` 前缀和 `jsonl` 后缀匹配，并非严格校验日期名；logs/ 应为本程序专用目录，不放置同样匹配的其他文件或归档。正常 I/O 条件下满足文件数量上限；删除失败会报告错误，不能保证上限。

- 工作目录不可写且没有预建可写 logs/ 时，启动失败。只读根文件系统部署需要挂载可写 logs/；不自动改用状态目录或其他路径。
- `--instance` 仍用于节点身份，状态目录锁只保护数据库，不能隔离日志。多进程必须使用不同工作目录和独立 logs/；即使 --state-dir 不同，也不支持共写相同日志文件。
- stderr 与文件各写一份，journald 的留存与应用文件相互独立，磁盘占用也分别计算；部署层负责磁盘字节配额和告警。
- 旧 `.log` 不混写、不自动删除，也不计入 JSONL 的 7 个文件上限，须由部署者另行归档。
- 内部测试继续使用 JSON subscriber 检查事件字段类型；独立验收 JSON 报告与生产日志格式无关，历史报告保留原样。

## 输出与关闭边界

初始化在 CLI 解析之后、runtime 创建之前完成。help/version 和 CLI 解析失败不创建日志资源；日志目录或文件创建失败时，stderr 报告路径、动作和底层错误，进程返回失败，不继续启动业务。subscriber 注册失败也返回初始化错误。

文件与 stderr 各有一个独立 writer 线程、4,096 条有界队列，均为 lossy 模式。慢文件不阻塞终端，慢终端不阻塞文件；队列满载丢弃本次投递，不等待输出端腾出名额。格式化仍发生在调用线程，队列条数不是字节硬上限，不应记录完整报文、metadata 或巨大对象。两个标准 fmt 层分别格式化；JSON 包装通过对象解析和序列化补充 run_id，不承诺零分配。投递丢弃由下述 logging_queue 事件报告。

运行中轮转创建失败由依赖向 stderr 报错并保留旧 writer；删除失败同样直接报告。普通后台写入 I/O 错误并非都能通过现有依赖对外观察，本实现没有针对这些写入错误的故障监督或自动降级，也不承诺无损日志。

main 持有两个 WorkerGuard，直到会话、runtime 收尾、最终错误事件及最终队列观察事件发出之后。每个 guard 尝试投递关闭标记最多等待 100 ms，再等待刷新交接最多 1,000 ms；两个 guard 的等待可能累计。这独立于会话 30 秒清理和 runtime DNS 等待上限，不保证强杀、断电、I/O 故障时日志完整，也不等于持久落盘。验收仍以独立报告、正常退出和数据库复核为依据。

## 日志健康与事件边界

`logging_queue` 为 schema_version=1，两端各输出一条：`sink=file/stderr`、`queue_capacity=4096`、
`dropped_total`、`dropped_since_last_report`、`final_snapshot`。复用应用的 60 秒摘要（首次 tick 可立即发生），
先获取两端快照再输出；增量大于零为 WARN，否则 INFO。初始化成功后，提前失败或正常收尾均在最终业务错误之后报告最终值。

这是投递丢弃观察，不是队列当前占用或全部 I/O 错误。报告自身和最后刷新阶段的丢弃不包含在最终快照中，
它也可能被过滤或丢弃，不能作为无损日志证明。没有独立监控线程；业务循环等待初始化或收尾时不持续输出分钟统计。

INFO 用于重要生命周期、运行状态和聚合；DEBUG 用于具体 peer 排障。
WARN 表示值得关注的异常或降级，ERROR 表示操作失败或关键能力失效；普通公网 peer 超时不逐次升级为 ERROR。
成功以实际完成边界为准，下载不等于 Applied 提交。稳定类别由代码类型决定，error/message 只说明原因。

| event | 字段与含义 |
| --- | --- |
| bootstrap_connected | phase=bootstrap、family，已验证引导邻居 |
| bootstrap_retry_scheduled | phase=bootstrap、family、retry_after_ms，无邻居时安排重试 |
| bootstrap_dns_failed / bootstrap_dns_timeout | phase=dns、seed；失败有 error，超时有 timeout_ms |
| bootstrap_query_failed | DEBUG、phase=query、error |
| ipv6_listen_unavailable | phase=startup、action=continue_ipv4、error |
| node_status | phase=running，实际监听地址及路由/采样状态 |
| application_shutdown_started | phase=shutdown，开始有序收尾，不表示完成 |
| session_fault | schema_version=2、phase=running；kind 为 storage_write/database_exited/collector_failed/task_exited/task_failed；fatal 决定 action=shutdown/pause_collection |
| runtime_start_failed / application_failed | phase=startup/exit、error；初始化失败在 subscriber 可用前直接向 stderr 报告 |
| peer_fetch_failed | DEBUG、peer、stage、error、category；既有分类聚合不变 |
| storage_capacity_paused | WARN、phase=running、action=pause_collection、state_bytes、limit_bytes；达到配置上限减 64 MiB 的保护阈值后暂停采集 |
| sample_batch_save_started | DEBUG、phase=persist、responder、target、observed_at_ms（UTC）、interval_secs、num、count、confirmed_offset；开始保存或重试批次，offset 为已确认条数，不表示整个批次已提交 |
| metadata_committed | DEBUG、phase=commit、hash、source、peer_id、bytes；领取仍有效且事务提交成功，返回 Applied |

表中未另标的事件 schema_version=1。session_fault 保留 ERROR：存储操作已经失败，即使整个进程暂时仍能运行。
即时会话通知和退出诊断分别说明阶段，可能包含同一个故障；不将事件行数当成独立错误总量，也不按字符串全局去重。
KRPC 远端错误说明在 Display 边界将控制字符转义，转义后的说明最多 256 个字符，超长额外追加 `…`；不截断 UTF-8 字符或转义序列。无效 UTF-8 使用替换字符，原始错误字节与分类保持不变。

## 结构化聚合

下表事件包含独立的 `schema_version` 和稳定 `event` 标识；当前日志契约版本为 3，具体事件版本见文末。中文 message 供阅读，不作为解析键。聚合值使用具名数值字段，不以 Debug 字符串作为解析契约。

| event | 口径 |
| --- | --- |
| `collector_counter` | scope=total/interval；counter 名称使用当前枚举，value 为数值 |
| `peer_handshake_diagnostic` | 标准握手至扩展协商结束的整体耗时，按来源、地址族、结果与期限范围聚合 |
| `duration_histogram` | timing、class、count、overflow 和 p50/p95/p99 的上界或溢出字段 |
| `dht_traffic` | 入站、回复、限流丢弃、队列超时及双栈有效响应，scope 分区间/累计 |
| `dht_class` | collector/control/sampling/verification，包、字节、接纳待发及各结束原因 |
| `dht_wait_reason` | 每请求每原因最多一次；class_quota/destination_ip_quota/upload_bytes/ip_table_capacity |
| `verification_admission` | admitted/duplicate/cooldown/capacity/ip_table/succeeded/failed |
| `dht_occupancy`、`dht_queue_occupancy` | 当前占用 gauge，不作累计或区间增量 |
| `database_jobs` | 数据库状态数、metadata 数量/字节，final_snapshot 表示关闭最终快照 |
| `collector_status`、`collector_failure` | 当前采集状态、累计任务失败类别 |
| `sampling_backpressure` | 当前暂停原因、累计暂停时间和恢复次数 |
| `session_shutdown` | 关闭结果 success 和 error_count；具体故障由错误事件输出 |

分位数输出 `p95_upper_bound_ms` 或 `p95_exceeds_ms` 等数值字段。无样本时两个字段都缺省；溢出时只有 exceeds，不能用 0 替代缺失或把超过上界当精确耗时。领取等待有限桶到 24 小时，网络阶段到 180 秒。

`dht_class.dequeued_to_send` 仅表示离开待发进入发送流程，实际 socket 发出量看 packets。开始数和结束数可能属于不同区间，不能直接相除。诊断日志中的地址、错误仍可能为字符串；没有逐任务 span 或逐包事件。

## systemd 示例

提前创建 `/var/lib/bt-sniffer` 并将其及已有 logs/ 的必要读写权限授予专用账号。二进制仍放在 `/opt/bt-sniffer`；下面不安装服务或修改服务器配置。

```ini
[Service]
Type=simple
User=bt-sniffer
WorkingDirectory=/var/lib/bt-sniffer
ExecStart=/opt/bt-sniffer/bt-sniffer --state-dir /var/lib/bt-sniffer --sample --fetch
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bt-sniffer
KillSignal=SIGTERM
TimeoutStopSec=40s
Restart=on-failure
```

应用文件位于 `/var/lib/bt-sniffer/logs/`。`journalctl -u bt-sniffer -o cat` 查看 stderr 文本；journald 的限流、容量、留存和下游丢弃独立管理。部署多个进程时，分别提供独立工作目录、状态目录及监听端口，不使用此示例共写 logs/。

## 分类诊断与运行摘要

聚合每分钟输出累计和区间值，事件 schema_version 见[版本表](#日志契约版本-3)。文件 JSON 顶层与每条文本末尾附加同一 `run_id`，涵盖数据库线程与退出事件；启动 `application_start` 记录包版本和有效配置。版本不是源码指纹，基准报告另存源码清单与工具链。

- `peer_diagnostic`：固定 stage/source/family/result/deadline 维度，记录 count、sum_ms、p50/p95/p99 桶上界与 overflow。标准握手在协议名和目标 hash 校验后完成，扩展协商在可以请求分片时完成；仍共用原 5 秒绝对期限。EOF/reset/其他 I/O、协议、目标 hash/内容 hash、不支持扩展、限额、拒绝、超时及取消按类型分类，不解析中文文案。deadline=Stage 表示连接、共用握手或分片期限，Peer 表示单 peer 总期限，Task 表示 fetch 或整轮任务总期限；取消使用 None，超时不会覆盖实际 stage。
- `attempt_class_diagnostic`：领取时固定 Hint/Recent/Retry/History 类别，DueWait 表示本轮到期等待，DiscoveryAge 表示首次发现年龄，Execution 表示网络执行耗时。Downloaded 尚不代表存储提交成功；提交看 Applied 计数与 MetadataCommitted。任务 Diagnostic 桶包含 12、18、24 小时，网络 Network 桶上界为 180 秒。
- `collector_summary`：区间 metadata/claims/tcp_attempts、实际 running_workers、due_count、recent_active 和三个暂停原因。关闭输出剩余区间，final_snapshot=true；关闭摘要不承诺额外数据库快照。

阶段耗时诊断只输出已观察的固定类别组合，不保存 hash/IP 标签；Bencode 说明使用下述独立限量样本。成功、失败、取消分别聚合；peer 阶段统计不能和只保留最后一次失败的任务类别直接相除。任务 Diagnostic 分位数超过 24 小时上界时输出对应的 `p50/p95/p99_exceeds_ms=86400000`，不能解释为零。

首试缓冲使用 `admission_policy_version=2`。启动及 `admission_status` 的 `backpressure_basis` 在 sample/fetch freshness 下为 `first_attempt_waiting`，其余为 `capacity`。这只是策略版本，不是数据库 schema 或源码版本。

- `peer_failure_detail`：每个实际失败 peer 一次，按 stage/source/family/reason 输出 total/interval 的 count。Protocol 原因由 `WireErrorKind` 枚举产生，Resource 由资源枚举产生；I/O 子类为 ConnectionRefused、NetworkUnreachable、HostUnreachable、PermissionDenied、OtherIo、Eof、Reset、Timeout。其余为 Unsupported、Rejected、HashMismatch。`peer_diagnostic` 提供结果大类；成功和取消不增加失败细分，外层超时保留当前阶段。错误说明不是解析依据，不保留原始 errno、报文或地址标签。
- `sampler_diagnostic`：每分钟按地址族输出 running、collector_paused、candidates、in_flight、successful、failed、unsupported、pause、storage_error。成功/失败/不支持是采样器本次启动以来累计值，其余为快照。collector_paused 与内部 pause 独立；Storage 既可能等待存储确认，也可能故障，须同时看 storage_error。
- `admission_status`：总 active、近期活跃量 recent_active、first_attempt_waiting（Q）、buffer_limit（B）、high/low、策略版本及 basis。B/high/low 只在 freshness 下控制首试缓冲，capacity 下仍输出计算值供配置对照。
- `admission_backfill`：total/interval 的 recent_scanned/recent_inserted/history_scanned/history_inserted，仅在事务提交后累计；候选可重复扫描。每分钟及正常关闭尾段输出，区间取走清零。
- `admission_deferral`：total_capacity、first_attempt_buffer、history_reserve 的 unit=backfill_call，统计因对应额度直接跳过的一次补建调用；即使没有待补建 hash，也可能计数，不能解释为拒绝 hash 数。sample_waiting_backfill 的 unit=hash_observation，统计 freshness 批次提交后仍无任务且无 metadata 的观察次数，同批重复或跨批重复均可重复计数。这些事件用于说明接纳停顿，不执行每秒全库待补建计数扫描。

`collector_summary.recent_active` 表示近期活跃总量，`backlog_paused` 继续表示当前 freshness 策略暂停；判断具体依据须结合策略版本与 basis。关闭阶段保留 collector 摘要、补建/延后尾段及 peer/task 区间统计，不额外查询接纳 gauge。

## Bencode 样本与领取历史诊断

Bencode 样本与 peer 失败细分当前为 `schema_version=2`，其余本节事件为 1；接纳策略仍为版本 2。固定 `WireErrorKind` 决定程序行为及聚合，`WireError.detail` 只供诊断；例如 `peer_failure_detail.reason=Protocol(InvalidDictionary)` 表示固定的协议错误类别。

| event | 字段与口径 |
| --- | --- |
| `bencode_error_sample` | `stage/source/family/reason` 为已有固定维度；`reason` 为 WireErrorKind，`detail` 为 Bendy 底层说明，`truncated` 明确标记截断。无说明的本地校验错误不产生样本 |
| `bencode_sample_summary` | `scope=total/interval`、`emitted/suppressed`，正常关闭补齐 `final_snapshot=true` 的累计和尾段；每次输出后只清空区间计数 |
| `attempt_diagnostic` | `attempt_kind/failed_attempts_before/timing/result/reason`，`count/sum_ms` 和Diagnostic 固定桶分位数、溢出字段 |
| `attempt_commits` | `attempt_kind/failed_attempts_before/committed`，仅 `complete_job` 返回 Applied 才计数；无提交的组合可缺省 |
| `attempt_summary` | 每种 First/Repeat 的 `claims/executions/downloaded/committed/execution_sum_ms/execution_ms_per_commit`，包含零值类别 |
| `first_attempt_backlog` | 每分钟 `waiting/older_than_30m/oldest_discovery_age_ms/oldest_due_wait_ms/not_due` 只读快照；空队列的两个最大等待值缺省 |

Bencode 说明在转换错误时有界保存：最多 256 个 Unicode 字符，换行及控制字符转义，转义后的字符也占预算，不保留半段转义。不附加原始报文、metadata、hash 或地址。每个 Diagnostics 实例拥有一个共享固定窗口，自首次有说明的错误起算 60 秒，所有 worker 共用最多 8 条样本；额度不足只累计 suppressed。输出统计不会重置限额，无后台采样任务。先申请额度，获准后才格式化日志；emitted 表示获准并到达日志调用点的样本数，仍可能被 RUST_LOG 过滤或 lossy writer 丢弃，不保证输出或落盘。样本是限量观察，错误频数必须使用 `peer_failure_detail`，不能用 emitted 代替。

Bendy 0.6.1 没有公开错误 kind 访问器，不维护依赖补丁，不从说明字符串判断程序类别。`UnsortedKeys` 的说明为 `Keys were not sorted`，同时可能表示键乱序或重复键；单靠底层说明仍不能区分这两种情况；下述独立结构检查补充键序证据，不改变 Bendy 的访问限制。文本只供人读，不能作为分类键或稳定枚举协议。

First 表示领取前 generation=0，Repeat 表示此前已领取；带提示的再次领取、崩溃恢复及休眠再激活都属于 Repeat，表示领取历史而非提示存在性；调度版本 2 中 Hint/Recent/History 都属于 First，Retry 都属于 Repeat。generation 用于领取有效性，不能当精确网络尝试次数。`failed_attempts_before` 是领取事务读出的远端失败次数快照（0..6），不会随任务执行改变；本地延期、取消、恢复不递增，休眠再激活时清零，因此 Repeat 的失败次数可以为 0。

DueWait 为本次领取距到期时间，DiscoveryAge 为领取距首次发现时间，均以 Observed 记录；DueWait 的 count 就是该组合的领取数。Execution 在 worker 结束时记录一次，包含网络及资源等待，单位毫秒，不是 CPU 时间。结果区分 Downloaded、RemoteFailure、LocalDeferral、ControlFailure、Cancelled；RemoteFailure 的 reason 沿用 `no_peers/peer_io/peer_timeout/protocol/hash_mismatch/unsupported/receive_limit/rejected/metadata_unavailable/task_timeout`，未知类别归 `other`。它只表示整轮最终结果，不能代替期间所有 peer 的失败频数。

Downloaded 表示执行得到了已验证内容，Committed 表示完成事务已确认 Applied；Stale、写入错误、取消不计提交成功。执行计时不包括之后的保存事务。若存储通道未返回提交确认，即使最终数据库存在记录，也不能补猜本次内存提交计数；报告应保留差异并用最终数据库核对。

任务聚合和提交聚合都有 total/interval、final_snapshot。领取、执行结束和提交可能跨越输出区间，Execution 全额归入结束所在区间，不按分钟切分；区间数不能直接视为同批领取的成功率。报告优先使用最后的累计值：执行秒数=`execution_sum_ms/1000`，每次提交执行秒数=`execution_sum_ms/committed/1000`；committed=0 时为缺失值，不能填 0。日志 `execution_ms_per_commit` 为整数毫秒商。这个成本包括该类全部已结束执行（失败及取消），不只成功任务。

`first_attempt_backlog` 从已接纳的 fetch_jobs 索引出发，筛选 generation=0 且 pending/retry_wait，包含有提示、未到期以及超过近期窗口的任务。`older_than_30m` 使用严格大于 30 分钟，最老年龄从首次发现起算（未来时间按 0）。Q 使用近期窗口及合法提示筛选；该快照不参与背压、不扫描全库未接纳 hash。关闭时不在清理期限内新增查询，最末状态由退出后的只读数据库复核补足。操作和报告要求见 [诊断复测](diagnostics-validation.md)。

## 键序、领取与连接历史诊断

本节诊断用于观察，不参与调度或协议接纳决策；实际行为见[调度与扩展握手策略版本](#调度与扩展握手策略版本)。

### 键序检查

扩展握手被原解析器以 InvalidDictionary 拒绝后，独立检查当前输入，向 `bencode_error_sample` 附加 `unsorted_keys`、`duplicate_keys`、`inspection_status`。其他错误及 metadata 字典校验不附加这些字段。检查只遍历容器并借用原子，不构建语法树，不排序、重编码或重新接纳报文。每个字典单独判重，字节串正文不递归解析，原子语法仍由 Bendy 校验。

`inspection_status` 为 Complete、Malformed、DepthLimit 或 SizeLimit；输入最多 4096 字节且服从原握手上限，容器深度最多 64 且服从原深度配置。两个布尔值表示已经观察到的事实，检查不完整时 false 不能证明问题不存在。逆序与重复可能同时为 true，例如非相邻重复；不同字典的同名键不算重复。字段不含键内容、hash、地址或报文。

这些字段随样本共用每 60 秒最多 8 条的额度，不产生额外样本事件。Protocol 分类及 suppressed/emitted 口径见上文样本说明。独立检查结论不能被用于协议接纳或错误恢复决策。

### 领取类别与领取历史联合统计

`attempt_class_diagnostic` 包含固定 `claim_class`，其余维度及字段与 `attempt_diagnostic` 一致；`attempt_class_commits` 的维度为 attempt_kind、claim_class、failed_attempts_before，committed 只统计 Applied。类别在领取事务中固定在 Job 内，提示过期、恢复及保存结果时都不重新分类。

两个事件均输出 scope=total/interval、final_snapshot，schema_version=1。按 claim_class 求和必须回到对应 Attempt 聚合；`attempt_diagnostic` 不包含 claim_class，不按该维度拆成多条相同键记录。观察成本时同时看 DueWait 的 count、Execution 的 count/sum_ms 和实际提交，不把领取份额当成执行时间份额；零提交成本为缺失值。跨区间归属继续按领取、执行结束、提交各自发生时刻计算。

### 短期端点连接历史

所有 worker 共用 Diagnostics 内最多 4096 个完整 SocketAddr 条目，保存最近已完成 TCP 建连的结果及完成时刻，TTL 为 300 秒。不同端口及地址族独立。访问及输出快照时移除过期项，读取不续期；容量满时保留未过期项，不保存新的端点结果，累计 capacity_dropped。已存在条目可以更新；无后台清理任务。

`connect_history_diagnostic` 的固定维度为 attempt_kind（First/Repeat/Unknown）、source、family、history、result、可选 reason、deadline；数值为 count、sum_ms、固定桶分位数和 overflow。Unknown 表示调用者没有领取上下文；history 为 NoHistory、Success、Timeout、ConnectionRefused、Unreachable、OtherIo。本次 result/deadline 沿用 peer_diagnostic 类型，reason 在已记录具体错误时附加。

历史在开始连接时冻结；Connect guard 结束时只记录一次。Success 仅指 TCP 建连成功，后续握手失败不将历史改为连接失败；真实连接失败更新历史，取消及外层 peer/task 超时不污染历史。Stage 连接超时才写入 Timeout。查询历史不跳过、推迟、取消连接，不触发失败冷却；地址不输出到端点历史事件、不写数据库，也不作为聚合维度。

`connect_history_summary` 输出 entries 当前条目数，以及 expired、capacity_dropped 的累计和区间值；两个新事件均为 schema_version=1，正常关闭补齐 final_snapshot=true 的累计及尾段。entries 是 gauge，不能把 total/interval 的值相加。NoHistory 可能来自首次观察、过期或容量丢弃，不等于该端点从未出现；capacity_dropped 非零时历史覆盖不完整。端点历史跨 hash 共享，仅说明端点连接结果，不证明它能提供当前 metadata。

### 发现年龄和到期等待

`first_attempt_backlog.oldest_due_wait_ms` 为未首试积压筛选集合的 max(0, now−min(due_at))，`not_due` 为 due_at>now 的任务数。非空集合全部未到期时最大到期等待为 0；空集合的两个最大值均缺失。

最老发现年龄与最老到期等待可能来自不同任务，不能相减推算接纳前等待；due_at 不是通用的创建时间。没有精确接纳时间字段，不宣称能精确拆分接纳前耗时。两个字段在同一快照查询中计算，不扫描未接纳 hash，不参与 Q 和背压；关闭期限内不增加查询。

## 调度与扩展握手策略版本

`application_start` 和 `admission_status` 输出 `scheduling_policy_version=2`、`extension_handshake_policy_version=2`、`admission_policy_version=2`。策略版本与事件 schema_version 分别定义行为和日志字段，不能相互替代。

调度版本 2 的 Hint 仅包含带合法未过期提示的 First，Recent/History 为其余 First，Retry 包含全部 Repeat（含提示）。轮转为 Hint、Recent、Retry、Hint、History、Recent、Retry、Hint；类空时按 Hint、Recent、History、Retry 借用。预留领取机会 First/Repeat=3∶1，不保证执行时间比例。跨版本分类比较见[迁移说明](#从日志契约-1-迁移到-2)。

领取事务固定 `had_valid_hint`，提示过期或刷新不改变该次领取标签。`ClaimsWithHint/ClaimsWithoutHint` 继续按有／无有效提示计数，不改成首试／重复计数。`attempt_hint_summary` 的维度是 attempt_kind、had_valid_hint，数值为 claims、executions、execution_sum_ms、downloaded、committed；求和回到 First/Repeat 汇总，`attempt_summary` 不含提示维度。事件使用 schema_version=1、scope=total/interval、final_snapshot；领取、结束和 Applied 分别在发生时刻记入区间，取消只结束一次计时。committed=0 时每提交成本为缺失值。调度版本 2 中分析带提示 Repeat 应使用此汇总，而不是查询 Hint 类别。

扩展握手版本 2 优先严格解析，只有 InvalidDictionary 才尝试独立有界规范化并再次完整严格校验。兼容接纳不计 Protocol 失败，也不输出 bencode_error_sample；最终拒绝保留原严格错误及检查说明，在失败入口采样一次，共用 8 条／60 秒预算。跨版本 InvalidDictionary 频数差异不能直接解释为下载收益。

`extension_compatibility_summary` 使用 schema_version=1、scope=total/interval、final_snapshot，固定数值如下：

| 字段 | 含义 |
| --- | --- |
| attempted_frames | 严格 InvalidDictionary 后进入兼容尝试的帧数，含预算超限等拒绝 |
| accepted_frames | 规范化及完整严格字段复验通过的帧数；不表示会话协商已完成 |
| rejected_frames | 兼容仍不能接纳的帧数；attempted_frames=accepted_frames+rejected_frames |
| sessions | 至少接纳一帧兼容握手的 peer 会话数；重复增量更新不重复计数 |
| downloaded | 成功来源会话使用过兼容，且 metadata 已通过完整校验的数量 |
| committed | 上述成功结果在正常运行或退出保存中获得 Applied 确认的数量 |

失败、取消会话不增加 downloaded；先前失败 peer 的兼容标记不会传给后续 peer。Stale、写入错误或缺少确认不增加 committed。计数随不同阶段发生时刻归属区间，累计比较才适合计算总体收益；事件不输出逐 hash、地址、键内容或报文。

## 日志契约版本 3

`application_start.log_contract_version=3` 标识整体日志契约；数据库 schema 为 2，三个业务策略版本为 2。历史报告仅描述其对应运行；消费者升级见[迁移说明](#从日志契约-1-迁移到-2)。

所有生产事件使用真实 Rust 模块路径作为 target，例如 `bt_sniffer::app`、`bt_sniffer::collection::worker`、`bt_sniffer::collection::diagnostics`、`bt_sniffer::dht::traffic`。按 `bt_sniffer` 前缀过滤仍然适用；定位事件以 event 字段为准。DHT 与采集的 `duration_histogram` 字段一致，target 各属其模块。

| 事件 | schema_version | 当前内容 |
| --- | --- | --- |
| application_start | 3 | log_contract_version、log_filter 与有效配置的具名字段 |
| session_fault | 2 | kind、fatal、action、phase 和底层 error |
| logging_queue | 1 | 两端投递丢弃累计、增量及最终观察标识 |
| collector_counter | 2 | ClaimsWithHint、ClaimsWithoutHint、PeerAttempts、MetadataCommitted 等具名计数 |
| peer_diagnostic、peer_failure_detail、bencode_error_sample | 2 | 物理阶段与独立期限范围 |
| peer_handshake_diagnostic | 1 | 完整握手聚合 |
| 其他事件 | 1 | 字段含义见各事件说明；target 使用模块路径 |

启动配置字段均读取实际控制者：`concurrency`、`max_peer_attempts` 为数量；`fetch_timeout_ms`、`peer_timeout_ms`、`connect_timeout_ms`、`handshake_timeout_ms`、`piece_timeout_ms` 为毫秒；`max_metadata_size_bytes`、`max_frame_size_bytes`、`max_header_size_bytes`、`max_received_bytes` 为字节；其余为 `request_window`、`max_depth`、`max_received_frames` 和固定文本 `address_policy`。

任务耗时由 `attempt_diagnostic` 和 `attempt_class_diagnostic` 提供，实际提交由 `attempt_commits` 和 `attempt_class_commits` 提供；`attempt_summary` 与 `attempt_hint_summary` 保留实用摘要。各视图不是互斥数据，不能跨视图相加。远端失败与本地延期分别记录。

任务按完整领取维度只记录一份累计／区间耗时与独立 Applied 计数。成功领取后只读取一次观察时间，计算 DueWait 和 DiscoveryAge；Execution 在 worker future 首次被 poll、创建 guard 时开始，到执行结果返回或 future 丢弃时结束，不含 spawn 前等待及数据库提交。正常结束、取消和中止最多记录一次。桶采用 Diagnostic，省略维度后先合并桶再计算分位数。

物理阶段为 Connect、StandardHandshake、ExtensionHandshake、Transfer、Verify。deadline 独立使用 None、Stage、Peer、Task：Task 截断统一记录 Timeout/Task，普通取消为 Cancelled/None。任务最终重试判断保持原规则，不能从单 peer 阶段结果推算最终任务类别；外层超时和普通取消不更新端点失败历史。

`peer_handshake_diagnostic` 从进入 StandardHandshake 起计时，进入 Transfer 时计 Success，或在握手失败／取消／超时时计对应结果；增量握手不续期也不重置计时。维度为 source、family、result、deadline，字段为 scope、final_snapshot、count、sum_ms、p50/p95/p99 的 upper_bound_ms 或 exceeds_ms、overflow。使用 Network 桶，超出 180 秒显式记录溢出；Connect 失败不生成握手样本。两个握手阶段的最大值或分位数不能相加替代该统计。

完整握手在结束时归属区间，累计、分钟区间与关闭尾段共同输出，取走区间不会重置进行中的握手起点。样本、端点历史和兼容收益分别遵循对应小节的预算、清理及归属规则。

## 从日志契约 1 迁移到 2

版本 2 不提供历史 target 别名、退役事件双写或旧配置文本包装。按实际模块路径过滤 target，以 event 定位业务事件；启动配置读取具名字段，不解析 metadata_config 的 Debug 文本。

| 版本 1 表达 | 版本 2 读取方式 |
| --- | --- |
| ClaimsFresh / ClaimsOther | ClaimsWithHint / ClaimsWithoutHint；表示有／无有效提示 |
| Connections / MetadataCount | PeerAttempts / MetadataCommitted；区分尝试、下载与 Applied 提交 |
| task_diagnostic、ClaimWait / Task 直方图 | attempt_diagnostic、attempt_class_diagnostic；执行结果区分远端失败和本地延期 |
| peer_phase、PeerConnect / PeerTransfer / PeerVerify 直方图 | peer_diagnostic 的物理阶段与独立 deadline |
| PeerHandshake 直方图 | peer_handshake_diagnostic 的连续完整握手耗时 |

旧四阶段结果不能直接当作物理阶段统计。多个 attempt 视图不是互斥数据，不能相加；历史分位数也不能相加构造完整握手分位数。

历史 DHT 文本数组按采集、控制、采样、反向验证排列；queue_finished 的顺序为进入发送、取消、队列超时、本地拒绝，blocked 为类别配额、目的 IP 配额、发送字节、IP 表容量。当前日志用具名 class 与结束/等待原因字段表达；不要将数组位置当作当前文本字段契约。

业务策略版本须独立核对：早期日志没有调度及扩展握手策略字段时按对应版本 1 解读；接纳版本 1 的背压依据为 recent_active，版本 2 的 freshness 使用 first_attempt_waiting。调度版本 1 的 Hint 可包含 Repeat，版本 2 的 Hint 仅含 First，因此同名 Hint/Retry 不能跨版本当作同一人群比较。版本 1 中的 Hint × Repeat 可用于历史成本分析；版本 2 使用 attempt_hint_summary 观察带提示 Repeat。

版本 1 到 2 曾由 JSON 改为文本；升级当前版本另见下述迁移说明。历史编码不能混读，旧 --log-format 参数仍不支持。

## 从日志契约 2 迁移到 3

终端仍为文本，文件改为 `.jsonl`，不再将文本正则用于文件分析。
按顶层 run_id 选择一次运行，再读取 fields.event、fields.schema_version 和业务字段；
启动 log_contract_version 为 3、application_start schema_version 为 3，业务策略及数据库 schema 仍独立维护。
除启动和会话故障事件外，既有聚合事件版本及口径不变。

RUST_LOG 非空时完整替换默认规则；迁移已有启动环境时核对该变量，验收显式使用 warn,bt_sniffer=info。
旧 .log 与历史报告不重写或删除；外部解析器和部署配额由部署者同步核对。
消费者只处理以换行结束的完整 JSON 行，保存未完成末行等待后续字节；完整坏行必须报告，不静默忽略。
同一事件在 stderr 和文件各一份，统计仅选择文件一份。最终摘要缺失或出现投递丢弃时明确标注证据边界。
