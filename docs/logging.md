# 固定日志输出与字段契约

日志同时写入 stderr 和进程工作目录下的 `logs/bt-sniffer.YYYY-MM-DD.log`，为业务 stdout 留出空间。两端固定文本，本程序 INFO 及以上、第三方依赖 WARN 及以上；时间使用 UTC，保留 level、target，不默认输出线程、文件和行号。

```bash
# 日志目录是当前工作目录的 logs/，不随 --state-dir 改变。
./bt-sniffer --state-dir ./state --sample --fetch
```

不再支持 `--log-format` 或生产 JSON 输出，不解析或校验 `RUST_LOG`、`NO_COLOR` 等日志环境变量，不加载 `.env`，不提供热更新。原 DEBUG 事件仍保留在源码，但固定过滤策略下不输出。文件禁用 ANSI；stderr 仅在连接终端时启用颜色。锁定依赖创建 fmt 层时会内部读取 NO_COLOR，本程序显式 `with_ansi(...)` 完全覆盖它，其值不会影响输出。

## 文件、部署与消费者

文件使用 `tracing-appender 0.2.5` 的 DAILY 轮转：按 UTC 自然日命名，跨日后的首次实际写入触发轮转，不在午夜额外启动任务或生成空文件。后台队列中的日志按实际写入时刻选择文件；跨午夜积压时，事件时间与文件日期可以不同。

最多保留 7 个匹配文件，不是严格保留 7 天，不保证恰好保留 7 个，也不限制单文件大小。启动和轮转时清理，锁定依赖在打开文件前为新文件预留名额，同日重启可能少保留一个。候选文件以 `bt-sniffer` 前缀和 `log` 后缀匹配，并非严格校验日期名；logs/ 应为本程序专用目录，不放置同样匹配的其他文件或归档。正常 I/O 条件下满足文件数量上限；删除失败会报告错误，不能保证上限。

- 工作目录不可写且没有预建可写 logs/ 时，启动失败。只读根文件系统部署需要挂载可写 logs/；不自动改用状态目录或其他路径。
- `--instance` 仍用于节点身份，状态目录锁只保护数据库，不能隔离日志。多进程必须使用不同工作目录和独立 logs/；即使 --state-dir 不同，也不支持共写相同日志文件。
- stderr 与文件各写一份，journald 的留存与应用文件相互独立，磁盘占用也分别计算。
- 旧启动脚本必须移除 --log-format。旧 JSON 行解析器需要迁移；文本不是原 JSON fields 对象的兼容编码。本次仓库检查未发现必须保持的外部解析器或共享日志部署，无法据此排除仓库外消费者。
- 内部测试继续使用 JSON subscriber 检查事件字段类型；独立验收 JSON 报告与生产日志格式无关，历史报告保留原样。

## 输出与关闭边界

初始化在 CLI 解析之后、runtime 创建之前完成。help/version 和 CLI 解析失败不创建日志资源；日志目录或文件创建失败时，stderr 报告路径、动作和底层错误，进程返回失败，不继续启动业务。subscriber 注册失败也返回初始化错误。

文件与 stderr 各有一个独立 writer 线程、4,096 条有界队列，均为 lossy 模式。慢文件不阻塞终端，慢终端不阻塞文件；队列满载丢弃本次投递，不等待输出端腾出名额。格式化仍发生在调用线程，队列条数不是字节硬上限，不应记录完整报文、metadata 或巨大对象。独立 logging_queue 定时/退出统计及其全局状态已移除；业务采集、协议和数据库指标继续保留。

运行中轮转创建失败由依赖向 stderr 报错并保留旧 writer；删除失败同样直接报告。普通后台写入 I/O 错误并非都能通过现有依赖对外观察，本实现不增加故障监督或自动降级，也不承诺无损日志。

main 持有两个 WorkerGuard，直到会话、runtime 收尾和最终错误事件发出之后。每个 guard 尝试投递关闭标记最多等待 100 ms，再等待刷新交接最多 1,000 ms；两个 guard 的等待可能累计。这独立于会话 30 秒清理和 runtime DNS 等待上限，不保证强杀、断电、I/O 故障时日志完整，也不等于持久落盘。验收仍以独立报告、正常退出和数据库复核为依据。

## 结构化聚合 v1

下表事件包含 `schema_version=1` 和稳定 `event` 标识。中文 message 供阅读，不作为解析键。旧 `?stats`、类别数组和 `?quantile` 字符串不再重复输出完整副本；原累计统计语义保留。

| event | 口径 |
| --- | --- |
| `collector_counter` | scope=total/interval；counter 名称沿用原枚举，value 为数值 |
| `peer_phase` | connect/handshake/transfer/verify 的 succeeded/failed/cancelled |
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

`dht_class.dequeued_to_send` 仅表示离开待发进入发送流程，实际 socket 发出量看 packets。开始数和结束数可能属于不同区间，不能直接相除。诊断日志中的地址、错误仍可能为字符串；本轮没有逐任务 span 或逐包事件。

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
