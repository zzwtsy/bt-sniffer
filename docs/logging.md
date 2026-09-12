# 日志配置与字段契约

日志统一输出 stderr。默认 `text`，可用 `--log-format json` 输出每行一个 JSON 对象；JSON 的业务字段保留在 `fields`，不展开到根部。时间使用 UTC，保留 level 和 target，不默认输出线程、文件和行号。

```bash
# 正常采集：默认 warn,bt_sniffer=info
./bt-sniffer --state-dir ./state --sample --fetch

# 文件或服务器：JSON，无 ANSI 颜色
RUST_LOG='warn,bt_sniffer=info' ./bt-sniffer --log-format json \
  --state-dir ./state --sample --fetch > run.jsonl 2>&1

# 临时诊断采集明细
NO_COLOR=1 RUST_LOG='warn,bt_sniffer=info,bt_sniffer::collector=debug,bt_sniffer::metadata=debug' \
  ./bt-sniffer --state-dir ./state --sample --fetch > debug.log 2>&1
```

未设置 `RUST_LOG` 才采用默认值；显式配置整体覆盖默认值，空字符串关闭日志。非法或非 Unicode 配置启动失败，配置错误通过 stderr 文本报告。`--help` 和 `--version` 不初始化日志线程或运行资源。文本只有 stderr 是终端且 `NO_COLOR` 未设置或为空时启用颜色；JSON 永远不输出颜色。

## 输出与关闭边界

`tracing-appender 0.2.5` 使用独立 writer 线程，队列容量 4,096 条、lossy 模式。格式化仍发生在调用线程；队列满时丢弃本次投递，不等待输出端腾出名额。条数不是字节硬上限，不应添加完整报文、metadata 或巨大对象日志。

`logging_queue` 每 60 秒和入口退出时报告 `dropped_total`、`dropped_interval`。它只统计队列投递失败，不检测全部底层 I/O 错误，也不包含报告事件自身之后发生的丢弃。此事件也遵守过滤和队列策略：关闭日志或队列持续饱和时，它不能保证可见。

main 持有 WorkerGuard，直到 runtime 收尾及最终错误输出后再释放。锁定版本 guard 尝试投递关闭标记最多等待 100 ms，再等待刷新交接最多 1,000 ms；这独立于现有会话 30 秒清理及 runtime DNS 等待上限，并非将日志纳入数据库事务，也不保证持久落盘。卡住的输出端不能凭 guard 返回判为排空成功。最终验收仍以独立报告、正常退出和数据库复核为依据。

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
| `logging_queue` | 队列投递失败累计和区间值 |

分位数输出 `p95_upper_bound_ms` 或 `p95_exceeds_ms` 等数值字段。无样本时两个字段都缺省；溢出时只有 exceeds，不能用 0 替代缺失或把超过上界当精确耗时。领取等待有限桶到 24 小时，网络阶段到 180 秒。

`dht_class.dequeued_to_send` 仅表示离开待发进入发送流程，实际 socket 发出量看 packets。开始数和结束数可能属于不同区间，不能直接相除。诊断日志中的地址、错误仍可能为字符串；本轮没有逐任务 span 或逐包事件。

## systemd 示例

以下路径需按部署位置调整，使用专用账号并提前创建可写状态目录：

```ini
[Service]
Type=simple
User=bt-sniffer
WorkingDirectory=/opt/bt-sniffer
Environment="RUST_LOG=warn,bt_sniffer=info"
ExecStart=/opt/bt-sniffer/bt-sniffer --state-dir /var/lib/bt-sniffer --sample --fetch --log-format json
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bt-sniffer
KillSignal=SIGTERM
TimeoutStopSec=40s
Restart=on-failure
```

`journalctl -u bt-sniffer -o cat` 可查看应用 JSON 行。journald 的限流、容量和留存独立管理，也可能丢弃事件；`logging_queue` 无法统计下游丢弃。本轮不内置文件轮转，不安装服务或修改服务器配置。
