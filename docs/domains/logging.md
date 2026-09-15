# 日志机制

负责过滤、输出格式、队列及 writer 生命周期；字段契约由[事件参考](log-events.md)维护。源码入口：[日志初始化](../../src/app/logging/mod.rs)、[格式](../../src/app/logging/format.rs)、[队列诊断](../../src/app/logging/diagnostics.rs)。验证入口：`app::logging::event_tests::json_events_preserve_types_filters_overflow_and_interval_reset`、`app::logging::event_tests::dht_and_collection_histogram_contracts_match`，以及同目录 output_tests、writer_tests。

## 过滤和输出

默认过滤为 `warn,bt_sniffer=info`。非空 RUST_LOG 完整替换默认值；空值使用默认；非法指令或非 Unicode 值在文件资源初始化前失败。调试过滤会改变诊断事件可见性，不能把被过滤解释为业务事件没有发生。

stderr 为文本，只有终端支持 ANSI；文件是工作目录 `logs/` 下 UTC 日轮转的 `bt-sniffer.*.jsonl`，最多保留 7 个文件。日志路径不跟随 state-dir，也不由 instance 隔离，不支持多进程共写同一工作目录日志。

两端各有容量 4096 的有界后台 writer，均为 lossy 模式。队列满会丢弃，两端不保证拥有完全相同的事件；业务不为等日志空间阻塞网络循环。run_id 由格式层添加，进程内一致；JSON 保留字段原生类型，不把数值统一转字符串。

## 能保证与不能保证的事

`logging_queue` 是投递丢弃的观察值，不是队列当前长度。final_snapshot 的快照发生在自身报告及后续刷新前，不能覆盖最后阶段所有丢弃。区间指标在生成快照时取走，过滤或丢弃不会回滚区间；日志文件不能当精确审计账本。

main 持有 guard 到业务关闭、runtime 收尾和最终诊断结束。guard 的有限等待不等于 fsync 持久落盘，也不能保证进程强杀时写完队列。业务成功以数据库事务或对应完成确认判断，不以“已输出日志”判断。

采用有界丢弃输出保护网络事件循环，代价是过载时观测不完整。如果确有必须可靠保存的业务证据，应设计独立持久化路径及其资源预算，而不是把所有日志改成无限缓冲；重访依据是明确的可靠性需求与实测丢弃情况。

## 流量日志的临界区

[traffic](../../src/dht/traffic/mod.rs) 在一次 Budget 锁持有期间清理过期状态、复制累计 Stats、取走区间 Stats，并读取 IP 数与各类队列占用；固定大小的 TrafficLogSnapshot 不复制 IP 明细。释放锁后才调用 tracing，subscriber 回调不会持有流量锁。快照之后产生的新计数属于下一区间，过滤或丢弃不恢复已取走计数。

这缩短了临界区，但同步格式化和 subscriber 仍占用调用线程；current_thread 事件循环没有因此免除日志开销。验证入口是 `dht::traffic::tests::log_releases_budget_lock_and_consumes_interval_once`，字段类型仍由事件契约测试验证。

## 修改时核对

输出机制变更核对过滤、两端写入、轮转、run_id、guard 生命周期；事件变更核对所有 tracing 生产者、事件类型测试、诊断工具和聚合字段。日志专项方法见 [bt-sniffer-logging](../../.agents/skills/bt-sniffer-logging/SKILL.md)。
