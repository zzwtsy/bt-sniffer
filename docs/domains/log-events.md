# 日志事件参考

本页维护事件版本、字段类型和统计解释。源码入口：[应用事件](../../src/app/mod.rs)、[采集聚合](../../src/collection/diagnostics/mod.rs)、[计数与耗时](../../src/collection/diagnostics/metrics.rs)、[DHT 聚合](../../src/dht/traffic/mod.rs)。验证入口：`app::logging::event_tests`、`collection::diagnostics::tests`；消费者入口：[diagnostics.py](../../scripts/diagnostics.py)。输出保证见[日志机制](logging.md)。

## 公共字段与版本

JSON 外层包含格式层生成的 `run_id` 和事件 `fields`，消费者先按 run_id 隔离运行。`fields.event` 为字符串，`schema_version` 为整数，各事件独立演进；启动 `log_contract_version` 不替代事件自身版本。message 面向人类，不用于分类；Rust Debug 枚举输出是字符串，计数、毫秒、字节保持数值，布尔保持布尔。

`scope=total` 为进程内累计，`interval` 为上次快照后取走的区间；没有 scope 的快照不能作为区间计数。`final_snapshot` 为布尔，但只在生产者声明该字段时存在，不能假定所有事件都有。可选数值或标签没有值时可能不出现在 fields，消费者必须处理缺失，不以 0 替代未知。

## 启动、状态与数据库

| 事件 / 版本 | 字段与解释 |
| --- | --- |
| application_start / 3 | `version`、`log_filter`、`address_policy`、`backpressure`、`backpressure_basis` 为字符串；`sample`、`fetch` 为布尔；策略版本、concurrency、max_active、max_peer_attempts、request_window、max_depth、max_received_frames 为整数；所有 `*_timeout_ms` 为毫秒，`*_bytes` 及 `max_*_size_bytes` 为字节；DHT rate 为包/秒，upload 为字节/秒。记录有效配置，不等于已绑定或可达 |
| node_status / 1 | address、family、node_id、phase 为字符串；good、questionable、pending、recovery_queued、recovery_active 为当前数量；sampling 为布尔，samples_ok 为累计成功采样数，不证明公网入站可达 |
| sampler_diagnostic / 1 | 节点采样快照：running、collector_paused、storage_error 为布尔；candidates、in_flight 为当前占用；successful、failed、unsupported 为累计数；pause 为原因字符串 |
| database_jobs / 1 | pending、running、retry_wait、dormant、succeeded、metadata_count 为当前数据库数量，metadata_bytes 为原始 info 字节总量，final_snapshot 标识关闭快照 |
| first_attempt_backlog / 1 | waiting 为已接纳但 generation=0 且 pending/retry_wait 的数量，包含提示及未到期任务；older_than_30m、not_due 为子集数量；oldest_discovery_age_ms 与 oldest_due_wait_ms 为可选毫秒，可能来自不同任务 |
| session_fault / 2 | phase、kind、action、error 为字符串，fatal 为布尔；action 为 shutdown 或 pause_collection，消费者不能靠中文 message 推断可恢复性 |
| application_shutdown_started / 1、session_shutdown / 1 | 开始关闭不等于完成；session_shutdown 的 success 为布尔、error_count 为整数；未收到最终事件也不能仅凭日志断定进程仍运行 |
| logging_queue / 1 | sink 为 file/stderr；queue_capacity、dropped_total、dropped_since_last_report 为整数；final_snapshot 为布尔。不是队列长度，也不覆盖快照之后的丢弃 |

启动字段的数值默认与约束见[运行](../operations/running.md)，不在此重复参数表。异常启动事件包括 runtime_start_failed、application_failed、ipv6_listen_unavailable，版本均为 1；它们说明失败或降级，不能纳入采集成功统计。

## DHT 统计

以下事件版本均为 1，数值是整数；字段名含 bytes 的单位为 UDP payload 字节。

| 事件 | 字段与口径 |
| --- | --- |
| dht_occupancy | tracked_ips、verification_queued 为当前占用 |
| dht_traffic | scope；inbound_packets、inbound_bytes、reply_packets、reply_bytes、limited_drops、queue_timeouts、validated_v4、validated_v6；入站、回复、验证分别计数 |
| verification_admission | scope；admitted、duplicate、cooldown、capacity、ip_table、succeeded、failed；接纳与最终验证结果可能跨区间 |
| dht_class | scope、class；packets、bytes 是发送统计；queued、dequeued_to_send、queued_cancelled、queue_timeouts、local_rejected、inflight_cancelled 区分排队和在途结果 |
| dht_wait_reason | scope、class、reason 为字符串，count 为次数；reason 为 class_quota、destination_ip_quota、upload_bytes 或 ip_table_capacity，可重复观察同一查询 |
| dht_queue_occupancy | class、current；当前待发数量，不能与累计 queued 混用 |

class 为 collector/control/sampling/verification。流量约束与突发解释见 [DHT](dht.md)。

## 采集计数与耗时

`collector_counter` 版本 2，字段为 scope、counter 字符串和 value 整数。16 个 counter 的定义如下：

| counter | value 含义 |
| --- | --- |
| ClaimsWithHint / ClaimsWithoutHint | 领取时有无有效提示，均包含 First 和 Repeat |
| Lookups / LookupsFinished | 双栈逻辑查找开始 / 退出，退出包含取消 |
| RpcSent / LookupWithPeers | 查找退出时汇总的实际 RPC / 找到至少一个 peer 的查找 |
| PeerAttempts / PeerFailures | 筛选后尝试 peer / 尝试返回错误；直接丢弃 future 不增加 PeerFailures |
| MetadataCommitted / MetadataBytes | Applied 后的提交次数 / 提交 info 字节，非网络接收量 |
| RemoteFailures / LocalDeferrals | 已提交的远端失败重试 / 本地延期轮数 |
| LookupFirstPeer / LookupCancelledSuccess | 首次发现 peer / metadata 成功后主动结束查找 |
| SamplingResumes / AnnouncesAccepted | 从总体暂停恢复 / 确认接纳的 announce，包含刷新 |

`duration_histogram` 版本 1，scope、timing、class 为字符串，count、overflow 为整数。`p50/p95/p99_upper_bound_ms` 是可选桶上界，`p50/p95/p99_exceeds_ms` 是可选溢出下界，均为毫秒，不能标成精确分位值。采集 timing 为 Lookup、TcpWait、FirstPeer；TcpWait 不含建连。DHT 使用 dht_queue 与请求 class。计时 guard 在取消或提前返回时也记录，因此 count 不代表成功数。

[领取诊断](../../src/collection/diagnostics/attempts.rs)的事件版本均为 1：

| 事件 | 维度与数值 |
| --- | --- |
| attempt_diagnostic / attempt_class_diagnostic | scope、final_snapshot、attempt_kind、failed_attempts_before、timing、result、reason；class 版本额外 claim_class。count、sum_ms、overflow 及三组分位字段描述耗时 |
| attempt_commits / attempt_class_commits | 同一领取历史维度；committed 只在 Applied 后增加，class 版本额外 claim_class |
| attempt_hint_summary | attempt_kind、had_valid_hint；claims、executions、execution_sum_ms、downloaded、committed；提示维度独立于调度类别 |
| attempt_summary | attempt_kind；claims、executions、downloaded、committed、execution_sum_ms；execution_ms_per_commit 为可选整数毫秒，用全部已结束执行耗时除以提交数 |

First/Repeat 是领取历史，failed_attempts_before 是此前远端失败轮数，两者不等价。领取、结束和提交可以跨区间，不能用同一个 interval 的任意分子分母直接算成功率。

## peer、兼容与接纳诊断

| 事件 / 版本 | 字段及边界 |
| --- | --- |
| peer_diagnostic / 2 | scope、stage、source、family、result、deadline；count、sum_ms、overflow、三组分位字段；区分连接、握手、metadata 阶段与外层期限 |
| peer_failure_detail / 2 | scope、stage、source、family、reason、count；稳定类别统计，不解析底层错误文字 |
| peer_handshake_diagnostic / 1 | scope、final_snapshot、source、family、result、deadline 及耗时分布；标准握手到扩展协商的完整阶段 |
| bencode_error_sample / 2 | stage、source、family、reason、detail、truncated；unsorted_keys、duplicate_keys、inspection_status 为可选检查结果；detail 只供人读 |
| bencode_sample_summary / 1 | scope、final_snapshot、emitted、suppressed；所有 worker 共用 60 秒最多 8 条说明样本，窗口不随日志区间重置；emitted 不保证 writer 已保存 |
| extension_compatibility_summary / 1 | scope、final_snapshot；attempted_frames、accepted_frames、rejected_frames、sessions、downloaded、committed；帧、会话、下载和事务收益分开统计 |
| connect_history_diagnostic / 1 | scope、final_snapshot、attempt_kind、source、family、history、result、可选 reason、deadline 及耗时分布 |
| connect_history_summary / 1 | scope、final_snapshot、entries、expired、capacity_dropped；端点历史容量 4096、TTL 300 秒，NoHistory 不表示从未连接，不用于跳过连接 |
| admission_deferral / 1 | scope、final_snapshot、reason、count、unit；可能重复观察同一对象，total_capacity、first_attempt_buffer、history_reserve 的 unit 为 backfill_call，sample_waiting_backfill 为 hash_observation；不能当去重 hash 数 |
| admission_backfill / 1 | scope、final_snapshot、recent_scanned、recent_inserted、history_scanned、history_inserted；已提交回填事务统计，扫描可重复，未插入不等于额度拒绝 |

耗时分布沿用 count、sum_ms、overflow 和三组分位字段；计数是整数、标记是布尔、分类是字符串。精确枚举取值由 [observations](../../src/collection/diagnostics/observations.rs) 与 [diagnostics](../../src/collection/diagnostics/mod.rs) 中类型定义维护。

## 采集运行快照与调试事件

以下事件版本均为 1。当前占用、累计计数和区间结果混合在不同字段中，不能仅按事件名统一求和。

| 事件 | 字段与含义 |
| --- | --- |
| collector_status | due_count、fresh_due、active、connections、admitted_tasks_database 为当前数量；oldest_wait_ms 为毫秒；sample_hashes 为成功保存的采样观察累计数，含重复；succeeded、failed、announces、announce_dropped 为累计数；state_bytes 为状态占用；capacity_paused、backlog_paused、storage_paused 为布尔 |
| collector_summary | scope=interval，final_snapshot；metadata、claims、tcp_attempts 是区间数，running_workers 是当前任务数；运行期额外有 due_count、recent_active 和三种 paused 标记，关闭尾段不提供这些额外字段 |
| collector_failure | scope=total、category 字符串、count；任务轮次失败类别 |
| admission_status | active、recent_active、first_attempt_waiting、buffer_limit、high、low 为数量；三个 policy_version 为整数，backpressure_basis 为字符串；first_attempt_waiting 的筛选不同于 first_attempt_backlog |
| sampling_backpressure | mode 字符串；capacity、backlog、storage 布尔；paused_ms 是累计暂停毫秒，resumes 是总体恢复次数 |
| storage_capacity_paused | phase、action 字符串，state_bytes、limit_bytes 为字节；暂停并保留数据，重开后重新检查 |
| sample_batch_save_started | debug；phase、responder、target、received_at 为说明/身份字符串；observed_at_ms 为 UTC 毫秒，interval_secs 为秒；num、count、confirmed_offset 为数量，开始保存不等于完整提交 |
| metadata_committed | debug；phase、hash、source、peer_id 为字符串，bytes 为原始 info 长度；在事务提交后发出，过滤可使其不可见 |
| peer_fetch_failed | debug；peer、error、category 为字符串，stage 为可选阶段；错误文本只供诊断 |

引导事件位于 [bootstrap](../../src/app/bootstrap/mod.rs)：bootstrap_connected、bootstrap_retry_scheduled、bootstrap_dns_failed、bootstrap_dns_timeout、bootstrap_query_failed，版本均为 1。phase 区分 dns、query 等阶段，seed/error 为字符串，timeout_ms、retry_after_ms 为整数毫秒；DNS 成功和邻居响应不保证公网入站可达。

## 消费者联动

修改事件先定位全部生产者和测试，再检查 scripts/diagnostics.py、scripts/evidence.py、src/acceptance 和 scripts/tests 的解析/证据契约。消费者按 event、schema_version 和 run_id 路由，不凭 message 或文件名猜测模式。加入字段须明确缺失语义；改名、类型或统计口径须同步消费者及本页，不能仅升级一个总版本号。

诊断工具保存的运行与数据库证据有自己的 schema，开发检查报告另有类型；三者不混用。离线事件类型测试证明格式和消费边界，不证明日志无丢失或公网吞吐。
