# metadata 采集

负责发现接纳、任务领取、peer 查找、原始 metadata 验证和结果提交。源码入口：[scheduler](../../src/collection/scheduler.rs)、[worker](../../src/collection/worker.rs)、[jobs](../../src/collection/jobs/mod.rs)、[peer](../../src/collection/peer/mod.rs)、[ingest](../../src/collection/ingest/mod.rs)。验证入口：`collection::jobs::tests::dedup_capacity_recovery_and_stale_generation`、`collection::peer::tests::final_verification_rejects_untrusted_metadata`、`collection::tests::recovery::completion_rollback_and_stale_success_are_atomic`。

## 发现不等于接纳

采样 hash 先持久化，任务接纳再受活跃任务数、状态空间及策略约束。announce 提示入口容量为 1024，满时尽力丢弃；采样 ingest 通过有界通道和保存确认提供背压。fetch 可以消费历史 hash 和合法 announce，不要求同时采样。

`jobs/admission.rs` 管理接纳与回填，`claim.rs` 决定到期领取，`hints.rs` 管理提示。领取类别 Hint、Recent、Retry、History 与 First/Repeat 领取历史是不同维度；有有效提示不能把重复任务变成首次领取。当前调度策略版本为 2，首次与重复领取预留机会为 3:1，具体候选不足时的回退由领取 SQL 决定，不是完成吞吐比例保证。

Recent 窗口为首次发现后 30 分钟，重复观察不重置 first_seen。每 hash 最多 8 条 peer 提示，TTL 30 分钟。休眠再激活要求满足新的观察与 24 小时延迟条件，不能靠同一旧提示无限重试。策略变更应核对 jobs 的调度、接纳和比较测试，保持统计分类可解释。

## 主动采样背压

capacity、backlog 和 storage 三个暂停原因取 OR，只控制主动采样，不阻止已接纳任务继续处理，也不关闭 announce 入口。活跃任务达到上限暂停，降到 80% 阈值以下恢复；存储暂停保持独立。

freshness 观察近期未首试集合 Q：不含 running、有效提示和 generation>0，但包含未到期首试任务。高水位为 min(max_active, concurrency × 4)，低水位为高水位整数除以 4；达到高水位暂停，持续满足低水位 30 秒恢复。它不同于包含所有已接纳未首试任务的 first_attempt_backlog。近期回填用游标推进，不能通过一直重扫历史前缀消耗全部预算。

状态占用达到配置上限减 64 MiB 的保护阈值时，取消正在推进的采集并暂停，保留数据，重启后重新检查。该机制不是删除旧数据腾空间，也不是磁盘文件硬配额。

## 任务状态与提交

| 当前情况 | 转换与保证 |
| --- | --- |
| 新接纳或恢复任务 | pending，等待到期领取 |
| 领取到期 pending / retry_wait | running，并生成新的 generation |
| 本地资源不足、无路由或取消 | 延期至 retry_wait，不消耗远端失败次数 |
| 远端失败 | 增加 attempts，指数退避并加入抖动；达到 6 次后 dormant |
| 校验成功且领取仍有效 | 同事务保存 metadata、标记 succeeded、清除提示 |
| 旧 generation 或已非 running | Stale，不保存结果、不清提示、不计提交成功 |
| 启动恢复 running | 转为 pending，立即递增 generation 使旧领取失效 |

本地延期为 60 秒；远端退避基数 60 秒，抖动倍率为 0.8 至 1.2。失败分类由 `failure.rs` 和 jobs 的转换共同解释，不把本地容量压力算成远端不可靠。

完成与重试操作返回 `Applied` 表示事务更新已提交，`Stale` 表示领取无效，外层 `Err` 表示校验、资源或存储失败。已有相同 metadata 的有效完成仍可返回 Applied，因此提交次数不一定等于新增行数。hash 与结果不匹配先拒绝；同 hash 不同字节是 Conflict。结果等待被取消不能证明事务未提交。

## 诊断读取

[status.rs](../../src/collection/status.rs) 负责每分钟诊断组装，不负责领取或重试。它先取得进程内指标，背压日志和数据库快照共用一个观察时间 now_ms，再调用 `CollectionStore::status_snapshot`。一次数据库命令在一个只读事务中读取 DueStats、Stats、recent_active、first_attempt_waiting 和 FirstAttemptBacklog，全部成功后才输出数据库事件；任一查询失败沿存储故障路径返回，不输出部分成功快照。

组合入口复用[queries.rs](../../src/collection/jobs/queries.rs) 的单项查询实现，每次诊断的数据库交互由五次减为一次；SQL 扫描量不因此减少，进程内指标也不与数据库共享原子快照。每秒调度、五秒背压刷新和完成事务保持独立，避免建立第二份内存任务事实。验证入口是 `collection::jobs::query_tests::status_snapshot_matches_individual_queries_and_propagates_failure`。

## 网络轮次与原始字节

worker 每轮最多尝试 8 个 peer，整轮 180 秒。metadata 获取配置的任务期限为 120 秒、peer 30 秒，连接与握手各 5 秒、分片 10 秒；较早到达的外层期限仍能终止内层操作。同 IP TCP 并发限制为 1；get_peers 的采集限制为全局 10/s、每 IP 1/s，还受 DHT 共享预算约束。

BEP 9 分片组装后，对原始 info 字节计算 SHA1，并检查一个完整 Bencode 字典和无尾随字节；大小上限 4 MiB。不能重新编码后计算 hash，也不能用扩展握手兼容解析放宽 info 校验。扩展握手只在严格解析遇到 InvalidDictionary 时尝试乱序兼容，边界为 4096 字节、64 层，并拒绝重复键。

下载与入库分开计数。取消后必须回收 worker 的结果并处理领取，不能丢弃 JoinHandle 后声称任务可恢复，顺序见[生命周期](../architecture/lifecycle.md)。

SQLite 保存任务事实使重启可恢复，代价是调度需要事务和索引，吞吐受数据库服务能力约束。内存队列可以减少查询但会增加双份状态与恢复复杂度；只有明确测得数据库瓶颈时才重访，且保留 generation 与原子完成边界。
