# 存储与恢复

负责 SQLite 连接所有权、schema、命令预算与关闭；业务 SQL 由各 Store 维护。源码入口：[Storage](../../src/storage/mod.rs)、[schema](../../src/storage/schema.rs)、[DhtStore](../../src/dht/persistence/mod.rs)、[CollectionStore](../../src/collection/store.rs)、[任务事务](../../src/collection/jobs/transitions.rs)。验证入口：`storage::tests`、`collection::jobs::tests::v1_upgrade_preserves_hashes_and_backfills_only_on_fetch`、`collection::tests::recovery::completion_rollback_and_stale_success_are_atomic`。

## 线程、锁与预算

Storage 打开状态目录并独占 `instance.lock`，连接 `state.sqlite3`，使用 WAL、synchronous=FULL 和 foreign_keys=ON。一个状态目录只允许一个所有者，instance 名称不隔离目录锁。SQLite 阻塞工作在专用线程，网络 runtime 通过 StorageHandle 提交闭包并等待 oneshot 结果。

默认命令队列容量为 128，未完成变长载荷预算为 32 MiB。载荷许可与命令一起移动，直到实际处理结束才释放；调用者取消等待不能提前释放许可，也不能撤销已入队事务。磁盘空间策略与此内存预算不同，CLI 的状态预算不是对数据库文件每个字节的精确硬配额。

关闭命令是队列屏障；连接关闭、目录锁释放后才发送完成结果。仅丢弃 handle、收到取消通知或关闭结果通道均不能代替这一确认。

## schema 与业务事务

当前 user_version 为 2：新库在同一迁移事务中执行 v1、v2；v1 可迁移到 v2；高于 2 的版本拒绝打开。版本 2 会补建领取索引。补索引在迁移事务提交后执行，索引失败不撤销已提交的版本迁移。

| 表 | 核心事实与约束 |
| --- | --- |
| node_identities | instance 与 family 唯一，20 字节 node_id，random-v1 身份方法 |
| routing_contacts | 身份下的节点、IP、端口、最后响应时间 |
| infohashes | 20 字节 hash，first_seen 与 last_seen |
| metadata | hash 外键、1 至 4194304 字节原始 info、fetched_at |
| sampling_cooldowns | 身份、节点/IP kind、key、16 字节 lease、pending、期限与失败次数 |
| fetch_jobs | 状态、attempts、due_at、generation、updated_at 与 error |
| peer_hints | hash 对应地址与 observed_at |

完整 SQL 和索引定义以 schema 为准；状态含义只在[采集专题](collection.md)维护。完成事务把 metadata、任务状态、提示清理作为一个原子操作。存储校验和事务不能被仅对内存对象的检查替代。

## 故障与恢复

锁冲突、数据库错误、容量不足、数据冲突和 Closed 是不同原因。事务失败回滚未提交修改；结果接收失败不证明操作未发生。恢复 running 任务时先使旧 generation 失效，已有 metadata 的任务恢复为成功。采样冷却及联系人也有独立恢复路径，不能通过删数据库修复一个局部错误。

发现数据库写入故障后会话暂停自动采集，修复环境后通过关闭和重开恢复。排障先保存现场、确认进程退出，再只读复核；不要手删 WAL 或锁文件“解锁”。诊断命令和完整性检查局限见[诊断指南](../operations/diagnostics.md)。

专用线程串行执行简化事务与锁归属，代价是长操作阻塞后续命令。若持续观察到排队或磁盘瓶颈，先缩小操作及载荷，再评估架构调整；引入连接池需要重新证明预算、事务和关闭语义。
