# 存储与恢复

负责 SQLite 连接所有权、schema、命令预算与关闭；业务 SQL 由各 Store 维护。源码入口：[Storage](../../src/storage/mod.rs)、[schema](../../src/storage/schema.rs)、[DhtStore](../../src/dht/persistence/mod.rs)、[CollectionStore](../../src/collection/store.rs)、[任务事务](../../src/collection/jobs/transitions.rs)。验证入口：`storage::tests`、`collection::jobs::tests::v1_upgrade_preserves_hashes_and_backfills_only_on_fetch`、`collection::tests::recovery::completion_rollback_and_stale_success_are_atomic`。

## 线程、锁与预算

Storage 打开状态目录并独占 `instance.lock`，连接 `state.sqlite3`，使用 WAL、synchronous=FULL 和 foreign_keys=ON。一个状态目录只允许一个所有者，instance 名称不隔离目录锁。SQLite 阻塞工作在专用线程，网络 runtime 通过 StorageHandle 提交闭包并等待 oneshot 结果。

默认命令队列容量为 128，未完成变长载荷预算为 32 MiB。载荷许可与命令一起移动，直到实际处理结束才释放；调用者取消等待不能提前释放许可，也不能撤销已入队事务。磁盘空间策略与此内存预算不同，CLI 的状态预算不是对数据库文件每个字节的精确硬配额。

关闭命令是队列屏障；连接关闭、目录锁释放后才发送完成结果。仅丢弃 handle、收到取消通知或关闭结果通道均不能代替这一确认。

## schema 与业务事务

当前 user_version 为 5：新库在同一迁移事务中执行 v1 至 v5，旧库逐级迁移；高于 5 的版本拒绝打开。领取及最近目录查询索引在版本迁移提交后幂等补建，补建失败不撤销已提交的版本迁移。版本 3 新增可重建目录和 FTS5 trigram 外部内容索引；版本 4 增加路径搜索覆盖计数，并让 `indexed` 只计已确认覆盖状态的目录行。升级前的目录行以未知状态逐条重解析，Session 在 fetch 或 monitor 启用时每轮至多处理一条，间隔至少 100 ms。

| 表 | 核心事实与约束 |
| --- | --- |
| node_identities | instance 与 family 唯一，20 字节 node_id，random-v1 或 bep42 身份方法及外部地址缓存 |
| routing_contacts | 身份下的节点、IP、端口、最后响应时间 |
| infohashes | 20 字节 hash，first_seen 与 last_seen |
| metadata | 独立 id、规范完整 hash、1 至 4194304 字节原始 info、fetched_at |
| torrent_identities | 算法与完整 hash 唯一定位 metadata |
| swarm_metadata | 20 字节查找键到 metadata 的多对多关联与匹配依据 |
| sampling_cooldowns | 身份、节点/IP kind、key、16 字节 lease、pending、期限与失败次数 |
| fetch_jobs | 状态、attempts、due_at、generation、updated_at 与 error |
| peer_hints | hash 对应地址与 observed_at |
| torrent_catalog | v1／v2／hybrid info 的有界展示摘要、解析状态、搜索派生文本及路径覆盖状态 |
| torrent_catalog_fts | 名称及已纳入搜索文本的文件完整路径的 trigram 字面子串索引 |
| torrent_catalog_state | metadata 总数、已确认覆盖状态的目录数及路径搜索不完整数 |

完整 SQL 和索引定义以 schema 为准。`metadata_fetched_at_hash(fetched_at DESC, hash DESC)` 支持最近目录首屏和偏移分页倒序读取；`swarm_metadata_by_metadata(metadata_id, verification)` 支持列表逐项读取匹配依据，启动时也为已有 v5 数据库幂等补建，不改变 schema 版本；`search_incomplete` 记录派生子串索引未覆盖全部文件路径的目录行，12 MiB 搜索文本上限不会写入半截路径。监控 API 将目录回填完成与路径搜索完整性分开报告。完成事务把 metadata、目录/FTS、任务状态、提示清理作为一个原子操作；语义解析失败写入 `unavailable` 目录行并标记搜索覆盖不完整，不改变原始 metadata 的接纳规则。存储校验和事务不能被仅对内存对象的检查替代。

诊断组合查询使用一次只读事务，避免跨命令读取到不同提交状态；完整读取口径由[采集诊断](collection.md#诊断读取)维护。它仍占用专用线程，不能视为无成本读取，也不改变命令或载荷预算。

## 故障与恢复

锁冲突、数据库错误、容量不足、数据冲突和 Closed 是不同原因。事务失败回滚未提交修改；结果接收失败不证明操作未发生。恢复 running 任务时先使旧 generation 失效，已有 metadata 的任务恢复为成功。采样冷却及联系人也有独立恢复路径，不能通过删数据库修复一个局部错误。

发现数据库写入故障后会话暂停自动采集，修复环境后通过关闭和重开恢复。排障先保存现场、确认进程退出，再只读复核；不要手删 WAL 或锁文件“解锁”。诊断命令和完整性检查局限见[诊断指南](../operations/diagnostics.md)。

专用线程串行执行简化事务与锁归属，代价是长操作阻塞后续命令。若持续观察到排队或磁盘瓶颈，先缩小操作及载荷，再评估架构调整；引入连接池需要重新证明预算、事务和关闭语义。

## 完整身份与语义迁移

v5 的 metadata 使用独立整数主键，`torrent_identities` 以算法与完整 hash 唯一定位结果，`swarm_metadata` 将 20 字节发现键关联到一个或多个结果。不能按 SHA-256 前缀合并记录。有效 hybrid 的 SHA-1 与 SHA-256 指向相同原始字节，列表优先使用 v1 地址；相同完整身份但字节不同返回冲突，事务不覆盖原记录。

迁移保留旧原始字节、观察时间、领取历史和 generation，建立旧 v1 别名；语义与 hybrid 别名随后逐条回填。失败事务整体回滚；没有自动降级或全库网络重采集。归并另一个身份的任务会增加其 generation，旧领取迟到返回 Stale。原始数据、别名、查找键关联、目录／FTS、成功状态与提示清理共用事务。

历史 `invalid / empty_directory` 目录也由 Session 逐条重算。修正后的目录、hybrid 别名与任务归并在原事务内提交；失败回滚，重启继续处理尚未修正的记录，不重下载原始数据。
