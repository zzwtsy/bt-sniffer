# 诊断与证据

先确定是启动、发现、采集、存储还是关闭问题，再查相应所有者。源码入口：[诊断工具](../../scripts/diagnostics.py)、[证据身份](../../scripts/evidence.py)、[验收辅助](../../src/acceptance/mod.rs)。验证入口：`scripts/tests/test_diagnostics.py`、`src/acceptance/tests.rs`；本地工具检查用 `python3 scripts/check.py tools`。

## 按症状定位

| 症状 | 核对顺序 |
| --- | --- |
| 启动失败 | CLI 冲突、RUST_LOG、目录权限/锁、监听地址族与端口；application_start 不代表资源准备完成 |
| 没有邻居或采样 | no-bootstrap 和恢复联系人、地址策略、路由状态、sampler pause、DHT 流量与配额 |
| 有 hash 没 metadata | fetch 开关、接纳与 first_attempt_backlog、领取类别、peer 各阶段结果、数据库容量 |
| 下载多提交少 | Applied / Stale / Err，代际失效、事务回滚；不能混算下载和提交 |
| 日志缺失 | 过滤指令、logging_queue 丢弃、工作目录与轮转、run_id、进程退出状态 |
| 关闭卡住 | Session 当前关闭阶段、worker 与 dispatcher 依赖、数据库 finished、进程实际存活 |

字段解释以[事件参考](../domains/log-events.md)为准；任务状态和数据库不变量分别见[采集](../domains/collection.md)、[存储](../domains/storage.md)。

## 隔离的产品复测工具

工具面向 Linux 服务器，使用固定 Rust、Python 和可用构建依赖。先准备独立产物，在仓库根运行：

```sh
python3 scripts/diagnostics.py prepare
```

prepare 会构建 release 到独立临时 target，保存源码身份、Git diff、工具链及产物摘要；构建前后源码不同则失败。它不启动 DHT，但构建可能需要依赖源；stdout 返回新证据目录。成功须有 preparation.json 与匹配产物；Ctrl-C 中断构建后不能使用不完整准备记录。

以下两条以 prepare 输出路径替换 RUN。**observe 需要另行授权公网和长时间验收**，不是日常开发命令：

```sh
python3 scripts/diagnostics.py observe RUN
python3 scripts/diagnostics.py verify RUN
```

observe 校验产物并独占一次运行目录，启用双栈随机监听端口、sample、fetch 与并发 4，固定日志过滤；首次有效采样后观察 35 分钟。它写 state、logs、command.json、stdout.log、observation.json 并产生公网 UDP/TCP 流量。Ctrl-C 或 SIGTERM 请求收尾；若 40 秒后仍未确认退出，记录 still_running_pid 留待人工处理，不宣称正常结束，不强杀产品后继续复核。

verify 要求已确认进程退出，没有 still_running_pid，再以只读和 query_only 打开数据库。它检查 integrity_check、foreign_key_check、schema v3、running 为 0、metadata 的 SHA1、metadata/catalog/FTS/索引状态计数一致且回填完成，以及任务积压，输出 database-verification.json。成功判据还要求观察状态满足工具约束；保留 manual_review_required=true，不能把写出 JSON 当成验收通过。复核可 Ctrl-C 停止，未完成报告不算通过。

## Rust 验收报告

[Report](../../src/acceptance/mod.rs) 保持 JSON 版本 2，公共字段、运行身份和产物身份由具体可序列化类型维护。config、statistics、families 是业务扩展 JSON；verification 是字符串列表，run_errors 为可选字符串列表，未设置或清除时省略字段。Python 产品诊断和开发检查各自维护报告，不共享此类型。

初始化返回 io::Result；调用者开始执行时调用 running，结束时显式选择 Passed、Failed、Aborted 或 EnvironmentBlocked。准备阶段异常析构尽力保存 environment_blocked，执行阶段异常析构保存 failed，只有显式完成能产生 passed。完成在写入前锁定状态，成功或失败后都拒绝再次完成及修改，Drop 不重试。报告通过临时文件完整写入后排他发布，不覆盖旧证据；保存失败须由调用者报告，不能当作验收成功。打印使用只读序列化快照。

源码清单与摘要算法保持不变：运行时源码快照不证明构建一致性，Git HEAD 也不能代表完整工作区。身份测试继续与 Python 摘要交叉核对，报告测试验证状态及发布失败边界。

## 证据边界与现场保护

Python verify 校验原始 info 的 SHA1，但不做完整 Bencode 字典验证；Rust 的 metadata 和验收测试负责该边界。结构完整、哈希一致、本次公网观察成功分别证明不同事实，不能互相代替。短窗口和特定服务器的结果不能外推总体性能。

不要删除 WAL、SHM、锁文件或运行目录来“恢复正常”；先保留证据，确认进程状态，再处理实际故障。运行身份包含源码与产物摘要，Git HEAD 单独不足以表示未提交工作区。产品报告使用自己的版本，与 development-check 的 result.json 分开解释。
