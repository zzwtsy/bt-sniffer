# 采集可靠性与 DHT 公网运行加固交付记录

本轮代码实施完成；30 分钟本机与两小时公网验收均未启动，不能据此声称新版已通过公网持续产出验收。历史记录保留在 [metadata 采集说明](metadata-collection.md#2026-09-11-验证记录)，不沿用旧版测试结果。

## 分阶段审查入口

| 阶段 | 主要实现 | 行为与短测证据 |
| --- | --- | --- |
| 1. 查找补位 | `collector/lookup.rs`、`dht/shortlist.rs`、`dispatcher/maintenance.rs` | 独立协议状态共用有效近邻规则；每族一次 seeds；失败退出 shortlist，已尝试 ID/地址仍去重。双栈七个失败近邻加存活备用节点，通过真实 UDP 与直接查询对照；覆盖全部失败、正常收敛、重复地址、32 次上限、截止保留 peers、取消释放。维护查找补位及查询上限回归通过。 |
| 2. 本地等待分类 | `collector/lookup.rs`、`collector/mod.rs`、`dispatcher/fetch.rs`、`storage/jobs/mod.rs` | TCP 同 IP 公平 semaphore 和 RAII 清理；任务总期限包含等待。实际 RPC 发送由 dispatcher 记录，完全未发包不误计 no_peers。本地等待、无路由与取消延期 60 秒，实际远端失败保留六轮休眠规则；控制故障仍进入监督收尾。公平、取消、释放及真实 SQLite attempts 回归通过。 |
| 3. 新鲜任务调度 | `storage/jobs/mod.rs`、`storage/schema.rs`、`collector/mod.rs` | 已到期任务按新鲜合法 hint/其余两类 3∶1 轮转，空类借用；类内 `due_at, hash`，不提前领取退避任务。满载仍允许刷新已接纳 hints。新增到期部分索引，user_version 保持 2，不增加数据字段、不改写已有 metadata。临时 SQLite 顺序、过期、地址策略、满载刷新、generation 和查询计划回归通过。 |
| 4. 有界指标 | `metrics.rs`、`storage/jobs/mod.rs`、`collector/mod.rs` | 固定大小区间/累计计数、固定桶耗时、当前到期积压；查找取消和关闭收尾也保留聚合。事务返回 Applied/Stale，只有 Applied 计提交。固定桶、旧领取和事务回滚回归通过。 |
| 5. 统一 DHT 控制 | `dht/traffic/`、`dispatcher/traffic.rs`、`dispatcher/runtime/`、`net/udp/`、`sampler/durable/`、`config.rs`、`persistence/mod.rs` | 会话共享 governor 预算，双栈和四类主动查询共用额度；组合配额先探测再提交。待发及在途共用容量，5 秒本地超时，按目的地址补位发送。原始接收先限流再解码，响应预留仍校验协议身份，回复不排队。确认未发送的租约撤销；不确定结果/已发送取消保守恢复。额度前缀上界、响应预留、IP 表容量、待发取消/关闭、显式 RPC 取消及租约 generation 回归通过。 |

文件路径均相对 `src/`。改动未进行 Git 提交，原有暂存内容保持原状。

## 最终默认验证

对应源码清单 SHA-256：`eca0f6bec6232fa0f5e8ce99dde1dfd4a3f916d89e53ac34091b3e6c01a80b8b`。

清单由排序后的 `Cargo.toml`、`Cargo.lock` 和全部 `src/**/*.rs` 的 SHA-256 构成，再对清单求 SHA-256；逐文件值及工具链见 [JSON 报告](reports/scheduler-comparison-2026-09-12.json)。工具链为 `rustc 1.98.1 (48a229cea 2026-09-01)`，`x86_64-unknown-linux-gnu`。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --all-targets --all-features -- -D warnings` | 通过 |
| `cargo test --all-targets` | 265 通过，0 失败，3 忽略；约 16.06 秒 |
| `cargo build --release` | 通过 |
| `git diff --check`、`git diff --cached --check` | 通过 |
| 独立 Release 调度短基准 | 通过，见下表 |
| 30 分钟本机混合流量 | 未运行；保留独立手动入口 |
| 两小时公网双栈 | 未运行；保留独立手动入口 |

默认忽略项为两项手动长测和一项独立短基准。完整默认回归在允许 loopback socket 的环境执行；最初受限沙箱的 EPERM 不作为代码失败或公网结果。

## 固定输入调度比较

相同 Release 测试二进制、真实临时 SQLite，10,000 活跃任务与 40,000 历史记录。输入固定为 2,500 个较早的普通任务和 7,500 个新鲜任务；4 个模拟 worker，模拟窗口 2,000 ms，普通/新鲜任务服务时长分别固定为 100/10 ms。

| 指标 | 旧到期顺序 | 新 3∶1 顺序 |
| --- | ---: | ---: |
| 模拟完成任务 | 76 | 247 |
| 领取普通任务 | 80 | 62 |
| 领取新鲜任务 | 0 | 189 |
| 领取等待 p50 | 1,900 ms | 1,790 ms |
| 领取等待 p95 | 2,800 ms | 2,710 ms |
| 实际模拟执行耗时 | 0.0115 s | 0.1595 s |

这个输入验证新鲜任务可以推进，同时历史任务保留名额；新 SQL 的筛选开销更高，不能把模拟完成量差异称为网络性能提升。服务时长是假设，结果不预测公网吞吐或成功率，也不以 metadata 数除以全部发现数定义下载成功率。

查询计划测试确认新领取查询使用 `fetch_claim_due`，hint 子查询使用现有主键，无临时排序、无历史 hash 全表扫描。首选类为空或稀疏时仍可能扫描到期活跃任务；这是当前单机容量范围内为不持久化优先级字段作出的取舍。

## 手动验收报告与范围

按 README 中完整测试名执行 `--release ... -- --ignored --nocapture`，不要无筛选运行全部 ignored 测试。每次新建并保留独立临时目录，JSON 报告包含源码指纹、工具链、配置、时长、统计及 `passed` / `failed` / `aborted` / `environment_blocked` 状态。两项长测支持 Ctrl-C/SIGTERM 正常收尾；提前停止返回非零，不计通过。

- 本机入口包含持续新任务、成功/失败 peer、重复宣布、无效 token 与单 IP 限流压力；检查 worker/活跃任务界限、健康控制 2 秒期限、60 秒预热后的 32 MiB RSS 增量，以及关闭。该长场景覆盖 IPv4 loopback；IPv6 本机闭环在默认短测验证。
- 公网入口使用默认双栈服务配置；完整运行两小时且至少取得一份 metadata，正常关闭后检查 SQLite integrity_check，并逐条复核 SHA-1 和完整字典。分别报告 IPv4/IPv6 实际验证的 DHT 响应数；0 表示该族缺少正向证据。schema v2 不保存 metadata 来源地址族，报告明确不提供无法归属的每族下载成功数。
- 长测代码与报告格式已编译并通过报告状态短测；以上时长、RSS 及公网产出条件尚无本轮实测结果。

DHT 日志中的四元素包数/字节数数组依次对应采集、控制、采样、反向验证。配额允许一秒额度突发；TCP 只保留并发、同 IP、大小与期限边界，不提供总下载字节限速。

## 后续独立修复

服务器抓包暴露的 KRPC 未排序字典兼容问题已修复，见 [独立验证记录](reports/krpc-compatibility-2026-09-12.md)。该记录包含修复后的源码指纹、双栈本地回归与 Release 构建结果；上文五阶段交付的指纹和测试数量继续作为历史记录保留，不能用它们代表修复后版本的公网验收。

采集效率与积压控制的后续四阶段改动另见[独立交付记录](reports/collection-efficiency-2026-09-12.md)。本文件原测试数量、源码指纹和调度模拟继续作为历史证据。
