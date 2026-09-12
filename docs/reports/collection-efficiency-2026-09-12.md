# 采集效率与积压控制交付记录

本轮四阶段代码已实现：细分聚合指标、反向验证接纳控制、DHT 与单 peer 下载交叠、默认 freshness 采样背压。默认 worker、TCP 并发、DHT 配额和任务容量不变；schema v2 与原始 metadata 字节保持不变。

## 独立审查范围

| 阶段 | 主要文件 | 短测证据 |
| --- | --- | --- |
| 1 指标 | `src/metrics.rs`、`src/metadata/`、`src/dht/traffic/`、`src/persistence/mod.rs` | 桶边界与溢出、阶段成功/失败/取消、按请求而非轮询计数；关闭补齐最后区间 |
| 2 验证接纳 | `src/dht/traffic/`、`src/dht/dispatcher/traffic.rs`、`query.rs`、`response.rs` | 每 IP 60 秒且重复不延长、双栈共享 10 个待发许可、10,000 IP 上限、双栈轮换 Node ID/端口仍正常回复 |
| 3 流式交付 | `src/collector/lookup.rs`、`src/collector/mod.rs` | 双栈慢查找下提前连接、实际在途取消、连接许可回收、32 地址去重与通道边界、失败 hint 补位和单族无路由 |
| 4 积压背压 | `src/collector/backpressure.rs`、`src/config.rs`、`src/app/mod.rs` | 真实 SQLite 到期/退避/刷新、小容量取整、30 秒恢复防抖、暂停原因组合及 capacity 回退 |

阶段二完整默认回归为 276 项通过，阶段三 277 项，阶段四初次 281 项；新增补充回归后的最终默认测试为 **284 项通过、0 失败、5 项默认忽略**。忽略项包括两项手动长测、历史调度比较、本轮流式短比较和背压短模拟。格式检查、严格 Clippy（all-targets/all-features）、Release 构建和 Git 差异检查均通过。未提交 Git，未修改用户上传的 `state/`。

## 固定输入比较

原版源码在实现前复制到独立目录并记录指纹；仅向原版追加与新版逐字相同的 loopback fixture，不回移功能代码。原版与新版采用同一工具链、Release 配置、4 个相同 hash、相同 metadata 和远端行为：一个响应 DHT 节点返回可连接 peer 与三个不响应候选，peer 握手延迟 1,100 ms，DHT 查找期限 30 秒。关闭提前采样暂停，全部 4 个任务都已接纳；本场景直接驱动 worker，不包含 SQL 领取耗时。

| 指标 | 原版 | 新版 |
| --- | ---: | ---: |
| 成功结果 | 4/4 | 4/4 |
| 全部任务完成 | 约 34.4 秒 | 约 7.1 秒 |
| 首个 TCP 连接 | 约 30 秒 | 1 ms 内 |
| 采集 RPC | 16 | 8 |
| TCP 活跃峰值 | 1 | 1 |

新版在成功后取消剩余查找，测试实际观察到 collector transaction 取消，并等待 pending 和 TCP IP 表归零、peer 服务任务结束、dispatcher 正常关闭。另有 IPv4/IPv6 各自的默认短回归。时间是本机单次实测，0 ms 表示低于毫秒记录精度；本机 4 个固定任务不预测公网每小时产出。

资源峰值比较直接测量 TCP 活跃数；transaction 容量、IP 表、通道和双栈合计速率由默认有界回归验证。本短比较未采样 RSS，也没有测出生产负载的 transaction 峰值；这些不能标为本轮公网或内存长测通过。

背压单独采用固定输入模拟：1,800 秒虚拟窗口、每秒提供 4 个 hash、4 worker、每任务固定 30 秒。两种策略均完成 240 个任务；capacity 接纳 7,200 个、活跃峰值 6,964，freshness 接纳 1,040 个、峰值 1,008，累计暂停 1,540 秒。两者领取等待 p95 均为 1,653 秒。该输入证明减少主动扩张，**没有证明吞吐或领取等待改善**；已经接纳的积压、announce 和历史补建仍可能让等待超过 5 分钟。

完整源码清单、工具链、配置、fixture 指纹及原始比较数据见 [JSON 报告](collection-efficiency-2026-09-12.json)。新比较日志使用测试依赖 `serde_json` 输出；与历史调度模拟分开记录。

复现本轮两个独立短场景（不要无筛选运行所有 ignored 测试）：

```bash
cargo test --release --offline --locked collector::tests::pipeline_release_comparison -- --exact --ignored --nocapture
cargo test --release --offline --locked collector::backpressure::comparison::sampling_backpressure_release_comparison -- --exact --ignored --nocapture
```

## 部署与手动验收

新二进制为 `target/release/bt-sniffer`。继续原命令即可使用默认背压；只回退采样暂停策略可执行：

```bash
./bt-sniffer --state-dir ./state --sample --fetch --sample-backpressure capacity
```

默认策略为 `freshness`；回退不取消流式下载、验证冷却或新的指标。新指标口径见 [采集说明](../metadata-collection.md)。

本轮 **30 分钟本机混合压力与两小时公网验收均未运行**。控制查询 2 秒响应、预热后 RSS 增量 32 MiB、持续公网产出及优化版数据逐条复核，继续作为独立手动验收。每次保留新目录、新指纹和独立报告，提前停止标为中止。

用户此前上传的约 30 分钟公网结果（36 份 metadata，SHA-1、完整字典与 SQLite 检查通过，结束时 9,134 活跃任务、最后快照 1,637 次待发超时）仅作为本轮设计的历史基线。该版本没有超时类别归属，不能把它们全部归因于反向验证，也不能将该结果记为优化版通过。
