# 历史验证记录摘录

本文件保存从当前使用手册提取的历史记录，未重新执行其中的验证。测试数量、测量值、失败、未验证范围和临时路径均按原文保留，不能作为当前工作区的验收结果。临时材料可能无法取得；源码指纹缺失时不能推定对应当前版本。

## 采集验证

来源：`docs/metadata-collection.md` 的“2026-09-11 验证记录”小节。验证日期：2026-09-11；源码指纹：原记录未注明。以下为原文摘录。

### 2026-09-11 验证记录

| 检查 | 结果 |
| --- | --- |
| 默认测试 | 234 项通过；两个长时间测试默认忽略 |
| 格式、严格 Clippy、Release 构建 | 通过 |
| 本机真实时间混合流量 | 1,800.03 秒、17,593 轮，通过容量、响应、RSS 及正常关闭检查 |
| 公网两小时验收 | 用户在约 27 分 35 秒时中止，未完成两小时验收 |
| 中止后的数据复核 | 保留 46 份原始 metadata，共 2,760,836 字节；逐条 SHA-1 复核通过 |

公网进程被停止后保留了 4 个 `running` 任务；再次启动采集时按恢复规则重新调度。此次公网停止不是正常关闭验收，不能据此声称两小时测试通过。

本地机器证据保存在 `target/collection-validation/report.json` 和 `source-sha256.json`；公网临时状态目录为 `/tmp/bt-sniffer-public-acceptance-I4ZCuk`。这些构建及临时目录不随 Git 分发。

## 接口与状态收敛

来源：`docs/diagnostics-validation.md` 的“接口与状态收敛的本地验证记录”小节。验证日期：原记录未注明；源码指纹：原记录未注明。以下为原文摘录。

### 接口与状态收敛的本地验证记录

以下记录对应本次五批重构的工作区，不代表服务器采集效果。默认回归使用临时数据库和本地 socket，未运行公网及 ignored 长测。

| 批次 | 实际验证结果 |
| --- | --- |
| 配置与 peer 错误边界 | 340 个单元测试、4 个 CLI 测试通过，8 个 ignored |
| 任务存储与重试规则 | 22 个 jobs 测试通过，3 个 ignored；迁移前后 34 条 SQL 在归一化空白后一致 |
| 查找与 TCP 许可 | 110 个 collection 测试通过，7 个 ignored |
| 诊断与日志出口 | 342 个单元测试、4 个 CLI 测试通过，8 个 ignored |
| 采样消费与 Session | 2 个 ingest 测试通过，覆盖分段恢复及取消确认等待后的幂等重放 |
| 最终工作区 | 格式检查、全部目标编译、严格 Clippy 通过；343 个单元测试、4 个 CLI 测试通过，8 个 ignored |

最终命令为 `cargo fmt --all -- --check`、`cargo check --locked --workspace --all-targets`、`cargo clippy --locked --workspace --all-targets -- -D warnings` 和 `cargo test --locked --workspace`。

实施前基线曾在 `app::session::tests::shutdown_preserves_unverified_candidates` 重新打开数据库时出现一次 `Locked`；该测试单独复核及最终完整回归均通过，尚未稳定复现。首次受限沙箱运行中的 socket `PermissionDenied` 属于环境限制，之后在允许本地 socket 的环境完成上述验证。

## 日志契约清理

来源：`docs/diagnostics-validation.md` 的“日志契约版本 2 清理验证记录”小节。验证日期：原记录未注明；源码指纹：原记录未注明。以下为原文摘录。

### 日志契约版本 2 清理验证记录

本次清理默认测试仍使用本地 socket 和临时数据库，不读取历史运行数据库作为测试夹具。

| 阶段 | 实际结果 |
| --- | --- |
| 清理前基线 | 343 个单元测试、4 个 CLI 测试通过，8 个 ignored |
| 第一批：target、配置字段、命名及测试接口 | 344 个单元测试通过；修正旧配置文本断言后，4 个 CLI 测试通过 |
| 第二批：任务计时与结果 | 344 个单元测试、4 个 CLI 测试通过，8 个 ignored |
| 第三批：阶段与完整握手 | 首轮 344 个单元测试通过，1 个数据库重开测试出现 Locked；该测试单独复核通过 |
| 最终工作区 | 格式、全部目标编译和严格 Clippy 通过；346 个单元测试、4 个 CLI 测试通过，8 个 ignored |

新增或迁移的验证覆盖四种模式的生产启动字段、真实模块 target、任务首次 poll 与跨区间中止／panic、完整握手跨区间归属、重复结束保护、Network 桶溢出，以及失败、取消和各层期限的区别。jobs SQL 字符串对比一致，依赖、schema、两条 Bencode 规范化路径和历史报告字节未变。

本轮再次出现的 `Locked` 位于 `app::session::tests::shutdown_preserves_unverified_candidates`，单独复核和最终完整回归通过；未据此修改存储关闭策略，也不宣称该偶发问题已修复。未运行公网或 ignored 长测，未部署、未提交。
