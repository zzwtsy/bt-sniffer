---
name: rust-async-patterns
description: >
  设计、实现或排查 Rust/Tokio 的任务生命周期、取消、超时、通道背压、异步锁及阻塞边界。
  用于并发网络服务与异步资源管理；普通同步 Rust 风格问题无需加载。
metadata:
  source: https://github.com/wshobson/agents/tree/main/plugins/systems-programming/skills/rust-async-patterns
  version: "local.1"
  installed-source-hash: "20d32ef58158c94eb4dfc4aaf19bec7ae2a025ae5d42da7b49ccfbaeda389273"
---

# Rust 异步生命周期

从任务的所有者、完成条件和资源释放路径出发设计并发。使用当前项目已有的 runtime、依赖与调度方式。

## 使用方式

1. 检查 `Cargo.toml` 的 Tokio features、runtime 构建位置和相关调用链。在本仓库读取一次[项目 Rust 约束](../../../docs/rust-development.md)；其他仓库使用自己的规范。
2. 找到任务所有者、容量边界、取消入口和完成确认。明确调用者放弃等待后，工作是终止、继续，还是交给其他所有者。
3. 按下表阅读参考中的对应部分，再选择实现。通用错误或所有权问题可按需查阅 Rust 基础规范。
4. 验证失败、取消及收尾路径。不能用“已发送取消通知”或固定 sleep 作为清理完成的证据。

## 按问题阅读

| 问题 | 参考 |
| --- | --- |
| spawn、并发上限、任务结果 | [任务所有权](references/details.md#tasks) |
| 队列满载、sender 存活、消息丢弃 | [通道与背压](references/details.md#channels) |
| select、部分 I/O、超时、持久化请求 | [取消安全](references/details.md#cancellation) |
| 停止接纳、排空、join、abort | [退出与完成确认](references/details.md#shutdown) |
| 同步锁、异步锁、SQLite、RAII | [资源与阻塞边界](references/details.md#resources) |
| async trait、Pin、日志与测试 | [工具选择](references/details.md#tools) |
| 修改这里的示例 | [可运行示例与验证](references/details.md#examples) |

## 核心检查

- 有界通道只限制排队条数；另行检查载荷字节、等待发送的任务和在途工作数量。
- `tokio::spawn` 要求 future 及输出满足 `Send + 'static`；即使使用单线程 runtime，该要求仍然存在。需要非 `Send` 状态时，核对 `LocalSet`/`spawn_local` 的所有权与驱动方式。
- 同步锁用于短暂且不会跨 `.await` 的临界区；异步锁允许跨 `.await`，但仍需检查锁顺序、重入和竞争。
- `CancellationToken` 只通知取消。等待任务完成、释放 transaction/许可以及确认持久化结果，分别需要明确实现。
- 已启动的 `spawn_blocking` 工作不能靠 `abort` 停止；不要把异步等待超时解释为底层操作已停止。

此版本修订自元数据所列上游。安装 hash 不是 Git commit，也不表示本地修订后的内容 hash；保留它用于对照安装基线，更新前审阅上游差异。
