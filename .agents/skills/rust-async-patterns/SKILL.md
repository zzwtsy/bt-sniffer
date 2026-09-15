---
name: rust-async-patterns
description: 实现或排查 Rust/Tokio 任务所有权、取消、超时、背压、异步锁、阻塞和退出确认。普通同步风格问题不触发。

metadata:
  source: https://github.com/wshobson/agents/tree/main/plugins/systems-programming/skills/rust-async-patterns
  version: "local.1"
  installed-source-hash: "20d32ef58158c94eb4dfc4aaf19bec7ae2a025ae5d42da7b49ccfbaeda389273"
---

# 异步生命周期

在 [Rust 主流程](../rust-readable-apps/SKILL.md)之后使用。先定位 runtime 构建、Cargo features 和具体任务；本项目跨组件顺序由[生命周期](../../../docs/architecture/lifecycle.md)维护。

## 从所有者推导流程

为每个 spawn、通道、阻塞操作和进程明确创建者、状态所有者、停止入口、结果消费者和完成条件。没有结果处理的 JoinHandle 不能用丢弃来表示关闭；取消 token 是通知，abort 是中止请求，都需要相应回收确认。

沿正常完成、提前错误、超时、外层 future 被丢弃逐条检查。`select!` 未选中分支会被取消，核对它是否已发送命令或更新状态。入队后的数据库操作不因等待者退出而撤销；permit 应随真实工作保留到结束，而非只覆盖等待 future。

## 资源与并发

为队列、在途任务、变长载荷与期限分别设置边界。通道背压必须能被取消，不能在关闭消费者后等待永远无法完成的生产。组合操作说明共享期限或分阶段期限，避免嵌套 timeout 意外放大总时间。

同步锁只覆盖短且不跨 await 的状态更新；异步锁也不能用来隐藏混乱所有权。阻塞 I/O 从网络 runtime 隔离，但 spawn_blocking 不提供已开始工作的可取消保证。引入线程或进程时同时设计结束与回收，不新增无人持有的后台工作。

## 验证与参考

优先受控时钟、本机 socket 和明确同步屏障，覆盖取消恰好发生在发送、接纳或提交后的反例，不用 sleep 猜测完成。并发变化核对故障传播与关闭顺序，报告位置、触发条件、影响及证据，区分确定问题与待验证推断。

机制细节按问题读[参考](references/details.md)；可运行代码在[独立 workspace](examples/Cargo.toml)，修改它使用项目检查范围 examples。产品行为测试仍在产品模块，示例通过不证明产品关闭正确。项目检查规则见[验证指南](../../../docs/development/validation.md)。

元数据保留原始来源与安装 hash，本地适配不重算来源记录。
