---
name: rust-best-practices
description: >
  编写、审查或重构 Rust 代码时，依据所有权、错误契约、测试行为和实际性能证据选择实现。
  适用于 Rust API 与实现决策；纯文案修改无需加载，Tokio 任务生命周期由异步专项 skill 处理。
license: MIT
metadata:
  author: apollographql
  version: "1.1.1-local.1"
  source: https://github.com/apollographql/skills/tree/main/skills/rust-best-practices
  upstream-version: "1.1.1"
  installed-source-hash: "0ac6606adb27e64a31bdc8c1ed34e446ae0c14bff1c7648867c46e106adc048f"
---

# Rust 实现与审查

这是基于 Apollo GraphQL 手册的本地修订。以代码契约、项目约束和可验证证据作决定；规则不能代替对调用者、所有权和失败语义的分析。

## 使用方式

1. 先读相关实现与调用者，以及 `Cargo.toml`、工具链配置和已有验证命令。按项目的 MSRV、依赖版本和 feature 组合选择 API；本文不声明统一的最低 Rust 版本。
2. 在本仓库工作时，读取一次[项目 Rust 约束](../../../docs/rust-development.md)。在其他仓库使用时，改读该仓库的规范。
3. 根据下表读取与任务有关的章节，无需预读全部手册。异步代码只有涉及任务、取消、通道或锁时才需要额外加载异步专项指导。
4. 审查意见说明具体触发条件、影响和代码位置。区分确定错误、需要测量的性能猜测与可选风格建议。
5. 修改后执行与风险相称的检查，分别报告已通过、未运行及环境阻塞的验证。

## 按任务阅读

| 当前决策 | 参考 |
| --- | --- |
| 借用、移动、克隆、迭代或提取函数 | [第 1 章：编码与所有权](references/chapter_01.md) |
| Clippy、feature、工具链与 lint 配置 | [第 2 章：工具与检查](references/chapter_02.md) |
| 分配、数据布局、性能优化 | [第 3 章：性能证据](references/chapter_03.md) |
| 错误分类、传播、恢复与 panic | [第 4 章：错误契约](references/chapter_04.md) |
| 测试边界、夹具、异步时间与快照 | [第 5 章：行为测试](references/chapter_05.md) |
| 泛型、trait object 与装箱 | [第 6 章：分发与抽象](references/chapter_06.md) |
| 编译期状态与运行时状态机 | [第 7 章：类型状态](references/chapter_07.md) |
| 注释、rustdoc、TODO 与文档范围 | [第 8 章：文档契约](references/chapter_08.md) |
| Send、Sync、共享所有权与内部可变性 | [第 9 章：指针与线程安全](references/chapter_09.md) |

## 决策底线

- 读取数据通常借用，保留或转移数据则明确所有权；不要为消除一次廉价克隆制造跨任务生命周期耦合。
- 可恢复失败保留调用者需要的分类；panic 只表达明确的失败策略或内部不变量，不能用来处理不可信输入。
- 选择简单、可维护的抽象。没有测量依据时，不把泛型、迭代器、零克隆或固定字节阈值当成性能结论。
- 测试围绕行为和不变量组织，允许同一场景有多个相关断言。不要为统一风格重排无关测试或重构模块边界。

来源：[Apollo Rust Best Practices Handbook](https://github.com/apollographql/rust-best-practices)。元数据中的 hash 保留安装记录，不是 Git commit，也不表示本地修订后的内容 hash；更新时对照上游差异与本地规则，避免整目录覆盖。
