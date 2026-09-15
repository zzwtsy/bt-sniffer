---
name: rust-best-practices
description: 按具体问题查阅 Rust 借用、移动、分发、错误类型、内部可变性与性能证据。不是第二套默认实现流程；异步生命周期转专项。

license: MIT
metadata:
  author: apollographql
  version: "1.1.1-local.2"
  source: https://github.com/apollographql/skills/tree/main/skills/rust-best-practices
  upstream-version: "1.1.1"
  installed-source-hash: "0ac6606adb27e64a31bdc8c1ed34e446ae0c14bff1c7648867c46e106adc048f"
---

# Rust 语言机制参考

先用 [Rust 主流程](../rust-readable-apps/SKILL.md)确定行为与所有者，再说明尚未解决的语言问题，核对实际依赖版本和 feature。按下表只读相关章节，不把通用参考变成整个任务的额外流程。

| 问题 | 参考 |
| --- | --- |
| 借用、移动、克隆与函数提取 | [所有权](references/chapter_01.md) |
| lint 与编译工具行为 | [工具](references/chapter_02.md) |
| 分配和性能证据 | [性能](references/chapter_03.md) |
| 错误类型、传播与 panic | [错误](references/chapter_04.md) |
| 测试夹具与行为边界 | [测试](references/chapter_05.md) |
| 泛型、trait object 与装箱 | [分发](references/chapter_06.md) |
| 类型状态的收益与成本 | [状态](references/chapter_07.md) |
| rustdoc 与契约注释 | [文档](references/chapter_08.md) |
| Send、Sync 与内部可变性 | [共享所有权](references/chapter_09.md) |

借用和克隆按实际生命周期选择，不能为消除廉价 clone 制造跨任务耦合。保留调用者需要的错误分类，不用 panic 处理不可信输入。泛型、零克隆或某种语法不自动意味着更快，优化需要测量证据。

输出应是当前问题的方案、替代方案及取舍，审查给位置、触发条件、影响与证据，区分必须修复、建议和待验证推断。项目结构与验证分别见[目录设计](../../../docs/architecture/repository-layout.md)及[验证指南](../../../docs/development/validation.md)。

来源为 [Apollo Rust Best Practices Handbook](https://github.com/apollographql/rust-best-practices)及元数据记录的 skill 包。安装 hash 保留来源身份，不是 Git commit 或本地修订后的内容摘要；保留 MIT 来源与各参考的授权信息。
