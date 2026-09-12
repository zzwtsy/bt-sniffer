# 第 8 章：文档契约

## 注释需要提供的信息

注释应补充仅靠代码不容易看出的原因：协议限制、不变量、取消后的责任、事务边界、锁顺序、兼容性或有证据支持的性能选择。

`///` 适合调用者需要知道的用途、参数、返回结果、错误及 panic 契约；`//!` 适合模块职责和使用顺序。篇幅由信息价值决定，不机械要求每个函数都写说明，也不因注释长就拆分代码。

外部 library API 通常需要完整文档；内部 `pub(crate)` 和 binary 模块按实际调用边界描述。是否开启 `missing_docs` 由项目的公开接口和维护目标决定，不因安装 skill 自动新增全局 lint。

## 可执行示例

能独立运行的示例应补齐依赖、导入和输入；仅展示局部接口的示意应明确标识。不要将省略实现的片段称为可运行代码。

```rust
/// 将恰好两个字节解码为网络字节序的整数。
/// 长度不符时返回 None，不读取额外数据。
fn decode_u16(input: &[u8]) -> Option<u16> {
    Some(u16::from_be_bytes(input.try_into().ok()?))
}

assert_eq!(decode_u16(&[0x01, 0x02]), Some(258));
assert_eq!(decode_u16(&[0x01]), None);
```

对 library 的真实 API 示例使用 rustdoc，并用实际 crate 路径验证。二进制内部函数上的文档不意味着 `cargo test --doc` 自动覆盖它们，详见[第 5 章](chapter_05.md)。

## TODO 与长期记录

TODO 说明剩余问题、触发条件和影响；已有 issue 或工作项时链接它。对于需要跨版本跟进的问题，使用项目已有的跟踪方式，不强制每条 TODO 创建外部 issue。

把跨模块设计、取舍与长期边界放在一处权威文档，代码里链接必要背景。不要复制运行参数、测试结果或依赖版本到多份规范。

`unsafe` 注释需要解释具体操作的全部安全前提。例如 `copy_nonoverlapping` 不仅需要有效指针，还涉及范围、对齐、初始化和不重叠等要求；泛泛写“指针有效”不足以证明安全。

修改代码时同步核对相关注释和链接。文档陈述要区分源码事实、运行结果和仍未验证的性质。

参考：[rustdoc](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html)、[copy_nonoverlapping](https://doc.rust-lang.org/std/ptr/fn.copy_nonoverlapping.html)。
