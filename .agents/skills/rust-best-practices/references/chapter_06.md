# 第 6 章：分发与抽象

## 按契约选择

| 需求 | 可选方式 | 需要权衡 |
| --- | --- | --- |
| 调用者知道具体类型、热路径需要内联 | 泛型或 `impl Trait` | 单态化的编译量及代码体积 |
| 运行时选择实现、隐藏具体类型、降低泛型传播 | `&dyn Trait`、`Box<dyn Trait>` | 间接调用、接口约束，拥有值时可能分配 |
| 变体有限且由本模块维护 | `enum` | 新增变体时的匹配与依赖关系 |

异构集合只是动态分发的一种用途。接口隔离、运行时配置和控制编译体积同样可以是合理依据；不要为了消除 `dyn` 把泛型扩散到整个应用。

```rust
use std::io::{self, Write};

fn emit(writer: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)
}

let mut output = Vec::new();
emit(&mut output, b"ok").unwrap();
assert_eq!(output, b"ok");
```

这里借用已有 writer，无需为动态分发装箱。是否改成泛型应由接口需求和测量决定。

## 装箱与共享

- `Box` 表达拥有堆上值；`&dyn Trait` 只借用。动态分发不必然分配，装箱也不必然使用动态分发。
- 内部结构可为递归、稳定大小或运行时多态合理使用 `Box`；“只能在 API 边界装箱”不是 Rust 约束。
- 需要共享所有权时再考虑 `Arc`，并核对其内部类型的 `Send`/`Sync` 条件，见[第 9 章](chapter_09.md)。
- 热路径测量间接调用成本，同时观察二进制大小与指令缓存；不要把“泛型必然更快”写成无条件结论。

## Dyn compatibility

创建 trait object 前按当前 Rust Reference 核对完整的 dyn compatibility 规则。分发方法的泛型参数、返回 `Self`、关联类型及 receiver 都可能限制对象使用。

某些方法可通过 `where Self: Sized` 排除在动态分发之外，不意味着整个 trait 一定不能作为对象。不要用简短的 receiver 清单替代编译器检查。

原生 `async fn` in trait 自 Rust 1.75 可用于静态分发，但这类方法不能直接通过 `dyn Trait` 分发。确需动态分发时可选择显式装箱 future 或 `async-trait`，并说明分配、生命周期和 `Send` 要求；不要为普通 async 函数预装宏库。

参考：[dyn compatibility](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility)、[trait objects](https://doc.rust-lang.org/reference/types/trait-object.html)。
