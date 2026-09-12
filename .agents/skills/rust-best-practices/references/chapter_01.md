# 第 1 章：编码与所有权

## 参数与数据生命周期

先判断调用者是否还需要值、被调用者是否要保留值，以及是否跨任务边界。

| 需求 | 常用表达 |
| --- | --- |
| 只读字符串或连续元素 | `&str`、`&[T]` |
| 修改调用者拥有的值 | `&mut T` |
| 转移、保留或消耗值 | `T`、`String`、`Vec<T>` |
| 多个所有者共享同一数据 | 按线程边界选择 `Rc<T>` 或 `Arc<T>` |
| 通常借用，特定分支需要构造新值 | 有明确收益时使用 `Cow` |

`Arc::clone` 增加引用计数，`Vec::clone` 通常复制元素；不要把它们算作相同成本。小型、语义上可复制的值可以实现 `Copy`，但 24 字节或几个机器字不是通用界线。是否公开 `Copy` 也影响未来能否添加有所有权的字段。

`Cow` 应表达真实的借用/拥有分支，不用于掩盖尚未想清楚的 API：

```rust
use std::borrow::Cow;

fn normalize(value: &str) -> Cow<'_, str> {
    if value.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(value.to_ascii_lowercase())
    } else {
        Cow::Borrowed(value)
    }
}

assert!(matches!(normalize("rust"), Cow::Borrowed(_)));
assert_eq!(normalize("Rust"), "rust");
```

## Option、Result 与迭代

- 缺失是正常状态时使用 `Option`；调用者需要失败原因时使用 `Result`。
- `let ... else` 适合提前返回、跳过或退出；`?` 适合传播错误。不要为了减少缩进丢掉诊断信息。
- `unwrap_or` 立即求值默认值；昂贵或有副作用的回退通常用 `unwrap_or_else`。
- `.iter()` 借用，`.iter_mut()` 可变借用，`.into_iter()` 的语义取决于接收者类型；对拥有的 `Vec<T>` 会消耗容器并移动元素，不会自动克隆。元素实现 `Copy` 不是禁用 `.into_iter()` 的理由。
- 使用有副作用、提前退出或协议步骤的循环时，`for` 往往更清楚；转换管道可以用迭代器。`for` 本身也使用迭代器协议，性能优劣需要测量。
- 将 `.filter()` 放在 `.cloned()` 前可减少不必要的复制。是否收集到容器取决于调用方是否需要缓存、重复遍历或持有结果。
- 为有游标状态的 `Iterator` 实现 `Copy` 容易让推进只作用于隐式副本；优先区分可复制的数据与遍历器，必要时用 `IntoIterator` 创建独立游标。

## 提取函数与组织代码

提取函数的依据是共享同一业务知识、隐藏复杂度或给一段操作命名。出现三次可以作为提醒，不是硬门槛；一处调用也可能值得提取，两处也可能共享必须一致的规则。

相似语句若代表不同业务决策，保留独立实现。抽象需要不断增加模式参数时，重新检查调用者是否真的共享契约。

测试夹具可以共享；动作和结果应容易定位。允许封装复杂协议交互或共同不变量，但让失败信息暴露实际输入及预期，不为消除几行重复制造多层跳转。

imports 的排序与分组遵循仓库的 rustfmt 配置。不要为风格偏好引入 nightly 工具链或重排无关文件。注释范围见[第 8 章](chapter_08.md)。

参考：[所有权](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html)、[Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html)、[IntoIterator](https://doc.rust-lang.org/std/iter/trait.IntoIterator.html)。
