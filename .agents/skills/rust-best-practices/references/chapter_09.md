# 第 9 章：指针与线程安全

## Send 与 Sync 分别判断

`Send` 表示值可以跨线程转移；`Sync` 表示共享引用可以跨线程使用，即 `T: Sync` 等价于 `&T: Send`。它们不会自动保证应用层操作顺序或事务一致性。

下表针对标准库类型及默认 allocator；泛型边界需要分别满足：

| 类型 | 何时 Send | 何时 Sync |
| --- | --- | --- |
| `&T` | `T: Sync` | `T: Sync` |
| `&mut T` | `T: Send` | `T: Sync` |
| `Box<T>` | `T: Send` | `T: Sync` |
| `Rc<T>` | 不实现 | 不实现 |
| `Arc<T>` | `T: Send + Sync` | `T: Send + Sync` |
| `Cell<T>`、`RefCell<T>`、`OnceCell<T>` | `T: Send` | 不实现 |
| `Mutex<T>` | `T: Send` | `T: Send` |
| `RwLock<T>` | `T: Send` | `T: Send + Sync` |
| `OnceLock<T>` | `T: Send` | `T: Send + Sync` |
| `*const T`、`*mut T` | 不自动实现 | 不自动实现 |

`&mut T` 可以被独占地移交给另一线程；共享一个 `&mut T` 的引用不允许再借此任意修改 `T`。不要把“独占可变”误解为“不支持 Send”。

```rust
use std::{cell::Cell, sync::{Arc, Mutex}};
fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

assert_send::<&mut u32>();
assert_sync::<&mut u32>();
assert_send::<Cell<u32>>();
assert_send::<Arc<Mutex<Cell<u32>>>>();
assert_sync::<Arc<Mutex<Cell<u32>>>>();
```

`Arc` 只保护引用计数，不会给内部值增加同步能力。以下代码应编译失败：

```compile_fail
use std::{cell::Cell, sync::Arc};
fn assert_send<T: Send>() {}
assert_send::<Arc<Cell<u32>>>();
```

## 指针与可变性选择

- `Box` 表示单一所有者；`Rc` 和 `Arc` 表示多个所有者。是否共享由实际生命周期决定，不是越多包装越安全。
- `Cell` 的部分方法要求 `Copy`，但 `Cell<T>` 本身可容纳非 `Copy` 类型。`RefCell` 用运行时借用检查约束访问，违反规则会 panic。
- `Mutex` 与 `RwLock` 的选择需考虑竞争、临界区和调度；读多并不保证 `RwLock` 更快。Tokio 锁与标准库锁的 `.await` 边界应按异步专项指导判断。
- `LazyCell`、`LazyLock` 等还包含初始化闭包类型；核对闭包的自动 trait 条件，不能直接套用只含 `T` 的简表。
- 原始指针的解引用和自定义 `unsafe impl Send/Sync` 都需要单独证明有效期、别名与同步约束，不能仅为绕过编译错误添加实现。

参考：[Send](https://doc.rust-lang.org/std/marker/trait.Send.html)、[Sync](https://doc.rust-lang.org/std/marker/trait.Sync.html)、[Arc](https://doc.rust-lang.org/std/sync/struct.Arc.html)、[Mutex](https://doc.rust-lang.org/std/sync/struct.Mutex.html)。
