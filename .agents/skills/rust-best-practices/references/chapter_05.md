# 第 5 章：行为测试

## 一个场景，多个必要断言

测试名称描述触发条件与可观察结果。每个测试聚焦一个行为场景，而不是机械限制为一个 `assert`；一次失败写入可能同时需要证明返回错误、数据未修改、任务仍可恢复。

重复输入可使用表驱动循环并报告当前案例，无需仅为减少几行代码引入参数化测试库。不同失败原因或状态转换则适合独立命名。

```rust
fn decode_pair(input: &[u8]) -> Option<(u8, u8)> {
    match input {
        [left, right] => Some((*left, *right)),
        _ => None,
    }
}

let input = [3, 5];
let pair = decode_pair(&input).expect("恰好两个字节应解析成功");
assert_eq!(pair, (3, 5));
assert_eq!(input, [3, 5]);
for input in [&[][..], &[3][..], &[3, 5, 8][..]] {
    assert_eq!(decode_pair(input), None, "错误输入: {input:?}");
}
```

共享昂贵夹具和协议辅助代码时，让测试动作、预期及失败上下文容易定位。测试辅助函数也可能有缺陷；复杂辅助逻辑应简化或单独验证。

## 选择测试边界

| 需要证明的行为 | 常见位置 |
| --- | --- |
| 模块内部不变量、私有解析和状态转换 | 模块内 `#[cfg(test)]` 或其子模块 |
| library 的外部 API 组合 | `tests/` 下的集成测试 |
| 可执行程序的参数、退出码和输出 | 子进程测试，或已有可注入的应用入口 |
| 对外 API 示例可编译并保持约定 | library 的 rustdoc 示例 |

binary 可以保留内部模块测试，不必仅为外部集成测试增加公共 `lib.rs`。`pub(crate)` 可见性不能被外部集成测试直接访问。

`cargo test --all-targets` 不包含 doctest；有 library 文档测试时另跑 `cargo test --doc`，使用 nextest 也应单独核对文档测试流程。独立 Markdown 示例可以用 `rustdoc --edition=2024 --test 文件.md` 检查。

## 异步与存储

- 用 oneshot、通道、屏障或状态通知表达“已经开始/已经完成”，不要用固定 sleep 猜测任务进度。
- 纯 Tokio 定时逻辑可用暂停时钟；线程、文件、socket 和系统 DNS 不会因虚拟时间推进而完成。
- 每个可能挂起的场景要有合理终止条件。观察取消后的资源释放、队列排空、任务回收与持久化状态，不能只断言 token 已取消。
- 临时数据库应覆盖事务失败、旧领取结果、恢复与重试等有实际契约的路径；不要只模拟实现里的 SQL 调用次数。
- 隔离测试状态。公网、长时间和外部服务检查应与默认测试区分，按用户任务或项目验收要求执行。

## 快照与失败策略

快照适合稳定、可审阅的大段输出。随机标识、时间戳和内部调试输出应明确处理，更新快照前理解变化；不能为了测试变绿批量接受差异。

`#[should_panic]` 只用于已定义的 panic 契约，尽量约束原因。`#[ignore]` 表示默认跳过的独立验证，不是未完成功能已通过测试。

参考：[Rust 测试组织](https://doc.rust-lang.org/book/ch11-03-test-organization.html)、[rustdoc 测试](https://doc.rust-lang.org/rustdoc/write-documentation/documentation-tests.html)。
