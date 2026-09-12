# 第 4 章：错误契约

## 分类与恢复

错误类型由调用者需要做的决策决定，而不是由文件属于 binary 还是 library 单独决定。

- 调用者需要重试、跳过、暂停或区分协议违规时，保留枚举或其他稳定分类。
- `thiserror` 可减少 `Display`、`Error` 与转换实现的样板，但已有清晰的手写错误不必为形式统一迁移。
- 应用编排层不需要精细匹配时可以使用 `anyhow` 增加上下文；binary 内部也可以需要类型化错误。类型擦除会弱化编译期匹配，但不等于必然丢失错误链。
- 为 I/O、解析和存储错误增加操作上下文，并保留有用的原因链。不要把所有错误压成调用方无法区分的字符串，也不要在每层重复记录同一失败。
- 缺失属于正常业务结果时，允许 `Ok(None)`；不要仅因出现 `Option` 就创造错误。

```rust
#[derive(Debug, PartialEq)]
enum DecodeError {
    InvalidLength { actual: usize },
}

fn decode_id(bytes: &[u8]) -> Result<[u8; 20], DecodeError> {
    bytes.try_into().map_err(|_| DecodeError::InvalidLength {
        actual: bytes.len(),
    })
}

assert_eq!(decode_id(&[1; 20]), Ok([1; 20]));
assert_eq!(decode_id(&[1; 19]), Err(DecodeError::InvalidLength { actual: 19 }));
```

这里示例只演示分类；作为跨模块错误暴露时，按接口需要补充 `Display`、`std::error::Error` 和来源链。

## panic 与内部不变量

不可信网络输入、磁盘失败、队列关闭及正常取消通常都可能发生，应进入已定义的错误或退出路径。

测试可以使用 `unwrap/expect`。生产代码对已证明的不变量、编译期常量解析或明确的不可恢复失败策略，也可以有带原因的 `expect`/断言；审查时指出成立依据与失败影响。不能以“内部代码”作为免于论证的理由。

`todo!`、`unimplemented!`、`unreachable!` 都会在执行时 panic，不会自动证明分支不可达；未完成的生产路径不能靠这些宏通过验收。锁中毒后的 panic 或恢复同样应有明确策略。

## 传播、并发和部分成功

- 单纯传播用 `?`；需要增加上下文、清理或选择恢复路径时使用 `map_err`、`match` 等。
- `JoinHandle<Result<T, E>>` 有任务层 `JoinError` 与业务层 `E` 两种失败。不要只处理其中一层。
- 批处理若允许部分成功，应返回可识别的逐项结果或明确的部分成功类型；仅记录失败后返回 `Ok(成功项)` 会让调用者误判完整成功。
- 取消等待可能留下已执行的远端请求或已提交的存储操作。重试策略必须考虑未知结果、幂等性和去重标识。
- `Send + 'static` 来自任务和接口边界；`Sync` 只在共享或错误容器要求时添加。跨 `.await` 本身不要求所有错误都 `Send + Sync`。

错误测试优先验证分类及关联数据。只有错误文本本身是接口契约时才固定匹配文案，避免把内部措辞当作行为。

参考：[Error](https://doc.rust-lang.org/std/error/trait.Error.html)、[Result](https://doc.rust-lang.org/std/result/)。
