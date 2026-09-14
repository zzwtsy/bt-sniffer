# tracing 关键用法

以下为局部示意；采用时核对项目类型、版本及 feature，不直接复制为完整程序。

## 参数白名单

```rust
#[tracing::instrument(
    skip_all,
    fields(attempt_id = attempt_id, result = tracing::field::Empty)
)]
```

属性放在拥有 attempt_id 参数的业务函数上。结束时可调用
`tracing::Span::current().record("result", "committed")`，但只能在 Applied 确认后标记 committed。
默认 instrument 记录参数，err 默认输出 ERROR，ret 可能输出大对象，不无差别启用。

## 异步上下文

```rust
use tracing::Instrument;
let span = tracing::debug_span!("peer_attempt");
let handle = tokio::spawn(work().instrument(span.or_current()));
let result = handle.await?;
```

or_current 在子 span 被过滤时保留父上下文，不创建本来不存在的父 span。
只继承当前 span 可用 in_current_span。任务所有者仍需处理 JoinHandle 和业务错误。
同步线程可在 closure 内使用 span.in_scope；enter guard 不跨 await。

## 测试 subscriber

```rust
use tracing::instrument::WithSubscriber;
async {
    // 受测异步流程。
}
.with_subscriber(subscriber)
.await;
```

with_default 只覆盖同步 closure，不能绑定返回 future 的后续 poll。
子任务需要局部 subscriber 时，在 spawn 前用 with_current_subscriber；
subscriber 的传播不能代替业务 span 的传播。

## 官方来源

- [instrument](https://docs.rs/tracing/0.1.44/tracing/attr.instrument.html)：参数、级别、err/ret。
- [Instrument](https://docs.rs/tracing/0.1.44/tracing/trait.Instrument.html)：future poll 和上下文。
- [Layer](https://docs.rs/tracing-subscriber/0.3.23/tracing_subscriber/layer/index.html)：全局与每层过滤。
- [WithSubscriber](https://docs.rs/tracing/0.1.44/tracing/instrument/trait.WithSubscriber.html)：异步作用域。
- [NonBlocking](https://docs.rs/tracing-appender/0.2.5/tracing_appender/non_blocking/index.html)：队列、丢弃与 guard。

链接对应编写时核对的依赖；升级后以锁文件及匹配版本的官方文档为准。
