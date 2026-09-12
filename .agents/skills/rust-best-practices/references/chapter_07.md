# 第 7 章：类型状态

## 适用边界

类型状态适合让调用者只能执行合法操作顺序，例如构建配置后才能启动、完成握手后才能发请求。它是可选的 API 设计工具，不是所有 Rust 状态机的默认形式。

当状态来自数据库、网络或需要运行时枚举时，`enum` 与显式状态转换往往更容易恢复和维护。类型状态也无法替代外部输入校验、失败恢复或持久化事务。

## 让状态携带所需数据

与其让所有状态都携带 `Option` 再依赖 `unreachable!`，可以让已配置状态直接拥有必需的数据：

```rust
struct Missing;
struct Configured { endpoint: String }
struct Client<State> { state: State }

impl Client<Missing> {
    fn new() -> Self { Self { state: Missing } }

    fn configure(self, endpoint: String) -> Client<Configured> {
        Client { state: Configured { endpoint } }
    }
}

impl Client<Configured> {
    fn endpoint(&self) -> &str { &self.state.endpoint }
}

let client = Client::new().configure("127.0.0.1:6881".into());
assert_eq!(client.endpoint(), "127.0.0.1:6881");
```

这个示例仅保证先提供配置再读取；它没有宣称字符串已经通过地址校验。生产接口若需要校验，应在转换时返回 `Result` 或接收已验证的地址类型。

## 维护成本

- 保持构造字段的可见性受控，否则调用者可以伪造状态。
- 状态失败时考虑是否需要返回原对象以便重试，不能无意丢失连接或资源。
- 泛型状态组合会增加 API 与测试数量。先证明它减少真实误用，再决定是否引入。
- 运行时不同结果可使用 `enum` 或其他安全抽象表达，不应仅为绕过状态类型差异引入 `unsafe`。
- 标记不拥有实际状态数据时才考虑 `PhantomData`，并了解它对方差、drop 检查和自动 trait 的影响。

参考：[PhantomData](https://doc.rust-lang.org/std/marker/struct.PhantomData.html)。
