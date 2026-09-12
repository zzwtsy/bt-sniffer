# 第 2 章：工具与检查

## 选择命令

先读项目 README、CI、`Cargo.toml`、`rust-toolchain.toml` 和已有脚本。检查当前工具链及依赖配置，不把通用命令覆盖到项目流程。

在单 crate、默认 features 的常见项目中，可以从以下检查开始，再按项目要求调整：

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

有 library 文档示例时，另行确认 `cargo test --doc --locked` 是否属于必需验证；`--all-targets` 不包含 doctest。workspace 根据任务选择 `-p` 或 `--workspace`。

- `--all-features` 启用所选 package 的所有 features，**不会解决互斥 feature 冲突**。仅在项目支持这种组合时使用，否则运行已支持的 feature 矩阵。
- `--locked` 要求解析结果不更改锁文件。失败时核对 manifest、锁文件与工具链；不要自动执行 `cargo update`，它可能升级无关依赖。
- Clippy 缺失时先确认项目指定的工具链，再为该工具链安装组件。不要为了安装 Clippy 顺手运行 `rustup update`。
- `--offline` 适用于依赖已经缓存的环境，缺包时应说明具体缺项；它不是已具备联网验证的证明。

## Lint 策略

保留现有 lint 策略。新增约束时验证它是否适用于项目，而不是一次性开启全部 `pedantic` 或 `nursery` 后修复整个仓库。

`redundant_clone`、`needless_collect`、`large_enum_variant` 等可以提供线索，但可用性和默认组别应按当前 Clippy 文档确认。静态提示不是性能测量。

接受一条警告前，检查是否有更清楚的实现。有合理例外时，在最小范围标记原因；支持 Rust 1.81 及以上的项目可用 `#[expect(...)]` 检测过期豁免，低 MSRV 项目可保留有说明的 `#[allow(...)]`。

Cargo 的 `[lints]`/`[workspace.lints]` 需要 Rust/Cargo 1.74 及以上。示意配置如下，只有项目选择采用时才加入：

```toml
[workspace.lints.clippy]
all = { level = "warn", priority = -1 }
large_enum_variant = "deny"
```

各成员的 `Cargo.toml` 中显式继承：

```toml
[lints]
workspace = true
```

lint 组设置较低优先级，单项规则才可以覆盖组；workspace 声明不会自动应用到所有成员。独立 package 则使用 `[lints.clippy]`，无需 workspace 层。

## 验证结论

记录具体命令和失败原因。工具链、缓存、socket 或 DNS 权限失败是环境证据；不能将它们归类为“检查通过”。检查通过后，只在新修改或未解决疑点出现时扩展验证。

参考：[Cargo features](https://doc.rust-lang.org/cargo/reference/features.html)、[Cargo lints](https://doc.rust-lang.org/cargo/reference/manifest.html#the-lints-section)、[Clippy](https://doc.rust-lang.org/clippy/usage.html)。
