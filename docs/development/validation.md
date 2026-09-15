# 检查与验证

公开入口是 `python3 scripts/check.py <范围...>`，可从任意工作目录用脚本绝对路径调用。无参数仅显示帮助；不接受任意命令。源码依据：[check.py](../../scripts/check.py)、[环境检查](../../scripts/check_environment.py)、[CI](../../.github/workflows/check.yml)。工具准备见[开发流程](workflow.md)。

## 选择范围

| 变更 | 范围 | 执行内容 |
| --- | --- | --- |
| Rust 行为 | rust | 双栈 loopback 前提、fmt、全部目标 check、严格 Clippy、默认 Rust 测试 |
| Markdown、文档和 skills | docs | Markdown 格式、本地链接及锚点、检查工具专项 Python 回归 |
| Python 工具 | tools | 全部 scripts/tests Python 测试 |
| 独立异步示例 | examples | 独立 workspace 的 fmt、Clippy、测试 |
| 完整本地检查与 CI | all | 上述并集，重复阶段只执行一次 |

跨范围取并集，例如文档与检查工具一起修改：

```sh
python3 scripts/check.py docs tools
```

前提是已装固定工具；命令写 `target/checks/` 日志和报告、可能写编译缓存，不改源码和文档。成功判据为退出 0 且所有选定阶段通过。首次失败后后续阶段标记未运行，Ctrl-C 或 SIGTERM 会中止执行，向当前子进程组转发信号并等待最多 5 秒，再强制终止和回收。

只改 Rust 注释时执行 `cargo fmt --all -- --check`，不因此重复完整业务回归。文档检查纳入未忽略的新文件，跳过工作区删除；本地 skill 入口检查格式，参考材料继续检查链接。空输入、非法链接、格式和子命令失败都不能算通过。

## 受影响行为测试

先用源码中的精确测试名限定行为，避免 substring 意外多跑。以下命令在仓库根执行，需要固定 Rust 和依赖缓存；只进行本地测试，写编译缓存，不启动公网。成功应显示选中的 1 个测试通过，0 tests 不算验证；Ctrl-C 停止后记录未完成。

```sh
cargo test --locked app::config::tests::defaults_and_overrides -- --exact
cargo test --locked collection::jobs::tests::dedup_capacity_recovery_and_stale_generation -- --exact
cargo test --locked app::tests::graceful_shutdown_releases_state_directory -- --exact
cargo test --locked app::logging::event_tests::json_events_preserve_types_filters_overflow_and_interval_reset -- --exact
```

专题中 `storage::tests` 等模块名是定位前缀，不是精确测试名。可用 `cargo test --locked -- --list` 查看当前实际名字；该命令编译并列举，不运行测试。socket 测试需要本机 TCP/UDP loopback，双栈不可用或沙箱禁止绑定时应报告环境限制，不通过删测试或修改监听逻辑绕过。

## 报告与证据边界

每次运行保存独立 `target/checks/<时间>-<随机标识>/`，内有阶段完整日志和 `result.json`。报告记录范围、起止时间、实际命令、退出码、状态和日志路径；终端只展示摘要与最多 8 KiB 失败末尾。报告写入失败会使整体失败。

状态为 passed、failed、blocked、interrupted、not_run。缺工具或已识别版本问题为环境阻塞，不把任意命令失败猜成环境问题。退出码：成功 0，检查失败 1，参数或已识别环境问题 2，SIGINT 130，SIGTERM 143。报告类型是 development-check，不复用产品验收 schema；HEAD 不能完整表示未提交输入。

默认测试不执行 ignored 验收。`app::tests::public_collection_two_hours` 涉及公网长时采集，`collection::tests::acceptance::sustained_mixed_loopback_30_minutes` 是长时本机验收，均需单独授权和前提，不作为日常检查。产品证据工具见[诊断](../operations/diagnostics.md)。测试存在、实际运行通过、公网性能成立是三件事。

CI 保留固定工具准备，调用 all，失败上传该次检查证据并维持失败状态。不要在 workflow 再复制一套检查清单。
