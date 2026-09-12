# KRPC 未排序字典兼容修复验证

日期：2026-09-12。此记录对应独立修复，不替代五阶段交付时的历史验证或任何公网长测报告。

## 触发与原因

服务器提供的 2026-09-12 16:32 UTC 抓包中，libtorrent 返回的 83 字节 ping 响应具有正确 transaction 和来源端口，但顶层键顺序为 `ip, r, t, y, v`；92 字节入站 ping 查询也存在乱序键。相同版本 bendy 0.6.1 对原始响应返回 `UnsortedKeys`，仅排序键后可以解码。

另一个引导节点的响应 transaction 匹配，但来源端口从请求目标 6881 变成 39499。这属于不同问题：现有完整 SocketAddr 匹配继续拒绝该响应，不放宽来源保护。

## 修复范围

- `net/udp/ordering.rs` 仅在 UDP 大小校验后组织容器并排序字典，再进入原有 bendy KRPC 类型解码。字符串和整数由 bendy 校验，原子编码不改写；未知扩展同样检查重复键和结构。
- 输入受原 UDP 数据报大小约束（默认 4 KiB），容器最多 64 层；临时树的节点数受输入长度约束，规范化输出与输入等长。不缓存远端数据，不增加依赖。
- 拒绝所有层级的重复键、非字符串键、非法整数/长度编码、截断和尾随数据。响应仍匹配 transaction、来源 IP/端口和预期身份。
- peer-wire、原始 info 校验、metadata 字节和 SQLite schema 未修改。适配层增加有界的临时分配；本次未测量公网吞吐或 RSS 变化。

## 验证结果

工具链：rustc 1.98.1 (48a229cea 2026-09-01)，x86_64-unknown-linux-gnu。

| 检查 | 结果 |
| --- | --- |
| 抓包回放及 UDP 模块 | 12 个测试通过；原始乱序查询/响应均正确解码 |
| 双栈 loopback dispatcher | IPv4、IPv6 引导成功并清空 pending；乱序入站 ping 获得正确回复 |
| 来源保护 | 同 IPv6 地址改端口响应被拒绝，原 transaction 保留供正确来源完成 |
| 默认回归 `cargo test --offline --locked --all-targets` | 273 通过，0 失败，3 ignored |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --offline --locked --all-targets --all-features -- -D warnings` | 通过 |
| `cargo build --release --offline --locked` | 通过 |
| Git 差异空白检查 | 通过 |
| 修复后公网验证、手动长测 | 未运行 |

最初受限沙箱的 UDP socket 测试因 EPERM 失败；随后在允许本地 socket 的环境重跑上述回归并通过。默认回归没有启动 ignored 长测。

源码清单 SHA-256（算法同 `src/acceptance.rs`）：`18e0ef3e6b5196325f8d13eaadd0037c20abd76e07ed0c34c21e76e49c476203`。

本机构建的 `target/release/bt-sniffer` SHA-256：`4bf623a23f83c97521eb779628808ec1ea4059ebd3e6fd7238d3f2125bf4e170`。其他工具链重新编译的二进制摘要可能不同。

## 公网复验

当前长测正常结束后再替换二进制；使用独立状态目录和原默认双栈配置复验。观察 IPv6 引导成功日志及 `validated_v6` 增长，若仍为零，再结合对应 transaction 的抓包定位。单次引导成功仅证明该族 DHT 互操作；持续产出、资源边界和正常收尾仍按独立长测标准验收。
