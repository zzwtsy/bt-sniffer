# bt-sniffer

Rust 编写的 BitTorrent 自动发现与原始 metadata 获取程序。默认运行双栈 DHT 服务，恢复持久化身份与路由联系人；`--sample --fetch` 开启主动发现、peer 查找、metadata 获取和 SQLite 入库闭环。

## 开发者从这里开始

熟悉基本 Rust 语法后，从[上手指南](docs/getting-started.md)开始：先运行一个临时数据库测试，再按完整链路图阅读代码，通过采样到入库、原始字节校验和关闭重启三个小实验理解行为。

## 运行

```sh
# 默认监听 0.0.0.0:6881 和 [::]:6881，并使用内置引导服务器。
cargo run --release

# 指定状态目录并开始采样；采集到的 info-hash 保存到 SQLite。
cargo run --release -- --state-dir ./state --sample

# 完整闭环：主动采样、合法宣布和历史 hash 共用持久化下载任务。
cargo run --release -- --state-dir ./state --sample --fetch

# 只消费历史 hash 和合法宣布，不主动采样。
cargo run --release -- --state-dir ./state --fetch

# 只进行本机测试，不使用公共引导；新目录中没有恢复联系人。
cargo run -- --state-dir ./local-state --ipv4-only \
  --listen-v4 127.0.0.1:6881 --allow-local --no-bootstrap

# 自定义引导列表将替换内置列表，可以重复指定参数。
cargo run --release -- --bootstrap router.example.org:6881

cargo run -- --help
```

默认状态目录为系统本地数据目录下的 `bt-sniffer`（Linux 通常为 `~/.local/share/bt-sniffer`，遵循 XDG 配置）。`--instance` 默认为 `main`；IPv4/IPv6 分别保存身份和路由表。一个状态目录只能由一个进程持有。

`--ipv4-only`、`--ipv6-only` 可以限制地址族。监听端口允许为 `0`，系统分配的实际端口会写入日志。默认 IPv6 因环境明确不支持而无法监听时，会警告并继续 IPv4；显式指定 IPv6 地址、权限不足或端口占用不会被忽略。

日志固定同时输出 stderr 和进程工作目录下的 `logs/bt-sniffer.YYYY-MM-DD.log`，两端均为文本，本程序 INFO、第三方 WARN。文件按 UTC 自然日轮转，最多保留 7 个匹配日志文件（不是 7 天，也不限制文件大小）；两端各用 4,096 条有界后台队列。文件无 ANSI，终端仅在 stderr 连接终端时启用颜色。不读取日志环境变量或 `.env`，不提供 `--log-format`。工作目录需要可写 `logs/`，不同进程必须使用不同工作目录；部署和字段契约见[日志说明](docs/logging.md)。正常收到 Ctrl-C 或 Unix SIGTERM 时会停止采集、保存最终路由快照、排空待保存结果并关闭数据库；清理共用 30 秒期限，失败返回非零退出码。运行时退出后最多再等待系统 DNS 线程 1 秒，这不代表底层 DNS 调用可以被取消。

## 网络与数据边界

- “正在监听”和“已有响应的邻居”均不等于公网入站可达。提供公网服务仍需正确配置防火墙、UDP 端口转发或 IPv6 入站规则。
- `--allow-local` 统一允许本地单播地址，但**不会自动关闭公共引导**。纯本机部署应同时使用 `--no-bootstrap` 和全新的状态目录；关闭引导仍会恢复旧目录中的联系人。
- 引导每族每轮最多 8 个地址、2 个在途查询、发包间隔至少 1 秒；失败退避 60 秒至 15 分钟，并保留用户 transaction 名额。DNS 结果和恢复联系人都需要经过地址策略及响应验证。
- KRPC 接收兼容未排序的字典键，包括未知扩展内部；仍拒绝重复键、非法原子编码、尾随数据及超过 64 层的容器。UDP 大小与入站配额在解码前检查，响应仍严格匹配 transaction、来源 IP/端口及预期身份；同 IP 改端口的响应不能完成原查询。这项兼容不用于 metadata。
- 存储写入失败会暂停所有节点的主动采样和自动下载，基础 DHT 服务继续运行。确认磁盘问题修复后，应正常退出并重新启动；程序不会自动更换身份或重建数据库。
- `--sample` 仍只采样 hash；`--fetch` 才会启动自动下载。仅收到 `get_peers` 查询不会创建采集任务，合法 `announce_peer` 才会生成被动发现事件。
- metadata 保存的是 BEP 9 返回的原始 `info` 字典字节，经过 SHA-1、完整 Bencode 字典及尾随数据检查；不解析名称、文件清单，不做种子字段语义校验，不导出 `.torrent`，不下载文件内容，也不宣称完整支持 v2。
- schema v1 自动事务迁移到 v2，保留已有数据。开启 `--fetch` 后分批补建历史任务；已成功的 hash 不再下载。升级后的数据库不应交给旧版程序使用。
- 宣布采集入口为有界、非阻塞通道；满载或暂停时丢弃通知并计数。DHT 的成功 ACK 不承诺采集任务已持久化。
- DHT 默认启用双栈共享速率保护；TCP 不限制总下载带宽。公网持续产出与长期运维仍需独立手动验收。

## 采集运行参数

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `--fetch-concurrency` | 4 | 并行 hash 任务数，允许 1～64 |
| `--fetch-max-active-jobs` | 10,000 | pending/running/retry_wait 总上限 |
| `--state-max-bytes` | 10 GiB | SQLite 与 WAL 的应用层软容量预算 |

以上参数显式指定时必须带 `--fetch`。`--sample --fetch` 新增 `--sample-backpressure {freshness,capacity}`，默认 `freshness`：每 5 秒检查到期队列，达到任务容量 10% 或最老等待 5 分钟即暂停主动采样；降至 2% 且等待不超过 1 分钟，连续 30 秒后恢复。默认容量对应 1,000/200；未到期退避不计入，宣布与历史补建仍继续，所以这不是等待时间硬上限。仅按硬容量暂停可用 `./bt-sniffer --sample --fetch --sample-backpressure capacity`，沿用原状态目录时加原 `--state-dir`。

任务满载仍暂停主动采样，新 hash 由事务拒绝，宣布入口仍允许刷新已接纳任务的 hints；存量任务继续执行，低于上限的 80% 后解除容量暂停，积压等其他原因也解除后恢复。状态容量每 5 秒检查，预留 64 MiB 清理空间；触及阈值后暂停采集，保留数据，正常重启后重新检查。检查间隔和在途写入可能导致超额，这不是物理磁盘硬配额。

每轮最多连接 8 个不同 peer；优先尝试最多 2 个新鲜宣布地址，再进行双栈迭代 `get_peers`，最后尝试查找结果及其余宣布地址。总期限 180 秒。新增 `get_peers` 全进程最多 10 次/秒，同 IP 最多 1 次/秒；同 IP 同时最多一条 metadata TCP 连接，Tokio semaphore 按公平队列授予许可，等待可以取消。

失败按 1、2、4、8、16 分钟（±20% 抖动）退避，第 6 轮失败后休眠。重复发现不绕过退避；休眠满 24 小时且再次发现后才重新激活。本地资源等待、完全未发包、无路由或取消时延期 60 秒，不增加失败次数；实际远端 I/O、协议或校验失败消耗失败轮次。任务与退避持久化，正常退出取消 TCP/UDP 工作，崩溃后恢复已领取但未完成的任务。

已到期任务按有新鲜合法宣布地址与其余任务两类，以 3∶1 的成功领取次数轮转，各类内部按 `due_at, hash` 排序；空类可借用名额，未来退避任务不会提前领取。

默认每 60 秒显示任务状态、采样 hash 观察量、宣布及丢弃数量、完成/失败轮次、失败类别、连接数和状态容量。同时输出区间/累计领取、RPC、连接、提交字节、远端失败与本地延期，以及固定桶的领取等待、查找、TCP 等待和任务耗时近似分位数。查找与单 peer 下载交叠，成功后取消剩余查找；每 hash 同时最多一个 TCP 尝试。领取等待桶覆盖至 24 小时，分位数输出 `upper_bound_ms`/`exceeds_ms` 和溢出数量；增加连接、握手、传输、校验阶段结果，以及四类 DHT 排队与限流原因。正常关闭补齐最后不足一分钟的统计。旧 generation 的迟到结果不计成功。详见 [任务与验收说明](docs/metadata-collection.md)。

## DHT 流量参数

以下参数独立于 `--fetch`，会话内的 IPv4/IPv6 和所有查询来源共用预算。

| 参数 | 默认值 | 单位与下限 |
| --- | ---: | --- |
| `--dht-query-rate` | 20 | 主动查询/秒，至少 10 |
| `--dht-inbound-rate` | 200 | 普通入站数据报/秒，至少 1 |
| `--dht-upload-bytes-per-sec` | 262,144 | UDP payload 字节/秒，至少 32,768 |

配额允许一秒额度突发。主动查询按采集、控制（引导/恢复/维护/显式查询）、采样、反向验证 5∶3∶1∶1 分配，取整余数归控制类；同 IP 最多 2 次/秒。原有采样冷却和更严格的 `get_peers` 间隔继续生效。发送字节预算中 1/8 给主动查询，7/8 给回复。

普通入站另受每 IP 5 包/秒限制；挂起 RPC 的来源保留全局 128 包/秒解码额度，身份、地址和 transaction 仍须匹配，伪装的 Query 重新通过普通配额。IP 表最多 10,000 项，空闲 60 秒可回收；满时拒绝新键。回复超额或 socket 暂不可写时直接丢弃并计数。

主动 RPC 在 dispatcher 有界队列等待，待发与在途共享 transaction 容量并预留用户名额；最多等 5 秒，未发包超时按本地等待处理。正常放弃未发送的采样意图按租约撤销预约；发送后取消、发送结果不确定或异常退出仍采用保守恢复。反向验证额外受共享每 IP 60 秒接纳冷却和“验证速率 × 5 秒”待发名额约束（默认 10），轮换端口或 Node ID 不能绕过；拒绝验证不影响已合法生成的回复。关闭继续共用 30 秒清理期限。

## 验证

修改 Rust 实现前参考[开发约束](docs/rust-development.md)，其中记录运行时、取消、存储与测试边界。

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
git diff --check
git diff --cached --check
```

测试使用 loopback 和临时数据库，DNS 测试注入解析结果，不访问公共 DHT。受限沙箱需要允许本机 UDP socket。

长时间验收需要显式执行，默认测试不会访问公网：

```sh
# 本机真实时间 30 分钟混合流量验收
cargo test --release collector::tests::sustained_mixed_loopback_30_minutes -- --ignored --nocapture

# 公网两小时观察，/tmp 保留独立状态；没有成功获取 metadata 将判定未通过
cargo test --release app::tests::public_collection_two_hours -- --ignored --nocapture
```

两项长测均支持 Ctrl-C 或 Unix SIGTERM 提前停止：先保存状态、关闭数据库，再复核已采集数据；提前停止会以非零状态退出，表示本次验收未完成。每次在独立临时状态目录写入 JSON 报告，包含源码 SHA-256 清单、工具链、配置、时长、统计和通过/失败/中止/环境阻塞状态。公网报告分别记录各地址族实际验证的 DHT 响应数；schema v2 不存 metadata 来源地址族，因此不虚构每族下载成功数。正常关闭后 WAL/SHM 通常会自动清理，独立状态目录仍保留用于审计。SIGKILL 等强制终止无法执行收尾，遗留的 WAL 不应单独删除。

独立短的 Release 调度比较（固定 SQLite 输入与模拟服务时长，不能推断公网吞吐）：

```sh
cargo test --release scheduling_release_comparison -- --ignored --nocapture
```

此前五阶段可靠性实施与验证记录见[可靠性加固交付记录](docs/reliability-implementation.md)。长测不随实施收尾自动启动。

本轮采集效率优化的本机比较、回归范围和未运行的手动验收见[独立交付报告](docs/reports/collection-efficiency-2026-09-12.md)。
