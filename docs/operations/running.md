# 运行指南

源码入口：[CLI](../../src/app/config.rs)、[socket](../../src/app/sockets.rs)、[应用](../../src/app/mod.rs)。验证入口：`app::config::tests`、`app::tests`。开发环境见[开发流程](../development/workflow.md)。

## 最短本地启动

先在仓库根构建，需固定 Rust 和依赖缓存；构建只写 target，成功生成二进制，Ctrl-C 可停止：

```sh
cargo build --locked
./target/debug/bt-sniffer --help
```

help/version 不创建状态、日志或 socket。以下示例使用全新临时工作目录与状态，避免恢复任何公网联系人；需要本机 IPv4 UDP 权限。它只监听本机并写临时目录，不主动采样：

```sh
BT_SNIFFER_BIN="$PWD/target/debug/bt-sniffer"
BT_SNIFFER_RUN=$(mktemp -d)
cd "$BT_SNIFFER_RUN"
"$BT_SNIFFER_BIN" --state-dir "$BT_SNIFFER_RUN/state" --ipv4-only --listen-v4 127.0.0.1:0 --no-bootstrap --allow-local
```

成功判据是进程持续运行且状态中出现监听节点，不要求邻居数量增加。Ctrl-C 发起正常关闭，等待进程退出；示例不自动删除状态与日志。工作目录切换后，其他仓库命令应回仓库根执行。

## 参数

| 参数 | 默认值 | 约束与作用 |
| --- | --- | --- |
| --state-dir | 操作系统 data_local_dir 下 bt-sniffer | 状态目录；无法推导时必须显式指定 |
| --monitor-listen | 关闭 | loopback SocketAddr，例如 127.0.0.1:3001 或 [::1]:3001；允许端口 0，拒绝非 loopback |
| --instance | main | 1 至 128 UTF-8 字节；只参与身份键 |
| --listen-v4 | 0.0.0.0:6881 | IPv4 SocketAddr，端口 0 可由系统分配；与 ipv6-only 冲突 |
| --listen-v6 | [::]:6881 | IPv6 SocketAddr，端口 0 可用；与 ipv4-only 冲突 |
| --ipv4-only / --ipv6-only | 均关闭 | 互斥，只启用一种地址族 |
| --bootstrap | 内置三项 | 可重复 HOST:非零端口，替换默认列表；IPv6 用方括号，与 no-bootstrap 冲突 |
| --no-bootstrap | 关闭 | 禁止引导列表，仍恢复并验证磁盘联系人 |
| --sample | 关闭 | 主动 BEP 51 采样 |
| --fetch | 关闭 | 获取历史 hash 和合法 announce 对应 metadata |
| --sample-backpressure | freshness | freshness 或 capacity；可单独指定，仅 sample 与 fetch 同时开启时采用该选择，否则有效策略为 capacity |
| --fetch-concurrency | 4 | 1 至 64，显式设置要求 fetch；双栈共享 worker 上限 |
| --fetch-max-active-jobs | 10000 | 1 至 1000000，显式设置要求 fetch；不限制全部历史任务数 |
| --state-max-bytes | 10737418240（10 GiB） | 最小 134217728（128 MiB），显式设置要求 fetch；状态接纳预算 |
| --dht-query-rate | 20 | 最小 10，主动查询包/秒 |
| --dht-inbound-rate | 200 | 最小 1，普通入站包/秒 |
| --dht-upload-bytes-per-sec | 262144 | 最小 32768，UDP payload 字节/秒 |
| --allow-local | 关闭 | 允许本地单播与 loopback，仍校验地址；不会自动禁用引导 |
| --help / --version | 无 | 显示说明并退出，不启动应用资源 |

默认引导为 `dht.libtorrent.org:25401`、`router.bittorrent.com:6881`、`dht.transmissionbt.com:6881`。默认监听 IPv6 只有在系统明确不支持时可退到 IPv4；显式 IPv6、权限失败或端口占用不会静默降级。

## 目录与部署

状态包含 state.sqlite3、SQLite 伴随文件及 instance.lock，单目录独占。日志在进程工作目录 logs 下，详细过滤与轮转见[日志机制](../domains/logging.md)。多实例须使用不同状态目录和工作目录，不能只改 instance。

面向公网运行需要明确授权、允许的 UDP 出入站以及 fetch 所需 TCP 出站；DNS 引导需要解析能力。启用 sample 与 fetch 会主动发现并采集，产生持久化数据和公网流量。部署使用独立非特权用户、固定 WorkingDirectory 和显式 state-dir，确保目录可写。进程管理器发送 SIGTERM 后应留出会话关闭与日志收尾时间，不把立即强杀当正常停止。

退出应确认进程结束和关闭结果；看到邻居响应不证明公网入站可达，看到 metadata 日志不证明数据库完整。故障与数据库复核见[诊断指南](diagnostics.md)。

## 本机监控与 SSH 转发

在上述全新临时目录的本机启动命令后添加 `--monitor-listen 127.0.0.1:3001`。监听成功后可在另一终端读取；操作不修改任务：

```sh
curl http://127.0.0.1:3001/api/v1/snapshot
curl -N http://127.0.0.1:3001/api/v1/stream
```

snapshot 返回 JSON，stream 首先返回 hello；Ctrl-C 结束 curl。程序本身仍通过信号正常关闭。端口 0 的实际地址由启动日志报告。前端开发服务器将 `/api` 代理至此地址，浏览器通过同源请求连接。

远程机器已启动监控且有 SSH 访问权限时，在本机建立隧道；命令只转发端口，不启动采集：

```sh
ssh -N -L 3001:127.0.0.1:3001 user@host
```

本机仍访问相同 URL，Ctrl-C 结束隧道。转发不需要开放远程监控端口；三条接口、错误和重连规则见[观测接口](../domains/monitoring.md)。旧监控 API 不重定向，GET 访问返回 404。

## 观测网页

后端启用监控后，可使用独立[前端](../../web/README.md)查看当前状态、有限事件窗口，并在 `/torrents` 查询本地已保存的 v1 metadata 目录。开发服务器和静态反向代理仅监听 loopback，浏览器始终请求同源 `/api`。生产构建使用[代理示例](../../web/nginx.conf)，远端查看通过 SSH 转发前端端口。冻结显示仅属于流程总览且不影响采集；网页不提供任务控制、按需联网下载或 metadata/torrent 导出。
