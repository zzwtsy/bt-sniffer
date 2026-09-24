# bt-sniffer

Rust 编写的 BitTorrent DHT 服务、hash 发现与原始 metadata 采集程序。支持 IPv4/IPv6，使用 Tokio current_thread 和 SQLite 专用线程保存身份、联系人、采集任务及结果。

默认提供 DHT 服务；主动采样和 metadata 获取分别启用。metadata 以原始 info 字节进行 v1 SHA-1 或 v2 SHA-256 前缀匹配及完整字典校验，并可通过本机只读前端搜索已保存的名称与文件路径；支持 v1、v2／hybrid 目录与 BEP 47 文件属性；不下载文件内容或 piece layers，不提供 torrent 导出或完整 BEP 52 内容验证。

## 开发入口

- [源码目录设计](docs/architecture/repository-layout.md)：模块、依赖、测试和新增功能归属。
- [文档目录设计](docs/architecture/documentation-layout.md)：事实维护位置与 AI 阅读路径。
- [开发流程](docs/development/workflow.md)与[验证指南](docs/development/validation.md)：准备环境、实施、检查和交付。
- [完整导航](docs/README.md)与[AI 协作约定](AGENTS.md)：按任务读取。

## 运行入口

安装仓库固定 Rust 后，在仓库根查看 CLI；命令构建写入 target，help 不创建运行资源，退出 0 表示查看成功，可 Ctrl-C 中止构建：

```sh
cargo run --locked -- --help
```

本机隔离启动、完整参数和停止方法见[运行指南](docs/operations/running.md)，观察与数据库复核见[诊断指南](docs/operations/diagnostics.md)。公网和长时间验收需单独安排；日志可能丢弃，本地测试不证明公网吞吐或可达性。
