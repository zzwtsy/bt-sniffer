# bt-sniffer 文档导航

开发先定位负责模块，再读对应契约和测试。新任务的常驻规则见[根 AGENTS](../AGENTS.md)。

| 要解决的问题 | 入口 |
| --- | --- |
| 功能放在哪里、测试归谁 | [源码目录设计](architecture/repository-layout.md) |
| 文档放在哪里、AI 怎样按需阅读 | [文档目录设计](architecture/documentation-layout.md) |
| 系统如何协作 | [架构概览](architecture/overview.md) |
| 谁负责取消、故障和退出确认 | [生命周期](architecture/lifecycle.md) |
| 准备环境、实施与交接 | [开发流程](development/workflow.md) |
| 选择检查、解释结果 | [验证指南](development/validation.md) |
| 编写或修正文档 | [文档任务入口](AGENTS.md)、[写作规则](development/documentation.md) |
| 修改 DHT 协议和流量 | [DHT](domains/dht.md) |
| 修改接纳、采集与重试 | [采集](domains/collection.md) |
| 接入发现流程可视化与只读 API | [观测接口](domains/monitoring.md) |
| 修改数据库操作和恢复 | [存储](domains/storage.md) |
| 修改日志输出或字段 | [日志机制](domains/logging.md)、[事件参考](domains/log-events.md) |
| 启动程序与理解参数 | [运行指南](operations/running.md) |
| 排查故障与验证数据库 | [诊断指南](operations/diagnostics.md) |

专题提供源码与测试入口；测试存在不代表本次已经运行，操作示例不代表已获得公网验收授权。
