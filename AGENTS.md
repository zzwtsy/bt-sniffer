# bt-sniffer 协作约定

使用简体中文，保留代码、命令和专有名词。面向掌握基础 Rust 的维护者，以正确性、安全和明确资源约束为前提选择可读、直接的实现。性能结论需证据；不为假设用途新增配置、trait、插件或预留 API。

## 范围与授权

开始核对完整 Git 状态及相关暂存、未暂存 diff，保护用户未跟踪内容。审查任务不改文件；实现按授权范围执行，小修复不扩展全仓重构。行为整理与删除能力、配置变化或并发策略变化分开说明。

未经授权不提交、推送、部署、发布或运行公网和长时间验收。不以清理为由删除用户数据和证据。普通文档不覆盖更高优先级指令。可以从代码、配置和工具验证的事实先自行核对，不能把推断写成已确认结论。

## 任务路由

| 任务 | 阅读顺序 |
| --- | --- |
| 定位功能或新增模块 | [源码目录设计](docs/architecture/repository-layout.md) → 对应业务专题 |
| Rust 实现或审查 | [Rust 主 skill](.agents/skills/rust-readable-apps/SKILL.md) → 受影响专题、调用者与测试 |
| 异步、取消或关闭 | Rust 主 skill → [异步专项](.agents/skills/rust-async-patterns/SKILL.md) → [生命周期](docs/architecture/lifecycle.md)与相关专题 |
| 日志或消费者修改 | [日志专项](.agents/skills/bt-sniffer-logging/SKILL.md) → [日志机制](docs/domains/logging.md)，涉及字段再读[事件参考](docs/domains/log-events.md) |
| 文档修改 | [docs/AGENTS](docs/AGENTS.md) → 文档目录设计与写作规则 → 对应源码 |
| Python、检查工具或 CI | [开发流程](docs/development/workflow.md)、[验证指南](docs/development/validation.md) → 脚本调用者和离线测试 |
| 运行或排障 | [运行](docs/operations/running.md)或[诊断](docs/operations/diagnostics.md) → 必要业务定义 |
| Skill 维护 | [维护约定](docs/development/workflow.md#skills-维护) → 目标元数据、调用策略及引用 |

仅对具体语言机制疑问读取 rust-best-practices，不加载第二套默认流程；会话残留或事实完整性问题按需使用 trim-authoring-residue。缺失材料如实说明，不假称已读取。完整导航见 [docs](docs/README.md)，无需每次读取所有材料。

## 关键不变量

- DHT 响应匹配 transaction、来源与身份；原始 info 字节的 hash 和完整字典验证不被兼容解析替代。
- SQLite 是采集状态事实，generation 拒绝旧领取；metadata、任务成功和提示清理在同一事务，只有 Applied 计提交成功。
- 并发、队列、载荷与时间有上限；取消通知不等于退出确认。任务、数据库操作和进程有所有者与收尾。
- app 负责组装和生命周期；DHT 不依赖采集策略；两个具体 Store 共用数据库线程和预算。

详细契约按路由进入业务文档，本入口不重复维护参数、事件或状态表。

## 交付要求

明确可观察的验收条件，按[流程](docs/development/workflow.md)实施与交接，按[检查选择](docs/development/validation.md)执行相关范围。使用现有 feature、锁文件和工具链，不用删除测试、全局 allow 或关闭检查换取通过。

复读完整 diff 与关键失败路径。报告实际修改、命令和结果、证据位置、未运行范围及剩余风险；审查结论给位置、触发条件、影响和证据，区分必须修复、建议与待验证推断。纯文案不声称业务回归通过，本地检查不证明公网性能。
