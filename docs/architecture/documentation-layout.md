# 文档目录设计

文档为 AI 和掌握基础 Rust 的维护者提供任务地图、业务约束和验证入口。源码与测试决定事实，文档解释边界和取舍；发现冲突时先核实源码，不用文档覆盖实际行为。

## 完整结构

```text
README.md
AGENTS.md
.agents/skills/
docs/
├── README.md
├── AGENTS.md
├── architecture/
│   ├── overview.md
│   ├── repository-layout.md
│   ├── documentation-layout.md
│   └── lifecycle.md
├── development/
│   ├── workflow.md
│   ├── validation.md
│   └── documentation.md
├── domains/
│   ├── dht.md
│   ├── collection.md
│   ├── storage.md
│   ├── logging.md
│   └── log-events.md
└── operations/
    ├── running.md
    └── diagnostics.md
```

`docs/plans` 是忽略的本地任务材料；`docs/bittorrent.org` 是外部协议资料，均不属于现行项目手册目录。执行证据进入现有忽略目录，不建立报告归档树。

## 信息归属

| 位置 | 读者与问题 | 唯一维护的详细事实 | 源码依据与联动 |
| --- | --- | --- | --- |
| 根 README | 初次接触项目：做什么、从哪开始 | 能力摘要与限制 | Cargo、main；链接导航和运行 |
| 根 AGENTS | AI 接任务：先读什么、如何交付 | 仓库级协作边界与路由 | 导向 skills 和开发流程，不复制专题表 |
| architecture | 定位模块或改变边界的开发者 | 目录规则、依赖、跨组件生命周期 | main、app/session、模块声明；业务细节链接 domains |
| development | 实施与审阅者 | 工作流程、检查选择、文档维护规则 | check.py、CI、工具配置；操作环境链接 running |
| domains/dht | 协议与网络开发者 | 匹配、校验、采样、流量约束 | dht；联动采集、生命周期 |
| domains/collection | 采集开发者 | 任务状态、领取、重试、结果提交 | collection；联动存储与事件 |
| domains/storage | 持久化开发者 | schema、线程、预算、事务与恢复 | storage 和两个 Store；联动采集、诊断 |
| domains/logging | 日志机制开发者 | 过滤、输出、队列与关闭保证 | app/logging；联动生命周期 |
| domains/log-events | 事件生产者和消费者 | 字段类型、单位、版本和统计含义 | tracing 调用、diagnostics、Python 解析；联动日志机制 |
| operations/running | 运行者与 CLI 开发者 | 参数表、地址、目录、启动与停止 | app/config、sockets、main；联动相关领域 |
| operations/diagnostics | 排障与验收人员 | 工具操作、判据及证据局限 | scripts/diagnostics、evidence、acceptance；联动事件和存储 |
| skills | 实施专项任务的 AI | 通用 Rust、异步、日志设计、文案审查方法 | 按问题引用参考；项目值与规则链接本仓文档 |

参数只在运行页维护完整表，状态机只在采集页维护，事件字段只在事件参考维护。地图和导航解释如何找到它们，不复制第二份。业务专题开头必须给源码和验证入口；精确代码行为变更应同时核对该页和直接消费者。

## AI 阅读路径

| 任务 | 顺序 |
| --- | --- |
| 定位功能或新增模块 | 根 AGENTS → 源码目录设计 → 对应业务专题 |
| Rust 实现与审查 | 根 AGENTS → Rust 主 skill → 受影响专题与测试 |
| 异步、取消或关闭 | Rust 主 skill → 异步专项 → 生命周期及相关专题 |
| 日志或消费者修改 | 日志专项 → 日志机制；涉及字段再读事件参考 |
| 文档修改 | docs/AGENTS → 本页与写作规则 → 对应源码 |
| 运行与排障 | 运行或诊断指南 → 必要业务定义 |

一次只展开任务涉及的分支。遇到未知机制先定位具体问题，再读取 skill 参考，不要求全量预读。

## 维护取舍

按业务领域维护详细契约，可以把状态变更与负责的代码放在同一阅读路径。按实施日期组织会让读者自行合并多个时期的描述；按完整教程重复参数和状态会增加同步成本。当前采用专题加任务导航，代价是跨领域修改需要核对多个明确入口。

取舍写在所属专题，交代当前依据、替代方案、代价与重访条件。无法核实的历史动机不能写成事实。只有出现真实的新读者、独立能力或明显过长且拥有独立事实来源的专题，才拆页或增加分类；不提前创建空目录。写作细则见[文档维护](../development/documentation.md)。
