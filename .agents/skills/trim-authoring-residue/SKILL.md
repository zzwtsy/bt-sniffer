---
name: trim-authoring-residue
description: >-
  审查或清理文档与代码注释中依赖编写会话的引用、实施阶段残留、变更过程叙述及冗余解释。
  用于用户要求此类检查，或当前文案任务出现这些问题时；保留事实、约束和设计理由，
  不自动扩展为全项目清理，不把运行时新旧状态或历史报告当作残留。
license: MIT; see LICENSE
metadata:
  language: zh-CN
  adaptation: bt-sniffer
  upstream-commit: c291e7961a515f6d7af9304e7fd1d257929aef26
---

# 清理文档与注释中的编写会话残留

## 判据与适用范围

让读者仅凭当前仓库和明确链接的材料，就能理解引用、核对事实。检查的是文字视角与信息完整性，不是安全漏洞或模型内部推理泄露。

先确定用户要求的文件或当前任务涉及的内容。审查只报告；明确要求修复时直接处理范围内的确定问题。全项目审查须有明确请求，不因一个关键词扩展扫描或修改范围。

默认排除 `vendor/`、`.git/`、`target/`、`state/`、`.agents/notes/archived/`，也不经符号链接进入这些位置。`docs/reports/`、标明历史用途的交付记录和快照保留原始证据；可按需读取精确引用，不将其改写为当前行为。自动生成的内容从拥有它的源文件处理，不手改派生文件。

## 如何判断

对可疑段落逐项核对：

- 引用能否解析？没有出处的“决策 7”“评审 C2”“阶段二”应替换为可取得的材料链接，或保留独立成立的规则并去掉编号。未提交文件可以用于当前工作区核对，但不能作为读者无法取得的外部历史版本的证明。
- 是否描述当前机制？README、使用手册和源码注释优先说明现行行为。“本轮新增”“已移除”“评审确认”等过程叙述应改写。必要的版本迁移说明可以集中保留，并明确起止版本；版本号本身不是问题。
- 是否提供必要信息？保留非显然的所有权、取消、顺序、单位、失败后果和设计理由。仅复述相邻代码的注释可删除；操作手册的步骤、面向初学者的教学说明和解释测试同步关系的注释有实际用途，不能一概删除。
- 是否把未知写成承诺？“暂时应该够用”应核对真实上限与适用条件。缺少证据时明确未知；确有待办事项可用具体 TODO 或有效 issue 链接，不能虚构结论或待办。

运行时新旧连接、generation、token 轮次，协议/schema/日志版本，可解析的标准与 issue 引用，测量来源，lint 豁免理由和“没有这项保护会发生什么”的解释通常应保留。中英文技术名词按项目语言约定判断，不机械翻译标识符。

## 改写与验证

1. 先读相关实现或拥有规则的文档，再判断文字。专项审查时按需读取[分类检索与校准](references/recall-batteries.md)，在授权路径中查找候选并显式排除受保护目录；也抽读说明密集的段落。搜索只是线索，不按关键词批量替换。判断有疑问时读[中文正反例](references/examples.md)。
2. 改写前辨认段落中的主体、动作、条件、时间与顺序、必须/可以/禁止、例外、所有权、失败后果及证据来源。每项有用事实都应保留；不能把“不保证”变为“保证”，不能把计划写成已实现能力。
3. 当前说明只保留现行规则及必要理由。夹杂的历史验证证据可在用户授权整理文档时移入带日期的报告，并同步引用；已有历史报告保持原样。来源日期或源码版本未知时如实标记，不能用整理日期冒充验证日期。
4. 文案清理不修改代码逻辑、断言、SQL、CLI、日志字符串、事件字段或快照。发现文字与实现冲突时核对事实；需要改变行为才能解决的问题单独报告，不暗中实现。
5. 复读改写前后事实，检查本地文件链接、锚点及反引号中的源码路径。重新检索后解释合理保留项，而不是追求零命中。运行受影响路径的 `git diff --check`；仅改文案或 skill 时不要求业务回归，改到 Rust 注释可检查格式。若获授权改变行为，按项目要求补相应验证。

交付说明检查范围、确定问题或实际改写、必要保留项及验证限制。不以删行数验收，不为每次使用新增审查报告文件。

## 来源与本地维护

本 skill 为 bt-sniffer 的自包含中文适配版，依据 DeepSeek 的以下材料编写，固定参考版本为 `c291e7961a515f6d7af9304e7fd1d257929aef26`：

- [dsh-trim-cot-leakage](https://github.com/deepseek-ai/deepseek-harness/blob/c291e7961a515f6d7af9304e7fd1d257929aef26/.agents/skills/dsh-trim-cot-leakage/SKILL.md)
- [dsh-prose-standard](https://github.com/deepseek-ai/deepseek-harness/blob/c291e7961a515f6d7af9304e7fd1d257929aef26/.agents/skills/dsh-prose-standard/SKILL.md)
- [recall-batteries](https://github.com/deepseek-ai/deepseek-harness/blob/c291e7961a515f6d7af9304e7fd1d257929aef26/.agents/skills/dsh-trim-cot-leakage/references/recall-batteries.md)

上游采用 [MIT 许可](https://github.com/deepseek-ai/deepseek-harness/blob/c291e7961a515f6d7af9304e7fd1d257929aef26/LICENSE)，版权与许可声明见本目录 [LICENSE](LICENSE)。本地版保留事实完整性与引用核对原则，按项目授权范围执行，使用本地文档检查，不依赖上游的文档生成、双语配对或 Agent Notes 工作流。更新参考版本时复核差异与许可，不伪造 `skills-lock.json` 的安装来源或哈希。
