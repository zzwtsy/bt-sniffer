# 分类检索与校准

用于专项审查时减少漏检，判断原则见 [SKILL.md](../SKILL.md)，改写示例见[中文正反例](examples.md)。以下模式有意覆盖较宽；命中不是问题结论，零命中也不证明没有残留。还应直接阅读范围内的模块说明、长注释和文档正文。

## 范围与调用约定

- 先将用户授权的文件或目录写入 `residue_scope`。示例只检查 README，不代表默认授权扫描全仓；用户明确要求全项目审查时才使用 `.`。
- 使用 `--hidden` 覆盖 `.agents/` 等隐藏目录；不使用 `--follow`，也不显式传入指向受保护目录的符号链接。
- 文件类型包含规则在前，排除规则在后，避免后续包含规则重新纳入历史或第三方文件。按实际范围补充生成文件、夹具、快照目录。历史交付记录不一定在 reports 下，应先辨认用途再排除。
- 本 skill 的反例文字会自命中，默认排除自身目录；明确审查 skill 本身时人工区分教学引文和实际指令。
- 自然语言英文模式使用 `-i`；阶段代码模式保持大小写敏感，避免把普通变量 `t4` 当作 `T4`。英文完整短语用单词边界，避免 `this PR` 命中 `this project`。
- 正则和 glob 使用 shell 单引号。命令只列出候选，不执行替换。`rg` 返回 1 表示无命中，2 表示执行错误；不得把错误当作干净结果。

下面是 Bash/zsh 会话示例。先设置范围与共同选项，再选择所需分类执行；无需创建脚本或安装工具。

```sh
residue_scope=(README.md)
residue_options=(
  -n --hidden
  --glob '*.md' --glob '*.rs'
  --glob '!vendor/**' --glob '!node_modules/**'
  --glob '!.git/**' --glob '!target/**' --glob '!state/**'
  --glob '!.agents/notes/archived/**'
  --glob '!docs/reports/**' --glob '!docs/reliability-implementation.md'
  --glob '!.agents/skills/trim-authoring-residue/**'
)
```

## 英文与编号检索

```sh
# 会话编号、未指明出处的草稿及阶段代码。
rg "${residue_options[@]}" '\(decision [0-9]+|\(audit [A-Z][0-9]+|design §|plan §|design ledger|\(B ruling|\bP-I\b|\b[WT][0-9]+\b' -- "${residue_scope[@]}"

# PR、分支和提交的现场视角。
rg "${residue_options[@]}" -i '\bthis PR\b|\bthis branch\b|\bthis stack\b|\blater PRs?\b|\bprevious commits?\b|\bthis commit\b' -- "${residue_scope[@]}"

# 变更叙述及缺乏明确版本归属的时间词。
rg "${residue_options[@]}" -i '\bused to\b|\bno longer\b|\bpreviously\b|\bthe old\b|\bwas renamed\b|\bwas moved\b|\bv[0-9]+\b|\bthis cut\b|\bcut [0-9]+\b|\btoday\b|\bfor now\b|\broadmap\b' -- "${residue_scope[@]}"

# 评审过程、自我辩护与模糊承诺。
rg "${residue_options[@]}" -i 'rejected in review|review round|\breviewer\b|as of v[0-9]+|\bprobably\b|should be enough|should suffice|it simply|is safe —|is safe --' -- "${residue_scope[@]}"

# 章节号只提示核对出处，标准条款和有归属的文档章节应保留。
rg "${residue_options[@]}" '§[[:space:]]*[0-9]+' -- "${residue_scope[@]}"
```

## 中文检索

项目正文和 Rust 注释以中文为主，不能沿用上游只搜索 `*.zh.md` 的限制，也不能把中文本身判为语言残留。

```sh
# 会话或计划引用：核对编号是否有可解析的拥有者。
rg "${residue_options[@]}" '(决策|审计|评审)[[:space:]]*[A-Z]?[0-9]+|设计稿|设计台账|第[一二三四五六七八九十0-9]+批|阶段[一二三四五六七八九十0-9]+' -- "${residue_scope[@]}"

# 变更叙述：运行时轮次和状态会产生合法命中。
rg "${residue_options[@]}" '本轮|上一轮|本次(实现|重构|清理|改动)|本版|旧版|旧策略|旧配置|旧日志|已移除|不再|以前|遗留' -- "${residue_scope[@]}"

# 评审叙述及没有边界依据的规划语气。
rg "${residue_options[@]}" '评审(确认|认为|要求|拒绝)|审查者|本 PR|后续 PR|暂时.*(够用|可以|安全)|应该(够用|足够|没问题)|以后再|后续再' -- "${residue_scope[@]}"

# 高噪声补充：只读上下文，判断是语法复述还是必要顺序。
rg "${residue_options[@]}" '先.*(然后|再)|首先|接下来|最后' -- "${residue_scope[@]}"
```

`rg` 不解析 Rust 语法，结果可能是字符串或测试输入；文案审查不因此获得修改日志、协议夹具或断言的授权。语言混杂只在完整段落内判断，保留 API 名称、错误标签、标准名称及原文引用；不运行泛化的 ASCII 或汉字删除规则。

## 校准与误报判断

采用一个模式前，先核对下表对应的正例与近似反例。窄模式应匹配正例、排除词形近似的反例；宽模式可能同时命中两者，必须通过语义判断保留反例。修改模式后重新校准，不以检索次数或零命中作为验收。

| 分类 | 应发现的候选 | 应保留或排除的近似内容 |
| --- | --- | --- |
| 英文会话编号 | `(decision 7)`、`T4`，没有出处 | 局部变量 `t4` 不应因忽略大小写被纳入；有稳定出处的编号可保留 |
| PR 视角 | `This PR adds a counter.` | `This project records counters.` 不应命中；描述如何写 PR 的文档可合法讨论 PR |
| 英文变更/时间 | `The counter was renamed today.` | `the key used to sign requests` 表示用途；`/v1/chat` 是接口路径 |
| 评审/模糊语气 | `Rejected in review; should suffice.` | 引述该句作反例的教学文档；有出处的历史评审证据 |
| 章节号 | `design §4`，没有出处 | `RFC 9110 §10.1.5` 有外部标准归属 |
| 中文阶段 | `根据决策 7，在第二批修复。` | “关闭阶段一结束后进入阶段二”若定义了真实运行时阶段，应保留 |
| 中文变更 | `本轮已移除旧配置。` | “只接受当前和上一轮 token 密钥”“本次结果不再有效”描述运行时状态 |
| 中文评审/规划 | `评审确认暂时应该够用。` | 明确日期报告中的评审证据；有效 issue 跟踪的待办 |
| 控制流 | `先增加 i，然后检查 i。` | “先登记等待者再释放锁，避免遗漏唤醒”解释竞态约束 |

例如以下校准应只输出第一行；这里直接读取标准输入，不扫描项目文件：

```sh
rg -n -i '\bthis PR\b' <<'CALIBRATION'
This PR adds a counter.
This project records counters.
CALIBRATION
```

实际审查还需核对无链接的源码路径和候选引用的目标；现存文件不代表其中的历史版本陈述成立。反例、规范引文、运行时状态和历史证据均可形成合理保留项。最终结果按事实与上下文判断，不按关键词定性。

## 来源

改编自 DeepSeek [recall-batteries.md](https://github.com/deepseek-ai/deepseek-harness/blob/c291e7961a515f6d7af9304e7fd1d257929aef26/.agents/skills/dsh-trim-cot-leakage/references/recall-batteries.md)，适用本 skill 的 [MIT 许可](../LICENSE)。中文范围、Rust 文件类型、历史材料排除和校准示例按本项目适配。
