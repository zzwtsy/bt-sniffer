# 来源与适用范围

核对日期：2026-09-13。

## 设计来源

六张 PNG 是本次会话用户上传截图的原样副本：

| 包内文件 | 页面 | 用途 |
|---|---|---|
| assets/reference-home-light.png | 浅色首页 | 文案/产品展示的宽度关系、留白与视觉焦点 |
| assets/reference-issues-light.png | 浅色问题页 | 紧凑列表、筛选与元信息对齐 |
| assets/reference-issues-dark.png | 深色问题页 | 暖暗底、深色强调色与边界层级 |
| assets/reference-discussions-light.png | 浅色讨论页 | 稀疏内容的框架与自然留白 |
| assets/reference-releases-light.png | 浅色版本页 | 阅读列宽与折叠列表 |
| assets/reference-releases-dark.png | 深色版本页 | 文档型面板的深色层级 |

截图的页面背景像素分别为 RGB(247,244,238) 与 RGB(27,27,26)。
本包其他色彩、字号、宽度与间距参数是为复用而选取的建议值，不声明是原站 CSS。
本包未采用深色首页截图中未显示文字的大面板作为设计参考，因为无法凭截图确认该状态的原因。
截图右侧的粉色浮层也不属于本包要复用的视觉规则；未核实其来源。
不要把图片中的版本、日期或帖子内容当作当前实时产品资料。

## Agent Skills 格式与方法

[S1] Agent Skills Specification
`https://agentskills.io/specification`
用于 SKILL.md、frontmatter、命名和可选目录的格式依据。

[S2] Best practices for skill creators
`https://agentskills.io/skill-creation/best-practices`
用于任务边界、过程化指令、按需加载与根据实际执行修订的方法参考。

[S3] Evaluating skill output quality
`https://agentskills.io/skill-creation/evaluating-skills`
用于真实任务、对照评估和结果检查的方法参考。本包用例与分数阈值为自行设计。

## 宿主文档

[H1] Claude Code — Extend Claude with skills
`https://code.claude.com/docs/en/skills`
本包 README 中 Claude Code 的项目/个人路径和显式调用方式依据此文档。

[H2] OpenAI — Build skills
`https://developers.openai.com/codex/skills`
核对时跳转到官方文档 `https://learn.chatgpt.com/docs/build-skills`。
本包 README 中 Codex CLI/IDE 的 `.agents/skills` 路径、`$` 引用和 `/skills` 入口依据此文档。

## 可访问性

[W1] W3C — Understanding SC 1.4.3: Contrast (Minimum)
`https://www.w3.org/WAI/WCAG22/Understanding/contrast-minimum.html`
文字对比要求与脚本采用的 sRGB 相对亮度、对比度公式来源。

[W2] W3C — Understanding SC 1.4.11: Non-text Contrast
`https://www.w3.org/WAI/WCAG22/Understanding/non-text-contrast.html`
必要控件与状态识别信息的非文本对比要求；不把所有装饰线视作相同要求。

[W3] W3C — Understanding SC 2.5.8: Target Size (Minimum)
`https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum`
指针目标最低尺寸及其例外。44px 级触控目标是本包建议，不是把该最低条款改成 44px。

这些规范引用只覆盖本包提到的相关条款，不构成法律意见、认证或完整 WCAG 符合性结论。
