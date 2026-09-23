# 开发流程

从受影响行为和源码所有者开始，完成实现、检查与结果复核。模块归属见[源码目录设计](../architecture/repository-layout.md)，具体检查见[验证指南](validation.md)。

## 环境准备

开发检查使用 POSIX、Python 3.11 以上、Git。Rust 版本和组件由 [rust-toolchain.toml](../../rust-toolchain.toml) 固定，依赖与 features 由 Cargo 文件维护。文档工具是 Node 24.x、markdownlint-cli2 0.18.1、lychee 0.20.0。

以下命令在仓库根运行，需要软件源访问，会安装工具或写依赖缓存；成功判据是命令退出 0 和工具版本匹配。安装过程可 Ctrl-C 停止，未完成时不能运行检查并宣称通过：

```sh
rustup toolchain install 1.98.1 --profile minimal --component rustfmt --component clippy
npm ci --prefix tools/docs
cargo install lychee --version 0.20.0 --locked
```

Node 和 Python 通过本机已有包管理方式准备。不要用 npx 自动下载替代固定本地依赖。检查入口本身不安装工具、不修格式、不更新锁文件或快照。离线环境先准备缓存，必要时设置 CARGO_NET_OFFLINE=true；缓存缺失应报告阻塞。

## 实施闭环

1. 检查完整 Git 状态、暂存及未暂存 diff，保护未跟踪与本地材料，确定任务边界。
2. 按根 AGENTS 路由读入口、调用者、状态所有者和相关测试；区分已确认事实与待验证推断。
3. 明确可观察的验收条件、失败与取消行为；跨模块变化先确定职责和依赖，重大取舍写在所属专题。
4. 小批修改实现与联动文档。先运行受影响行为测试，再按任务选择检查范围。
5. 复读完整 diff 和关键路径，交付实际命令、结果、证据与未覆盖范围。已通过且未失效的检查不因交付或提交再次运行。

复杂任务在已有忽略的 `docs/plans` 中记录目标、工作区基线、已完成事项、实际验证、未完成项及下一步；不把 HEAD 当作未提交工作区的完整身份。交接保留可执行下一步与证据位置。完成后长期规则归入专题或开发文档，重要取舍就地说明；小修复无需创建计划文件。

## Skills 维护

保留五个通用与 Rust 职责入口：Rust 主流程、语言机制按需参考、异步生命周期、日志专项、文案残留审查；前端另保留当前 shadcn/Base UI 项目使用的 shadcn 专项。一次性迁移 skill 不在项目仓库常驻。项目参数与运行契约由 domains/operations 维护，目录规则由 repository-layout 维护；skill 只链接这些事实，不创建第二套参数或目录规范。

维护时核对 name、description、引用与实际触发任务；保留 agents/openai.yaml 的调用策略。上游许可证、来源、安装 hash 是来源记录，不能按本地修改内容重算或伪造。参考材料按具体问题选读，更新时对照本地适配，不整包覆盖。

技能结构可用已安装 skill-creator 的 `scripts/quick_validate.py` 检查；这属于维护时的辅助验证，不是仓库公开检查入口，也不能替代各任务路由的人工作用核对。

## 前端开发

前端使用 Node 24 和 pnpm 11.22.0，安装锁定依赖与 Chromium 后执行 `python3 scripts/check.py web`。CI 在检查前安装工具；检查阶段不自动下载。涉及公共检查脚本时同时选择 `tools`，变更说明同步选择 `docs`。页面切片、同源代理与静态构建见[前端说明](../../web/README.md)。
