# 发现流程观测前端

只读中文观测台仅保留流程总览（`/`）。首页挂载 `OverviewPage`，展示六项指标、七泳道粒子示意、事件结果分布、转化漏斗、吞吐曲线、阶段耗时分布和任务状态统计；粒子动画只是当前事件窗口的视觉示意，不提供详情跳转。后端接口和字段以[观测专题](../docs/domains/monitoring.md)为准；过程仅在当前运行的有限窗口内可用。

## 开发与检查

从仓库根目录执行，需要 Node 24、pnpm 11.22.0。首次准备依赖和 Chromium 需要联网；统一检查只使用已经安装的工具。

```bash
pnpm --dir web install --frozen-lockfile
pnpm --dir web exec playwright install chromium
pnpm --dir web dev
```

打开终端输出的 loopback 地址。开发与 preview 通过 `/api` 同源代理到 `http://127.0.0.1:3001`，后端需显式启用监控。可为开发服务器设置 `MONITOR_PROXY_TARGET`；该变量不进入浏览器构建。停止开发服务使用 Ctrl-C。后端运行与本机隔离约束见[运行指南](../docs/operations/running.md)。

```bash
python3 scripts/check.py web
pnpm --dir web build
pnpm --dir web preview
```

检查依次执行 ESLint、引用项目类型检查、Vitest、构建及 Chromium 本机 HTTP/SSE 测试。测试不恢复既有采集状态，不访问公网 DHT。已构建 Rust binary 和前端后，可另外执行 `node web/tests/rust-smoke.mjs` 验证真实无引导节点的代理、SSE 和退出；独立状态与证据保存在 `target/checks/frontend-rust-smoke/`。证据保存在 `target/checks/`，负载时间只用于同机诊断。`dist/` 是站点根路径部署产物。

## 模块与数据所有权

- `src/app` 负责布局、Provider、主题及唯一同步控制器的生命周期。
- `src/routes` 只挂载首页并负责未找到页面；`src/features/overview` 保存首页的查询组合、视图转换及测试。
- `src/components/ui` 集中 shadcn 原语；业务呈现一律经由这些原语，不手写控件样式，统计图统一使用 Chart 与配套 Recharts。
- `src/components/observation` 保存首页复用的状态、指标、面板和新鲜度展示。
- `src/lib/api` 负责错误、请求限额和最小读取；`src/lib/observation` 负责公共契约、SSE、有限事件缓存和趋势。

切片不引用其他切片内部实现，通过 URL 关联。公共层不反向依赖页面。新增 UI 原语使用仓库已有 shadcn CLI，不另建图表框架。样式一律使用 Tailwind utility 写在组件上（含响应式断点），元素级基础样式集中在 `index.css` 的 `@layer base`；状态色阶使用 `index.css` 的状态 token（`--status-*`），不手写 hex 或按类做 `.dark` 覆盖。

每标签页一个 SSE，从 snapshot 游标开始，接纳完整事件批次后推进序号；snapshot 消息不推进事件游标。普通断线有限退避续传，运行或窗口 reset 清除过程并重新获取快照。容量 reset 与不兼容格式停止自动同步，提供手动重新同步入口。数据库读取失败保留上次值并标记陈旧。

浏览器历史最多 5,000 条、8 MiB 编码载荷、15 分钟；当前状态独立保存。趋势最多 900 点，不连续区间不补零。冻结只复制首页所需数据，最多 2 MiB；事件仍继续接收。

请求调度上限为同时 2 个、数据库 1 个、5 次/秒及突发 5 次，待执行队列最多 20 个。页面查询最多每 5 秒刷新；后台标签页停止轮询。取消浏览器等待不表示数据库操作已经退出。

## 静态站接入

[Nginx 示例](nginx.conf)仅监听 loopback。将 `root` 改为实际 `web/dist` 绝对路径，在已有 Nginx 的 `http` 段包含该文件，然后按本机运维流程检查配置和启动。成功判据是直接访问 `/` 显示首页，`/api/v1/snapshot` 返回 JSON，首页显示实时连接。其他前端路径与旧监控 API 按不存在处理，不提供重定向。Nginx 进程由运行者按部署环境管理；本仓检查不会部署或启动 Nginx。

远端服务器的后端、代理都保持 loopback，在本机执行：

```bash
ssh -N -L 8080:127.0.0.1:8080 user@server
```

浏览器打开 `http://127.0.0.1:8080`，Ctrl-C 停止转发。无需 CORS，不转发后端 UDP 端口。代理关闭 SSE 缓冲与缓存，读取期限长于保活周期。
