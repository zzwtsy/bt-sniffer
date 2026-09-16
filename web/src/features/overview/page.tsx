import { Link } from "@tanstack/react-router";
import { Activity, Database, Network, Timer } from "lucide-react";
import { CartesianGrid, Line, LineChart, XAxis, YAxis } from "recharts";
import {
  Empty,
  Freshness,
  HashLink,
  Metric,
  PageTitle,
  Panel,
  Status,
} from "@/components/observation/common";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useDisplayed, useEngine, useMonitor } from "@/lib/observation/context";
import { record, rows, source } from "@/lib/observation/contracts";
import { count, label, time } from "@/lib/observation/format";
import { FlowDiagram } from "./flow";
import { flowStats } from "./flow-stats";

const chartConfig = {
  commits: { label: "metadata 提交 / 秒", color: "var(--chart-2)" },
};
export function OverviewPage() {
  const monitor = useMonitor();
  const engine = useEngine();
  const snapshot = useDisplayed("overview:snapshot", monitor.snapshot);
  const points = useDisplayed("overview:points", engine.trends.points);
  const flow = useDisplayed(
    "overview:flow",
    flowStats(monitor.snapshot, engine.buffer.select({}, 5000)),
  );
  const nodes = rows(snapshot?.cached.nodes);
  const collector = source(snapshot, "collector");
  const config = source(snapshot, "config");
  const database = record(snapshot?.cached.database);
  const facts = record(database.value);
  const jobs = record(facts.jobs);
  const active = snapshot?.runtime.active ?? [];
  return (
    <>
      <PageTitle
        eyebrow="LIVE OBSERVATORY"
        title="看见每一次发现"
        description="从 DHT 网络到 metadata 提交，追踪当前进展与等待原因。"
      />
      <div className="metrics">
        <Metric
          title="节点可用"
          value={
            snapshot
              ? `${nodes.filter(n => n.available === true).length} / ${nodes.length}`
              : "—"
          }
          detail="IPv4 / IPv6 独立观察"
          icon={<Network size={17} />}
        />
        <Metric
          title="运行中 worker"
          value={count(collector.running_workers)}
          detail={
            config.fetch === false ? "采集未启用" : "当前执行中的采集任务"
          }
          icon={<Activity size={17} />}
        />
        <Metric
          title="任务等待重试"
          value={count(jobs.retry_wait)}
          detail="数据库统计，不等同于正在等待的 RPC"
          icon={<Timer size={17} />}
        />
        <Metric
          title="已保存 metadata"
          value={count(facts.metadata_count)}
          detail="数据库累计 · 重启后保留"
          icon={<Database size={17} />}
        />
      </div>
      <div className="overview-meta">
        <Freshness at={snapshot?.runtime.sources.collector?.observed_at_ms} />
        <Freshness
          at={database.observed_at_ms}
          stale={database.stale === true}
        />
      </div>
      <Panel
        title="发现与采集链路"
        description="节点只陈述对应来源的事实；窗口计数来自浏览器事件缓冲，连接初期为空。"
      >
        <FlowDiagram
          stats={flow}
          perSecond={points.findLast(p => p.commits !== null)?.commits}
        />
      </Panel>
      <div className="overview-grid">
        <Panel
          title="提交吞吐"
          description="次 / 秒 · 浏览器连接以来，最多保留 15 分钟"
        >
          <ChartContainer config={chartConfig} className="h-57.5 w-full">
            <LineChart data={points} accessibilityLayer>
              <CartesianGrid vertical={false} />
              <XAxis
                dataKey="at"
                tickFormatter={v =>
                  new Date(Number(v)).toLocaleTimeString("zh-CN", {
                    hour: "2-digit",
                    minute: "2-digit",
                  })}
                minTickGap={40}
              />
              <YAxis allowDecimals={false} width={38} />
              <ChartTooltip
                content={
                  <ChartTooltipContent labelFormatter={v => time(v)} />
                }
              />
              <Line
                dataKey="commits"
                type="linear"
                stroke="var(--color-commits)"
                dot={false}
                connectNulls={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ChartContainer>
          {points.length < 2 && (
            <p className="muted">
              等待两个连续、有效的采集快照；未启用采集时不生成曲线。
            </p>
          )}
          <details>
            <summary>查看图表数据（最近 20 点）</summary>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>时间</TableHead>
                  <TableHead>提交 / 秒</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {points.slice(-20).map(p => (
                  <TableRow key={p.at}>
                    <TableCell>{time(p.at)}</TableCell>
                    <TableCell>
                      {p.commits === null
                        ? "缺失 / 不连续"
                        : p.commits.toFixed(2)}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </details>
        </Panel>
        <Panel title="值得查看" description="业务状态与监控连接状态分别判断。">
          <div className="attention-item">
            <Status
              value={
                config.fetch === false
                  ? "采集未启用"
                  : collector.running_workers === undefined
                    ? "未知"
                    : collector.storage_paused === true
                      ? "failed"
                      : (Boolean(collector.capacity_paused)) || (Boolean(collector.backlog_paused))
                          ? "local_wait"
                          : "ready"
              }
            />
            <h3>采集背压</h3>
            <p className="muted">
              {((collector.storage_paused === true))
                ? "存储错误导致暂停，请查看后端诊断"
                : ((collector.capacity_paused === true))
                    ? "容量达到限制"
                    : ((collector.backlog_paused === true))
                        ? "积压导致等待"
                        : config.fetch === false
                          ? "采集未启用"
                          : collector.running_workers === undefined ? "采集数据尚不可用" : "当前没有观察到背压"}
            </p>
          </div>
          <div className="attention-item">
            <h3>有限历史窗口</h3>
            <p className="muted">
              后端已淘汰
              {count(snapshot?.window.evicted)}
              {" "}
              条，截断
              {count(snapshot?.window.truncated)}
              {" "}
              条。
            </p>
            <Link to="/events">查看事件证据 →</Link>
          </div>
          <div className="attention-item">
            <h3>数据边界</h3>
            <p className="muted">
              趋势不包含连接前的指标。断线、重启和不可用区间留空。
            </p>
          </div>
        </Panel>
      </div>
      <Panel
        title="当前活跃阶段"
        description="快照独立于历史缓存；最多展示 50 项。"
      >
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>关联 hash</TableHead>
              <TableHead>阶段</TableHead>
              <TableHead>generation</TableHead>
              <TableHead>开始时间</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {active.slice(0, 50).map((a) => {
              const c = record(a.context);
              return (
                <TableRow key={String(c.span_id)}>
                  <TableCell>
                    {typeof c.hash === "string"
                      ? (
                          <HashLink hash={c.hash} />
                        )
                      : (
                          <span className="muted">非 hash 阶段</span>
                        )}
                  </TableCell>
                  <TableCell>{label(a.step)}</TableCell>
                  <TableCell>{count(c.generation)}</TableCell>
                  <TableCell>{time(a.since_ms)}</TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
        {active.length === 0 && <Empty>当前没有保留的活跃阶段。</Empty>}
      </Panel>
    </>
  );
}
