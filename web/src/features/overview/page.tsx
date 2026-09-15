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
import { useDisplayed, useEngine, useMonitor } from "@/lib/observation/context";
import { record, rows, source } from "@/lib/observation/contracts";
import { count, label, time } from "@/lib/observation/format";

const chartConfig = {
  commits: { label: "metadata 提交 / 秒", color: "var(--chart-2)" },
};
export function OverviewPage() {
  const monitor = useMonitor();
  const engine = useEngine();
  const snapshot = useDisplayed("overview:snapshot", monitor.snapshot);
  const points = useDisplayed("overview:points", engine.trends.points);
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
        description="阶段可并行推进；未观察到活跃阶段不代表没有发生过。"
      >
        <div className="pipeline">
          <Link to="/discoveries">
            <span className="stage-number">01</span>
            <h3>发现</h3>
            <p>采样 · announce · 历史回填</p>
            <Status
              value={config.sample === false ? "主动采样未启用" : config.sample === true ? "sampling" : "未知"}
            />
          </Link>
          <Link to="/jobs">
            <span className="stage-number">02</span>
            <h3>接纳与调度</h3>
            <p>领取、资源等待与重试</p>
            <Status
              value={
                (Boolean(collector.capacity_paused))
                || (Boolean(collector.backlog_paused))
                || (Boolean(collector.storage_paused))
                  ? "local_wait"
                  : "当前状态见任务"
              }
            />
          </Link>
          <Link to="/events" search={{ kind: "peer", mode: "live" }}>
            <span className="stage-number">03</span>
            <h3>查找与下载</h3>
            <p>双栈查找、握手与分片</p>
            <span>
              {
                active.filter(a =>
                  ["lookup", "connect", "transfer"].includes(String(a.step)),
                ).length
              }
              {" "}
              个保留活跃阶段
            </span>
          </Link>
          <Link to="/metadata">
            <span className="stage-number">04</span>
            <h3>校验与提交</h3>
            <p>原始字节校验与事务结果</p>
            <span>只有 Applied 计提交</span>
          </Link>
        </div>
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
            <table>
              <thead>
                <tr>
                  <th>时间</th>
                  <th>提交 / 秒</th>
                </tr>
              </thead>
              <tbody>
                {points.slice(-20).map(p => (
                  <tr key={p.at}>
                    <td>{time(p.at)}</td>
                    <td>
                      {p.commits === null
                        ? "缺失 / 不连续"
                        : p.commits.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
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
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>关联 hash</th>
                <th>阶段</th>
                <th>generation</th>
                <th>开始时间</th>
              </tr>
            </thead>
            <tbody>
              {active.slice(0, 50).map((a) => {
                const c = record(a.context);
                return (
                  <tr key={String(c.span_id)}>
                    <td>
                      {typeof c.hash === "string"
                        ? (
                            <HashLink hash={c.hash} />
                          )
                        : (
                            <span className="muted">非 hash 阶段</span>
                          )}
                    </td>
                    <td>{label(a.step)}</td>
                    <td>{count(c.generation)}</td>
                    <td>{time(a.since_ms)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {active.length === 0 && <Empty>当前没有保留的活跃阶段。</Empty>}
      </Panel>
    </>
  );
}
