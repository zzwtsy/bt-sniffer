import type {
  DurationEntry,
  Funnel,
  JobStates,
  ResultsSummary,
  ThroughputPoint,
} from "./model";
import { Fragment, memo } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  XAxis,
  YAxis,
} from "recharts";
import { Empty } from "@/components/observation/common";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";

import { count, label, time, timeTick } from "@/lib/observation/format";
import { JOB_STATES, quantileLabel } from "./model";

/** 漏斗四级颜色与七泳道一致，跨面板扫读时保持同一阶段的同色联想。 */
const funnelColors: Record<string, string> = {
  discovery: "var(--chart-1)",
  admission: "var(--chart-2)",
  claim: "var(--chart-3)",
  commit: "var(--status-success)",
};
/** 非零计数的最小可见高度占比，保证数量级悬殊时末级仍可辨认。 */
const MIN_LEVEL_HEIGHT = 0.12;
/** 级间连接带宽度（px）：越宽斜边越平缓，越窄流失感越尖锐。 */
const CONNECTOR_WIDTH = 56;
/**
 * 转化漏斗：发现 → 接纳 → 领取 → 提交四级窗口计数，横向排列与七泳道方向一致。
 * 每级为垂直居中的矩形，高度 ∝ 窗口计数；级间斜边连接带的收窄坡度即转化率的几何表达。
 * recharts FunnelChart 不支持横向（recharts/recharts#2799），故自绘；窗口口径下级间计数
 * 不保证单调，连接带如实扩张或收窄，零计数级退化为参考细线而非消失。
 */
export const FunnelChart = memo(({ data }: { data: Funnel | undefined }) => {
  if (data === undefined)
    return <Empty>窗口内尚无记录，连接初期或缓冲为空时不展示漏斗。</Empty>;
  const levels = data.levels;
  const max = Math.max(...levels.map(level => level.count), 1);
  const heights = levels.map(level =>
    level.count > 0 ? Math.max(level.count / max, MIN_LEVEL_HEIGHT) : 0,
  );
  return (
    <div data-slot="funnel-chart" className="overflow-x-auto">
      <div
        className="grid min-w-115 gap-y-1"
        style={{
          gridTemplateColumns: levels.map(() => "minmax(0,1fr)").join(` ${CONNECTOR_WIDTH}px `),
          gridTemplateRows: "auto 144px auto",
        }}
      >
        {levels.map((level, i) => {
          const color = funnelColors[level.id] ?? "var(--chart-2)";
          const before = i > 0 ? levels[i - 1] : undefined;
          const rate = before === undefined
            ? undefined
            : before.count > 0
              ? `${(level.count / before.count * 100).toFixed(1)}%`
              : "样本不足";
          const h = heights[i];
          const column = i * 2 + 1;
          // 连接带左右缘高度；零计数级收成细尖，如实表达断流而非维持形状
          const from = Math.max(i > 0 ? heights[i - 1] : h, 0.015);
          const to = Math.max(h, 0.015);
          return (
            <Fragment key={level.id}>
              <div
                className="min-w-0 text-center"
                style={{ gridColumn: column, gridRow: 1 }}
              >
                <p className="text-[11px] text-muted-foreground">{level.title}</p>
                <p
                  className="text-sm font-semibold tabular-nums"
                  style={{ color }}
                >
                  {count(level.count)}
                </p>
              </div>
              <div
                className="relative min-w-0"
                style={{ gridColumn: column, gridRow: 2 }}
                title={`${level.title} ${count(level.count)}`}
              >
                <div
                  className="absolute inset-x-0"
                  style={h > 0
                    ? { top: `${(1 - h) / 2 * 100}%`, height: `${h * 100}%`, background: color }
                    : { top: "calc(50% - 1px)", height: 2, background: "var(--muted)" }}
                />
              </div>
              {before !== undefined && rate !== undefined && (
                <>
                  <div
                    aria-hidden="true"
                    className="relative"
                    style={{ gridColumn: column - 1, gridRow: 2 }}
                  >
                    <div
                      className="absolute inset-0"
                      style={{
                        clipPath: `polygon(0% ${(1 - from) / 2 * 100}%, 100% ${(1 - to) / 2 * 100}%, 100% ${(1 + to) / 2 * 100}%, 0% ${(1 + from) / 2 * 100}%)`,
                        background: `linear-gradient(to right, color-mix(in oklab, ${funnelColors[before.id] ?? "var(--chart-2)"} 55%, transparent), color-mix(in oklab, ${color} 55%, transparent))`,
                      }}
                    />
                  </div>
                  <p
                    className="text-center text-[10px] text-muted-foreground tabular-nums"
                    style={{ gridColumn: column - 1, gridRow: 3 }}
                    aria-label={`${before.title}到${level.title}的转化率 ${rate}`}
                  >
                    {rate}
                  </p>
                </>
              )}
            </Fragment>
          );
        })}
      </div>
    </div>
  );
}, (before, after) => before.data === after.data || (
  before.data !== undefined && after.data !== undefined
  && before.data.levels.length === after.data.levels.length
  && before.data.levels.every((level, i) => {
    const next = after.data!.levels[i];
    return level.id === next.id && level.title === next.title && level.count === next.count;
  })
));

const throughputConfig = {
  discoveries: { label: "发现 / 分", color: "var(--chart-1)" },
  commits: { label: "提交 / 秒", color: "var(--chart-2)" },
};

/** 结果汇总行：失败红、完成绿、进行中与其他中性；条长相对本组最大值。 */
function ResultRow({ name, tone, value, total, max }: {
  name: string;
  tone: string;
  value: number;
  total: number;
  max: number;
}) {
  return (
    <div className="grid grid-cols-[104px_52px_1fr] items-center gap-2.5 text-xs">
      <span className="flex items-center gap-1.5 text-muted-foreground">
        <span
          aria-hidden="true"
          className="size-2 shrink-0 rounded-full"
          style={{ background: tone }}
        />
        {name}
      </span>
      <span className="text-right font-medium tabular-nums">{count(value)}</span>
      <span className="flex items-center gap-2">
        <span className="h-1.5 min-w-0 flex-1 rounded-full bg-muted">
          <span
            className="block h-full rounded-full"
            style={{
              width: `${Math.max(value / max * 100, value > 0 ? 2 : 0)}%`,
              background: tone,
            }}
          />
        </span>
        <span className="w-12 text-right text-[11px] text-muted-foreground tabular-nums">
          {total > 0 ? `${(value / total * 100).toFixed(1)}%` : "—"}
        </span>
      </span>
    </div>
  );
}

/** 事件结果分布：先回答"有没有错"，再列出主要失败原因。 */
export const ResultsChart = memo(({ data }: { data: ResultsSummary | undefined }) => {
  if (data === undefined)
    return <Empty>窗口内尚无匹配事件；连接初期、缓冲为空或过滤阶段无事件时不展示分布。</Empty>;
  const max = Math.max(data.failed, data.succeeded, data.other, 1);
  return (
    <div data-slot="results-chart" className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <ResultRow name="失败" tone="var(--status-danger)" value={data.failed} total={data.total} max={max} />
        <ResultRow name="正常完成" tone="var(--status-success)" value={data.succeeded} total={data.total} max={max} />
        <ResultRow name="进行中与其他" tone="var(--chart-4)" value={data.other} total={data.total} max={max} />
      </div>
      <div>
        <p className="mb-1 text-xs font-medium text-muted-foreground">主要失败原因</p>
        {data.failures.length === 0
          ? <p className="text-xs text-muted-foreground">窗口内无失败事件。</p>
          : (
              <ul>
                {data.failures.map(failure => (
                  <li
                    key={failure.result}
                    className="flex items-center justify-between gap-3 border-b py-1.5 text-xs last:border-b-0"
                  >
                    <span className="min-w-0 truncate" title={failure.result}>
                      {label(failure.result)}
                    </span>
                    <span className="shrink-0 tabular-nums">{count(failure.count)}</span>
                  </li>
                ))}
              </ul>
            )}
      </div>
    </div>
  );
}, (before, after) => before.data === after.data || (
  before.data !== undefined && after.data !== undefined
  && before.data.total === after.data.total
  && before.data.failed === after.data.failed
  && before.data.succeeded === after.data.succeeded
  && before.data.other === after.data.other
  && before.data.failures.length === after.data.failures.length
  && before.data.failures.every((failure, i) => {
    const next = after.data!.failures[i];
    return failure.result === next.result && failure.count === next.count;
  })
));

/** 双折线吞吐：发现速率来自本地事件窗口，提交速率复用快照差分；断线留空。 */
export const ThroughputChart = memo(({ points }: { points: ThroughputPoint[] }) => {
  return (
    <>
      <ChartContainer config={throughputConfig} className="h-45 w-full">
        <LineChart data={points} accessibilityLayer>
          <CartesianGrid vertical={false} />
          <XAxis
            dataKey="at"
            tickFormatter={timeTick}
            minTickGap={40}
          />
          <YAxis yAxisId="left" allowDecimals={false} width={38} />
          <YAxis
            yAxisId="right"
            orientation="right"
            allowDecimals={false}
            width={38}
          />
          <ChartTooltip
            content={<ChartTooltipContent labelFormatter={time} />}
          />
          <Line
            yAxisId="left"
            dataKey="discoveries"
            type="linear"
            stroke="var(--color-discoveries)"
            dot={false}
            connectNulls={false}
            isAnimationActive={false}
          />
          <Line
            yAxisId="right"
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
        <p className="text-xs text-muted-foreground">
          等待连续的实时数据；断线与未启用区间留空，不补零。
        </p>
      )}
    </>
  );
});

const durationConfig = { ms: { label: "耗时（桶上界，毫秒）", color: "var(--chart-4)" } };
const durationColors: Record<string, string> = {
  lookup: "var(--chart-4)",
  tcp_wait: "var(--chart-5)",
  first_peer: "var(--chart-3)",
};
const timingLabels: Record<string, string> = {
  lookup: "peer 查找",
  tcp_wait: "TCP 许可等待",
  first_peer: "首个 peer",
};
/** 阶段耗时：p50/p95/p99 为固定桶上界，溢出只标注超过最大桶。 */
export const DurationsChart = memo(({ entries }: { entries: DurationEntry[] }) => {
  const rows = entries.flatMap(entry =>
    (["p50", "p95", "p99"] as const).map(q => ({
      name: `${timingLabels[entry.timing] ?? entry.timing} ${q}`,
      ms: entry[q].upperBoundMs ?? undefined,
      text: quantileLabel(entry[q]),
      count: entry.count,
      timing: entry.timing,
    })),
  );
  if (rows.length === 0)
    return <Empty>采集未启用或指标尚不可用。</Empty>;
  return (
    <>
      <ChartContainer
        config={durationConfig}
        className="h-45 w-full"
      >
        <BarChart
          data={rows}
          layout="vertical"
          accessibilityLayer
          margin={{ left: 8, right: 16 }}
        >
          <XAxis type="number" hide />
          <YAxis
            type="category"
            dataKey="name"
            width={96}
            tickLine={false}
            axisLine={false}
          />
          <ChartTooltip
            content={(
              <ChartTooltipContent
                formatter={(_value, _name, item) => {
                  const row = item.payload as { text: string; count?: number };
                  return (
                    <span>
                      {row.text}
                      {" "}
                      · 样本
                      {" "}
                      {count(row.count)}
                    </span>
                  );
                }}
              />
            )}
          />
          <Bar dataKey="ms" radius={3} isAnimationActive={false}>
            {rows.map(row => (
              <Cell
                key={row.name}
                fill={durationColors[row.timing] ?? "var(--chart-4)"}
              />
            ))}
          </Bar>
        </BarChart>
      </ChartContainer>
      <p className="text-xs text-muted-foreground">
        柱形为固定桶上界（≤ 该值），非精确分位数；空样本不绘制。
      </p>
    </>
  );
}, (before, after) => before.entries === after.entries || (
  before.entries.length === after.entries.length
  && before.entries.every((entry, i) => {
    const next = after.entries[i];
    return entry.timing === next.timing && entry.count === next.count
      && (["p50", "p95", "p99"] as const).every(q =>
        entry[q].upperBoundMs === next[q].upperBoundMs && entry[q].exceedsMs === next[q].exceedsMs);
  })
));

const stateColors: Record<string, string> = {
  pending: "var(--chart-1)",
  running: "var(--chart-2)",
  retry_wait: "var(--status-warning)",
  dormant: "var(--muted-foreground)",
  succeeded: "var(--status-success)",
};
const stateConfig = Object.fromEntries(JOB_STATES.map(state => [
  state,
  { label: label(state), color: stateColors[state] },
]));
/** 任务状态环图：数据库 30 秒统计；图例只用于解释当前快照。 */
export const JobStatesChart = memo(({ data }: { data: Pick<JobStates, "states"> | undefined }) => {
  if (data === undefined)
    return <Empty>数据库统计尚不可用。</Empty>;
  const states = data.states.map(s => ({ ...s, value: s.count }));
  const total = states.reduce((sum, s) => sum + s.value, 0);
  if (total === 0)
    return <Empty>数据库中尚无采集任务。</Empty>;
  return (
    <>
      <ChartContainer
        config={stateConfig}
        className="h-45 w-full"
      >
        <PieChart accessibilityLayer>
          <ChartTooltip content={<ChartTooltipContent nameKey="state" />} />
          <Pie
            data={states}
            dataKey="value"
            nameKey="state"
            innerRadius="55%"
            outerRadius="85%"
            isAnimationActive={false}
          >
            {states.map(s => (
              <Cell key={s.state} fill={stateColors[s.state]} />
            ))}
          </Pie>
        </PieChart>
      </ChartContainer>
      <div className="flex flex-wrap gap-1.5">
        {states.map(s => (
          <span key={s.state} className="inline-flex items-center gap-1.5 rounded-full border bg-card px-2.5 py-0.75 text-[11px] text-card-foreground">
            <span
              aria-hidden="true"
              className="inline-block size-2 rounded-full"
              style={{ background: stateColors[s.state] }}
            />
            {label(s.state)}
            {" "}
            {count(s.count)}
          </span>
        ))}
      </div>
    </>
  );
}, (before, after) => before.data === after.data || (
  before.data !== undefined && after.data !== undefined
  && before.data.states.length === after.data.states.length
  && before.data.states.every((state, i) =>
    state.state === after.data!.states[i].state && state.count === after.data!.states[i].count)
));
