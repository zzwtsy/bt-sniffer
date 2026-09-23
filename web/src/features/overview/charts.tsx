import type { CSSProperties } from "react";
import type {
  DurationEntry,
  Funnel,
  JobStates,
  QuantileBound,
  ResultsSummary,
  ThroughputPoint,
} from "./model";
import { Fragment, memo } from "react";
import {
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
/** 与 src/histogram.rs NETWORK_BOUNDS 一一对应；后端调整桶边界时这里必须同步。 */
const BUCKET_BOUNDS_MS: readonly number[] = [1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000, 180000];
/** 溢出槽位：超过最大桶的分位置于轴末端，不丢弃也不伪造位置。 */
const OVERFLOW_SLOT = BUCKET_BOUNDS_MS.length;
const SLOT_COUNT = BUCKET_BOUNDS_MS.length + 1;
const QUANTILES = ["p50", "p95", "p99"] as const;

function bucketTick(ms: number): string {
  return ms < 1000 ? `${ms}` : `${ms / 1000}s`;
}
/** 溢出槽刻度不标毫秒数，避免与"超过"语义相混。 */
const BUCKET_TICKS = [...BUCKET_BOUNDS_MS.map(bucketTick), "溢出"];

interface SlotPosition {
  index: number;
  state: "bucket" | "overflow" | "unknown";
}
/** 分位值映射到固定槽位；未识别的上界归入排序位置并空心标记，避免静默错位。 */
function slotOf(q: QuantileBound): SlotPosition | null {
  if (q.upperBoundMs !== null) {
    const bound = q.upperBoundMs;
    const exact = BUCKET_BOUNDS_MS.indexOf(bound);
    if (exact !== -1)
      return { index: exact, state: "bucket" };
    const nearest = BUCKET_BOUNDS_MS.findIndex(candidate => candidate >= bound);
    return { index: nearest === -1 ? OVERFLOW_SLOT : nearest, state: "unknown" };
  }
  if (q.exceedsMs !== null)
    return { index: OVERFLOW_SLOT, state: "overflow" };
  return null;
}

function markerStyle(state: SlotPosition["state"], color: string): CSSProperties {
  if (state === "overflow")
    return { background: color, boxShadow: `0 0 0 2px color-mix(in oklab, ${color} 45%, transparent)` };
  if (state === "unknown")
    return { background: "transparent", boxShadow: `inset 0 0 0 1.5px ${color}` };
  return { background: color };
}

/** 桶列参考线只画在刻度区，不延伸到行标签。 */
const GUIDE_LINES = {
  backgroundImage: `repeating-linear-gradient(to right, color-mix(in oklab, var(--border) 55%, transparent) 0 1px, transparent 1px calc(100% / ${SLOT_COUNT}))`,
};

/**
 * 阶段耗时：p50/p95/p99 是固定桶分位，离散槽位与桶一一对应，三阶段共享同一刻度轴。
 * recharts 散点不支持跨行分组标签与逐点状态样式，与 FunnelChart 一样自绘；
 * 每阶段三条子行避免同桶分位重叠，溢出与未识别桶值有显式标记。
 */
export const DurationsChart = memo(({ entries }: { entries: DurationEntry[] }) => {
  if (entries.length === 0)
    return <Empty>采集未启用或指标尚不可用。</Empty>;
  return (
    <>
      <div data-slot="durations-chart" className="@container">
        <div>
          <div className="flex">
            <span className="w-24 shrink-0" />
            <span className="w-8 shrink-0" />
            <div className="grid flex-1 grid-cols-14">
              {BUCKET_TICKS.map((tick, index) => (
                <span
                  key={tick}
                  className={`text-center text-[10px] leading-4 text-muted-foreground tabular-nums${index % 2 === 1 && index !== OVERFLOW_SLOT ? " @max-[430px]:hidden" : ""}`}
                >
                  {tick}
                </span>
              ))}
            </div>
          </div>
          <div>
            {entries.map((entry) => {
              const color = durationColors[entry.timing] ?? "var(--chart-4)";
              const name = timingLabels[entry.timing] ?? entry.timing;
              return (
                <div key={entry.timing} className="mt-2 flex border-t border-border/50 pt-2 first:mt-0 first:border-t-0 first:pt-0">
                  <div className="flex w-24 shrink-0 flex-col justify-center">
                    <span className="text-xs font-medium">{name}</span>
                    <span className="text-[10px] text-muted-foreground tabular-nums">
                      样本
                      {" "}
                      {count(entry.count)}
                    </span>
                  </div>
                  <div className="flex-1">
                    {QUANTILES.map((q) => {
                      const slot = slotOf(entry[q]);
                      return (
                        <div key={q} className="flex h-6 items-center hover:bg-muted/40">
                          <span className="w-8 shrink-0 text-[10px] text-muted-foreground">{q}</span>
                          <div className="relative h-full flex-1" style={GUIDE_LINES}>
                            {slot !== null && (
                              <span
                                data-state={slot.state}
                                title={`${name} ${q}：${quantileLabel(entry[q])}，样本 ${count(entry.count)}`}
                                className="absolute top-1/2 size-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full"
                                style={{ left: `${(((slot.index + 0.5) / SLOT_COUNT) * 100).toFixed(2)}%`, ...markerStyle(slot.state, color) }}
                              />
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
      <p className="text-xs text-muted-foreground">
        圆点位于固定桶刻度（≤ 该值上界；500 及以下为毫秒，1s 起为秒），非精确分位数；带环为超过最大桶，空心为未识别桶值就近放置，空行为无样本；样本数含取消的计时。
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
