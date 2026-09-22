import { Activity, Gauge, Network, Percent, Radar, Upload } from "lucide-react";
import { useMemo, useState } from "react";
import {
  Chip,
  Freshness,
  Metric,
  PageTitle,
  Panel,
} from "@/components/observation/common";
import { useDisplayed, useEngine, useMonitor } from "@/lib/observation/context";
import { record, rows, source } from "@/lib/observation/contracts";
import { count, label } from "@/lib/observation/format";
import {
  DurationsChart,
  FunnelChart,
  JobStatesChart,
  ResultsChart,
} from "./charts";
import { Pipeline } from "./lanes";
import {
  durations,
  jobStates,
  STAGES,
  summarizeOverview,
  summarizeResults,
} from "./model";
import { Throughput } from "./throughput";

export function OverviewPage() {
  const monitor = useMonitor();
  const engine = useEngine();
  const snapshot = useDisplayed("overview:snapshot", monitor.snapshot);
  const points = useDisplayed("overview:points", engine.trends.points);
  const [stageFilter, setStageFilter] = useState<string | null>(null);

  // 缓冲事件随 monitor.revision（≤5Hz）派生；冻结时展示派生结果的冻结副本
  const bufferEvents = useMemo(
    () => engine.buffer.select({}, engine.buffer.limits.count),
    // eslint-disable-next-line react/exhaustive-deps -- buffer 为非响应式，由 monitor.revision 通知重算
    [engine, monitor.revision],
  );
  const derived = useDisplayed(
    "overview:derived",
    useMemo(() => summarizeOverview(bufferEvents, Date.now()), [bufferEvents]),
  );

  // 泳道过滤：右侧结果分布按 kind 集合聚合；过滤状态在面板右上角可见
  const filterKinds = stageFilter !== null
    ? (STAGES.find(s => s.id === stageFilter)?.kinds ?? null)
    : null;
  const results = useDisplayed(
    "overview:results",
    useMemo(
      () => summarizeResults(bufferEvents, filterKinds),
      [bufferEvents, filterKinds],
    ),
  );

  const nodes = rows(snapshot?.cached.nodes);
  const collector = source(snapshot, "collector");
  const config = source(snapshot, "config");
  const database = record(snapshot?.cached.database);
  const jobs = useMemo(() => jobStates(snapshot), [snapshot]);
  const durationEntries = useMemo(() => durations(snapshot), [snapshot]);
  const active = snapshot?.runtime.active ?? [];
  const paused = [
    collector.capacity_paused === true ? "capacity" : null,
    collector.backlog_paused === true ? "local_wait" : null,
    collector.storage_paused === true ? "failed" : null,
  ].filter((p): p is string => p !== null);
  const lastCommit = points.findLast(p => p.commits !== null)?.commits;
  const commitRate = lastCommit ?? undefined;
  const successRate
    = derived.commitFinished > 0
      ? derived.commitApplied / derived.commitFinished
      : undefined;
  return (
    <>
      <PageTitle
        eyebrow="PIPELINE"
        title="采集流水线"
        description="发现 → 接纳 → 领取 → 查找 → 下载 → 校验 → 提交；泳道动画仅为当前窗口示意。"
      />
      <div className="grid grid-cols-6 gap-4 max-[1200px]:grid-cols-3 max-[1200px]:gap-2.5 max-[760px]:grid-cols-2">
        <Metric
          title="发现速率"
          value={
            derived.perMinute === undefined ? "—" : `${derived.perMinute} 次/分`
          }
          detail="最近 60 秒的发现事件数"
          hint="统计自浏览器事件缓冲窗口，窗口外历史不计入"
          icon={<Radar size={17} />}
        />
        <Metric
          title="活跃任务"
          value={count(snapshot ? active.length : undefined)}
          detail="当前在途的查找与下载任务"
          hint="来自 runtime.active 快照，按任务当前阶段统计"
          icon={<Activity size={17} />}
        />
        <Metric
          title="提交吞吐"
          value={
            commitRate === undefined ? "—" : `${commitRate.toFixed(2)} 次/秒`
          }
          detail="本次连接以来的平均速率"
          hint="由快照提交计数差分得出"
          icon={<Upload size={17} />}
        />
        <Metric
          title="成功率"
          value={
            successRate === undefined
              ? "—"
              : `${(successRate * 100).toFixed(1)}%`
          }
          detail={`窗口内完结提交 ${count(derived.commitFinished)} 次`}
          hint="成功提交 ÷ 完结提交；仅覆盖当前事件窗口，不代表全量历史"
          icon={<Percent size={17} />}
        />
        <Metric
          title="DHT 节点"
          value={
            snapshot
              ? `${nodes.filter(n => n.available === true).length} / ${nodes.length}`
              : "—"
          }
          detail="可用 / 总数"
          hint="IPv4 与 IPv6 节点分别观察"
          icon={<Network size={17} />}
        />
        <Metric
          title="采集背压"
          value={
            collector.running_workers === undefined || config.fetch === false
              ? "—"
              : paused.length === 0
                ? "运行中"
                : `已暂停 · ${paused.map(p => label(p)).join(" · ")}`
          }
          detail={
            config.fetch === false
              ? "采集未启用"
              : paused.length === 0
                ? "容量、积压、存储均未触发暂停"
                : "后端背压策略暂停中"
          }
          icon={<Gauge size={17} />}
        />
      </div>
      <div className="mx-0.5 mt-2.5 mb-5 flex flex-wrap justify-between gap-3">
        <Freshness
          at={snapshot?.runtime.sources.collector?.observed_at_ms}
          source="采集快照"
        />
        <Freshness
          at={database.observed_at_ms}
          stale={database.stale === true}
          source="数据库统计"
        />
      </div>
      <div className="grid grid-cols-[minmax(0,3fr)_minmax(320px,2fr)] gap-5 max-[1200px]:grid-cols-1">
        <Panel
          title="七泳道流水线"
          description="点击泳道过滤右侧结果分布；粒子仅为窗口内的视觉示意。"
          hint="读数口径：发现、接纳、校验、提交为事件窗口计数；领取为数据库 30 秒统计；查找、下载为实时快照在途数。"
          className="min-w-0"
        >
          <Pipeline
            events={bufferEvents}
            snapshot={snapshot}
            lanes={derived.lanes}
            selected={stageFilter}
            onSelect={setStageFilter}
          />
        </Panel>
        <Panel
          title="事件结果分布"
          description={`当前窗口保留最近 ${count(monitor.events)} 条事件（已丢弃 ${count(monitor.evicted)} 条），按结果归类。`}
          className="min-w-0"
          action={stageFilter !== null && (
            <Chip selected onClick={() => setStageFilter(null)}>
              已过滤：
              {STAGES.find(s => s.id === stageFilter)?.title}
              {" ×"}
            </Chip>
          )}
        >
          <ResultsChart data={results} />
        </Panel>
      </div>
      <div className="grid grid-cols-2 gap-5 max-[1200px]:grid-cols-1">
        <Panel
          title="转化漏斗"
          description="事件窗口内各阶段的数量与逐级转化率。"
          className="min-w-0"
        >
          <FunnelChart data={derived.funnel} />
        </Panel>
        <Panel
          title="吞吐曲线"
          description="每分钟发现数与每秒提交数；断线区间留空不补零。"
          className="min-w-0"
        >
          <Throughput
            events={bufferEvents}
            phase={monitor.phase}
            epoch={monitor.epoch}
            commits={engine.trends.points.at(-1)?.commits ?? null}
          />
        </Panel>
        <Panel
          title="阶段耗时分布"
          description="查找、TCP 等待、首个 peer 的 p50 / p95 / p99 耗时。"
          className="min-w-0"
        >
          <DurationsChart entries={durationEntries} />
        </Panel>
        <Panel
          title="任务状态"
          description="数据库 30 秒统计；图例展示当前各状态数量。"
          className="min-w-0"
        >
          <div className="mb-2 flex flex-wrap items-center gap-2.5">
            <Freshness at={jobs?.observedAt} stale={jobs?.stale} source="数据库统计" />
          </div>
          <JobStatesChart data={jobs} />
        </Panel>
      </div>
    </>
  );
}
