import type { ObservationEvent, Snapshot } from "@/lib/observation/contracts";
import { useNavigate } from "@tanstack/react-router";
import { cn } from "cn";
import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { useDisplay, useMonitor } from "@/lib/observation/context";
import { number, record, source } from "@/lib/observation/contracts";
import { count } from "@/lib/observation/format";
import {
  AnimationClock,
  durations,
  LOOKUP_ACTIVE_STEPS,
  ParticlePool,
  quantileLabel,
  STAGES,
  TRANSFER_ACTIVE_STEPS,
} from "./model";
import { ParticleLayer } from "./particle-layer";

const SWEEP_MS = 250;
const RENDER_LIMIT = 200;

/** 粒子颜色图例；色值与 ParticleLayer 渲染保持一致。 */
const PARTICLE_LEGEND = [
  { color: "var(--chart-1)", label: "主动采样" },
  { color: "var(--chart-4)", label: "announce" },
  { color: "var(--status-warning)", label: "历史回填" },
  { color: "var(--status-danger)", label: "失败进入重试" },
];

interface Reading {
  value: string;
  note: string;
  timing?: string;
  disabled?: boolean;
}

/** 泳道窗口统计；缓冲为空时各字段为 undefined，不补零。 */
export interface LaneStats {
  discoveryPerMinute?: number;
  admissionApplied?: number;
  validation?: number;
  commitApplied?: number;
}

/** 泳道读数：只有查找与下载有在途概念，其余泳道展示窗口速率并注明口径。 */
function laneReading(
  stageId: string,
  snapshot: Snapshot | undefined,
  lanes: LaneStats,
): Reading {
  const config = source(snapshot, "config");
  const jobs = record(record(record(snapshot?.cached.database).value).jobs);
  const active = snapshot?.runtime.active ?? [];
  const activeCount = (steps: readonly string[]) =>
    active.filter(a => steps.includes(String(a.step))).length;
  const timing = (name: string) => {
    const entry = durations(snapshot).find(d => d.timing === name);
    return entry ? `p95 ${quantileLabel(entry.p95)}` : undefined;
  };
  const fetchOff = config.fetch === false ? "采集未启用" : undefined;
  switch (stageId) {
    case "discovery":
      return {
        value:
          lanes.discoveryPerMinute === undefined
            ? "—"
            : `${count(lanes.discoveryPerMinute)} 次/分`,
        note:
          config.sample === false
            ? "主动采样未启用，被动 announce 仍计入"
            : "最近 60 秒发现",
        disabled: config.sample === false,
      };
    case "admission":
      return {
        value:
          lanes.admissionApplied === undefined
            ? "—"
            : count(lanes.admissionApplied),
        note: fetchOff ?? "窗口内接纳",
        disabled: fetchOff !== undefined,
      };
    case "claim":
      return {
        value: count(number(jobs.running)),
        note: fetchOff ?? "数据库运行中 · 30 秒刷新",
        disabled: fetchOff !== undefined,
      };
    case "lookup":
      return {
        value: count(activeCount(LOOKUP_ACTIVE_STEPS)),
        note: fetchOff ?? "在途 · 实时快照",
        timing: timing("lookup"),
        disabled: fetchOff !== undefined,
      };
    case "transfer":
      return {
        value: count(activeCount(TRANSFER_ACTIVE_STEPS)),
        note: fetchOff ?? "在途 · 连接至传输",
        timing: timing("tcp_wait"),
        disabled: fetchOff !== undefined,
      };
    case "validation":
      return {
        value: lanes.validation === undefined ? "—" : count(lanes.validation),
        note: fetchOff ?? "窗口内校验",
        disabled: fetchOff !== undefined,
      };
    case "commit":
      return {
        value:
          lanes.commitApplied === undefined ? "—" : count(lanes.commitApplied),
        note: fetchOff ?? "窗口内成功提交",
        disabled: fetchOff !== undefined,
      };
    default:
      return { value: "—", note: "" };
  }
}

export function Pipeline({
  events,
  snapshot,
  lanes,
  selected,
  onSelect,
}: {
  events: ObservationEvent[];
  snapshot: Snapshot | undefined;
  lanes: LaneStats;
  selected: string | null;
  onSelect: (stage: string | null) => void;
}) {
  const monitor = useMonitor();
  const display = useDisplay();
  const navigate = useNavigate();
  const [clock] = useState(() => new AnimationClock());
  const [pool] = useState(() => new ParticlePool(() => clock.time));
  const consumedRef = useRef("0");
  const epochRef = useRef(monitor.epoch);
  const [version, bumpVersion] = useReducer((value: number) => value + 1, 0);
  useEffect(() => {
    clock.setPaused(display.frozen);
    if (!display.frozen)
      clock.tick(performance.now());
  }, [clock, display.frozen]);

  // 数据消费挂在 monitor.revision（≤5Hz）上；冻结时暂停消费与渲染
  useEffect(() => {
    if (display.frozen)
      return;
    let cleared = false;
    if (epochRef.current !== monitor.epoch) {
      epochRef.current = monitor.epoch;
      pool.clear();
      consumedRef.current = "0";
      cleared = true;
    }
    const previous = consumedRef.current;
    const next = pool.consume(
      events,
      previous,
    );
    consumedRef.current = next;
    if (cleared || next !== previous)
      bumpVersion();
  }, [monitor.revision, monitor.epoch, display.frozen, events, pool]);

  // 生命周期清扫以 250ms 间隔驱动；冻结时时钟暂停，粒子寿命不计入冻结时长
  useEffect(() => {
    if (display.frozen)
      return;
    const timer = setInterval(() => {
      if (pool.sweep(clock.tick(performance.now())))
        bumpVersion();
    }, SWEEP_MS);
    return () => clearInterval(timer);
  }, [display.frozen, pool, clock]);

  const visible = useMemo(() => {
    // ParticlePool 原地更新；version 是该选择结果的显式失效键。
    void version;
    const particles = Array.from(pool.particles.values());
    return particles.length > RENDER_LIMIT
      ? particles.sort((a, b) => b.changedAt - a.changedAt).slice(0, RENDER_LIMIT)
      : particles;
  }, [pool, version]);
  const activateParticle = useCallback((hash: string) => {
    void navigate({
      to: "/hashes/$hash",
      params: { hash },
    });
  }, [navigate]);

  return (
    <div>
      <div className="overflow-x-auto">
        <div
          role="group"
          aria-label="流水线泳道"
          className="grid min-w-140 grid-cols-7 gap-1.5"
        >
          {STAGES.map((stage) => {
            const reading = laneReading(stage.id, snapshot, lanes);
            return (
              <button
                key={stage.id}
                type="button"
                className={cn(
                  "flex flex-col gap-1 rounded-md border border-t-[3px] bg-card px-2.5 pt-2.5 pb-2 text-left text-foreground",
                  selected === stage.id
                    ? "border-primary bg-primary/8"
                    : "hover:bg-muted",
                  reading.disabled === true && "opacity-55",
                )}
                style={{ borderTopColor: `var(${stage.color})` }}
                aria-pressed={selected === stage.id}
                onClick={() =>
                  onSelect(selected === stage.id ? null : stage.id)}
              >
                <span className="text-xs font-semibold">{stage.title}</span>
                <span className="font-[620] text-lg tabular-nums">{reading.value}</span>
                <span className="text-[10px] text-muted-foreground">{reading.note}</span>
                {reading.timing !== undefined && (
                  <span className="text-[10px] text-muted-foreground">
                    {reading.timing}
                  </span>
                )}
              </button>
            );
          })}
        </div>
        <ParticleLayer
          particles={visible}
          frozen={display.frozen}
          onActivate={activateParticle}
        />
      </div>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
        {PARTICLE_LEGEND.map(item => (
          <span key={item.label} className="inline-flex items-center gap-1.5">
            <span
              aria-hidden
              className="size-2 rounded-full"
              style={{ backgroundColor: item.color }}
            />
            {item.label}
          </span>
        ))}
      </div>
    </div>
  );
}
