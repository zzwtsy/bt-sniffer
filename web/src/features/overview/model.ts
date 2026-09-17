import type { ObservationEvent, Snapshot } from "@/lib/observation/contracts";
import { number, record, rows, source } from "@/lib/observation/contracts";
import { duration } from "@/lib/observation/format";

/** 七泳道定义；颜色只引用 CSS token 名，由画布经 getComputedStyle 解析。 */
export interface Stage {
  id: string;
  title: string;
  kinds: readonly string[];
  color: string;
}
export const STAGES: readonly Stage[] = [
  { id: "discovery", title: "发现", kinds: ["sampling", "discovery"], color: "--chart-1" },
  { id: "admission", title: "接纳", kinds: ["admission"], color: "--chart-2" },
  { id: "claim", title: "领取", kinds: ["job"], color: "--chart-3" },
  { id: "lookup", title: "查找", kinds: ["lookup", "rpc"], color: "--chart-4" },
  { id: "transfer", title: "下载", kinds: ["peer", "piece"], color: "--chart-5" },
  { id: "validation", title: "校验", kinds: ["validation"], color: "--status-warning" },
  { id: "commit", title: "提交", kinds: ["commit"], color: "--status-success" },
];
/** runtime.active 的 step 只覆盖以下阶段；其余泳道没有在途概念。 */
export const LOOKUP_ACTIVE_STEPS: readonly string[] = ["lookup"];
export const TRANSFER_ACTIVE_STEPS: readonly string[] = [
  "connect",
  "handshake",
  "extension",
  "transfer",
];
export const JOB_STATES = [
  "pending",
  "running",
  "retry_wait",
  "dormant",
  "succeeded",
] as const;

/** metadata 事务终态是提交统计的唯一口径；metadata 事件只是同次提交的附加证据。 */
export function isCommitFinished(event: ObservationEvent): boolean {
  return event.kind === "commit" && event.step === "complete_transaction" && event.result !== "started";
}
export function isCommitApplied(event: ObservationEvent): boolean {
  return isCommitFinished(event) && event.result === "applied";
}

/** 生命周期时钟；暂停后保留最后时间，恢复不计入冻结时长。 */
export class AnimationClock {
  time = 0;
  private previous: number | undefined;
  private paused = false;

  setPaused(paused: boolean) {
    if (this.paused === paused)
      return;
    this.paused = paused;
    this.previous = undefined;
  }

  tick(now: number): number {
    if (!this.paused) {
      if (this.previous !== undefined)
        this.time += Math.max(0, now - this.previous);
      this.previous = now;
    }
    return this.time;
  }
}

/** 与 Status 组件保持一致的失败判定。 */
const FAILURE = /failed|error|invalid|mismatch|timeout/;

export type Source = "sample" | "announce" | "backfill" | "neutral";
/** 来源着色只依据明确字段；无法判断时保持中性，不猜测。 */
export function sourceOf(event: ObservationEvent): Source {
  if (event.kind === "discovery" && event.step === "announce")
    return "announce";
  if (event.kind === "discovery" && event.step === "hash_saved")
    return "sample";
  if (event.kind === "admission" && event.step === "backfill")
    return "backfill";
  if (
    event.kind === "job"
    && event.step === "claim"
    && record(event.data).attempt_kind === "repeat"
  ) {
    return "backfill";
  }
  return "neutral";
}

export interface Particle {
  hash: string;
  /** 当前泳道下标（STAGES 顺序）。 */
  stage: number;
  /** 最近一次阶段或状态变化的时钟毫秒。 */
  changedAt: number;
  source: Source;
  /** 失败结果坠入重试区。 */
  falling: boolean;
  /** 已提交，离场后由 sweep 回收；容量淘汰直接移除。 */
  leaving: boolean;
  leavingAt: number;
  generation?: number;
  sequence: string;
}

const CLAIM_STAGE = STAGES.findIndex(s => s.id === "claim");
const COMMIT_STAGE = STAGES.findIndex(s => s.id === "commit");
const FALL_TTL = 4000;
const LEAVE_TTL = 800;
const IDLE_TTL = 30_000;

/**
 * 粒子状态机：只消费事件流，不接触 DOM。
 * 窗口中段进入的 hash 直接在当前阶段诞生；动画只是窗口内示意，不构成审计依据。
 */
export class ParticlePool {
  static readonly LIMIT = 500;
  static readonly BATCH = 100;
  readonly particles = new Map<string, Particle>();
  /** 已消费的 metadata 事务成功计数（受动画批次上限影响）。 */
  committed = 0;
  /** 因单批上限丢弃的增量事件数。 */
  dropped = 0;
  private readonly clock: () => number;
  readonly limit: number;
  readonly batch: number;
  constructor(
    clock: () => number = () => Date.now(),
    limit: number = ParticlePool.LIMIT,
    batch: number = ParticlePool.BATCH,
  ) {
    this.clock = clock;
    this.limit = limit;
    this.batch = batch;
  }

  /** 增量消费 sequence 严格大于 lastConsumed 的事件，返回新的消费游标。 */
  consume(events: ObservationEvent[], lastConsumed: string): string {
    const after = BigInt(lastConsumed);
    const fresh = events.filter(e => BigInt(e.sequence) > after);
    if (fresh.length === 0)
      return lastConsumed;
    const overflow = fresh.length - this.batch;
    if (overflow > 0)
      this.dropped += overflow;
    const batch = overflow > 0 ? fresh.slice(overflow) : fresh;
    for (const event of batch) this.apply(event);
    return batch.at(-1)!.sequence;
  }

  private apply(event: ObservationEvent) {
    if (event.kind === "commit" && event.step !== "complete_transaction")
      return;
    const now = this.clock();
    if (isCommitApplied(event))
      this.committed++;
    const hash = event.context.hash;
    if (hash === undefined || hash === "")
      return;
    const stage = STAGES.findIndex(s => s.kinds.includes(event.kind));
    const isRetry = event.kind === "retry";
    if (stage === -1 && !isRetry)
      return;
    const failed
      = FAILURE.test(event.result)
        || (event.kind === "commit"
          && event.result !== "applied"
          && event.result !== "started");
    let particle = this.particles.get(hash);
    if (particle?.leaving) {
      this.particles.delete(hash);
      particle = undefined;
    }
    if (!particle) {
      if (this.limit <= 0)
        return;
      while (this.particles.size >= this.limit)
        this.particles.delete(this.particles.keys().next().value!);
      particle = {
        hash,
        stage,
        changedAt: now,
        source: "neutral",
        falling: false,
        leaving: false,
        leavingAt: 0,
        sequence: event.sequence,
      };
      this.particles.set(hash, particle);
    }
    if (event.context.generation !== undefined)
      particle.generation = event.context.generation;
    const origin = sourceOf(event);
    if (origin !== "neutral")
      particle.source = origin;
    // 重新发现、重试与再次领取都使粒子重新流动
    const revive
      = event.kind === "discovery"
        || event.kind === "retry"
        || (event.kind === "job" && event.step === "claim" && event.result === "applied");
    if (particle.falling && !revive) {
      particle.sequence = event.sequence;
      return;
    }
    const target = isRetry ? CLAIM_STAGE : stage;
    if (target !== particle.stage || particle.falling) {
      particle.stage = target;
      particle.changedAt = now;
    }
    particle.falling = failed;
    if (failed)
      particle.changedAt = now;
    if (isCommitApplied(event)) {
      particle.stage = COMMIT_STAGE;
      particle.leaving = true;
      particle.leavingAt = now;
    }
    particle.sequence = event.sequence;
  }

  /** 回收离场、坠落超时与长期空闲的粒子。 */
  sweep(now: number): boolean {
    let removed = false;
    for (const [hash, particle] of this.particles) {
      if (particle.leaving && now - particle.leavingAt > LEAVE_TTL) {
        this.particles.delete(hash);
        removed = true;
      } else if (particle.falling && now - particle.changedAt > FALL_TTL) {
        this.particles.delete(hash);
        removed = true;
      } else if (
        !particle.leaving
        && !particle.falling
        && now - particle.changedAt > IDLE_TTL
      ) {
        this.particles.delete(hash);
        removed = true;
      }
    }
    return removed;
  }

  clear() {
    this.particles.clear();
    this.committed = 0;
    this.dropped = 0;
  }
}

export interface Funnel {
  levels: { id: string; title: string; count: number }[];
}
/** 四级窗口计数；空缓冲返回 undefined，不补零。 */
export function summarizeOverview(events: ObservationEvent[], now: number) {
  let discoveries = 0;
  let recent = 0;
  let admissions = 0;
  let claims = 0;
  let validation = 0;
  let commitApplied = 0;
  let commitFinished = 0;
  for (const event of events) {
    if (event.kind === "discovery") {
      discoveries++;
      if (event.at_ms >= now - 60_000)
        recent++;
    }
    if (event.kind === "admission" && event.result === "applied")
      admissions++;
    if (event.kind === "job" && event.step === "claim" && event.result === "applied")
      claims++;
    if (event.kind === "validation")
      validation++;
    if (isCommitFinished(event)) {
      commitFinished++;
      if (isCommitApplied(event))
        commitApplied++;
    }
  }
  const present = events.length > 0;
  const funnel: Funnel | undefined = present
    ? {
        levels: [
          { id: "discovery", title: "发现", count: discoveries },
          { id: "admission", title: "接纳", count: admissions },
          { id: "claim", title: "领取", count: claims },
          { id: "commit", title: "提交", count: commitApplied },
        ],
      }
    : undefined;
  return {
    funnel,
    perMinute: present ? recent : undefined,
    commitApplied,
    commitFinished,
    lanes: {
      discoveryPerMinute: present ? recent : undefined,
      admissionApplied: present ? admissions : undefined,
      validation: present ? validation : undefined,
      commitApplied: present ? commitApplied : undefined,
    },
  };
}
export function funnel(events: ObservationEvent[]): Funnel | undefined {
  return summarizeOverview(events, Date.now()).funnel;
}

export interface ResultBucket {
  result: string;
  count: number;
}
const RESULT_SUCCESS = /applied|succeeded|validated|ready|good/;
const FAILURE_TOP = 5;
export interface ResultsSummary {
  total: number;
  failed: number;
  succeeded: number;
  other: number;
  /** 失败原因按计数降序，最多前 5 名。 */
  failures: ResultBucket[];
}
/**
 * 事件窗口内的结果健康度汇总：失败 / 正常完成 / 进行中与其他，附主要失败原因。
 * 分类正则与 Status 徽章一致；空缓冲或过滤后无匹配返回 undefined，不补零。
 */
export function summarizeResults(
  events: ObservationEvent[],
  kinds: readonly string[] | null,
): ResultsSummary | undefined {
  if (events.length === 0)
    return undefined;
  let failed = 0;
  let succeeded = 0;
  let other = 0;
  const failureCounts = new Map<string, number>();
  for (const event of events) {
    if (kinds !== null && !kinds.includes(event.kind))
      continue;
    if (FAILURE.test(event.result)) {
      failed++;
      failureCounts.set(
        event.result,
        (failureCounts.get(event.result) ?? 0) + 1,
      );
    } else if (RESULT_SUCCESS.test(event.result)) {
      succeeded++;
    } else {
      other++;
    }
  }
  if (failed + succeeded + other === 0)
    return undefined;
  const failures = [...failureCounts.entries()]
    .map(([result, count]) => ({ result, count }))
    .sort((a, b) =>
      b.count !== a.count ? b.count - a.count : a.result.localeCompare(b.result))
    .slice(0, FAILURE_TOP);
  return { total: failed + succeeded + other, failed, succeeded, other, failures };
}

export interface JobStates {
  states: { state: (typeof JOB_STATES)[number]; count: number }[];
  observedAt: number | undefined;
  stale: boolean;
}
/** 数据库 30 秒刷新的任务状态统计；数据库缺失时返回 undefined。 */
export function jobStates(snapshot: Snapshot | undefined): JobStates | undefined {
  const database = record(snapshot?.cached.database);
  if (database.available !== true)
    return undefined;
  const jobs = record(record(database.value).jobs);
  const states: JobStates["states"] = [];
  for (const state of JOB_STATES) {
    const count = number(jobs[state]);
    if (count === undefined || !Number.isInteger(count) || count < 0)
      return undefined;
    states.push({ state, count });
  }
  return {
    states,
    observedAt: number(database.observed_at_ms),
    stale: database.stale === true,
  };
}

export interface QuantileBound {
  upperBoundMs: number | null;
  exceedsMs: number | null;
}
export interface DurationEntry {
  timing: string;
  count: number | undefined;
  p50: QuantileBound;
  p95: QuantileBound;
  p99: QuantileBound;
}
/** 分位是固定桶上界；空样本两个字段均为 null，绝不伪装精确值。 */
function quantile(value: unknown): QuantileBound {
  const q = record(value);
  return {
    upperBoundMs: typeof q.upper_bound_ms === "number" ? q.upper_bound_ms : null,
    exceedsMs: typeof q.exceeds_ms === "number" ? q.exceeds_ms : null,
  };
}
export function durations(snapshot: Snapshot | undefined): DurationEntry[] {
  const metrics = record(source(snapshot, "collector").metrics);
  return rows(metrics.durations).map(row => ({
    timing: typeof row.timing === "string" ? row.timing : "未知",
    count: number(row.count),
    p50: quantile(row.p50),
    p95: quantile(row.p95),
    p99: quantile(row.p99),
  }));
}
export function quantileLabel(q: QuantileBound): string {
  if (q.upperBoundMs !== null)
    return `≤${duration(q.upperBoundMs)}`;
  if (q.exceedsMs !== null)
    return `>${duration(q.exceedsMs)}`;
  return "无样本";
}

/** 最近 60 秒窗口内的发现事件数，作为"发现 / 分"速率。 */
export function discoveryPerMinute(
  events: ObservationEvent[],
  now: number,
): number {
  let count = 0;
  for (const event of events) {
    if (event.kind === "discovery" && event.at_ms >= now - 60_000)
      count++;
  }
  return count;
}

export interface ThroughputPoint {
  at: number;
  /** 发现 / 分；null 表示断线或换 run 的中断区间。 */
  discoveries: number | null;
  /** 提交 / 秒；null 语义同 trends。 */
  commits: number | null;
}
/** 双折线吞吐序列：900 点上限、15 分钟窗口、不连续留 gap、不补零。 */
export class ThroughputSeries {
  static readonly LIMIT = 900;
  points: ThroughputPoint[] = [];
  add(at: number, discoveries: number | null, commits: number | null) {
    const last = this.points.at(-1);
    if (
      last
      && last.discoveries !== null
      && Math.floor(last.at / 1000) === Math.floor(at / 1000)
    ) {
      this.points = [...this.points.slice(0, -1), { at, discoveries, commits }];
    } else {
      this.points = [...this.points, { at, discoveries, commits }];
    }
    this.points = this.points
      .filter(p => p.at >= at - 900_000)
      .slice(-ThroughputSeries.LIMIT);
  }

  /** 断线区间只插入一个空点，后续有效点自动恢复。 */
  gap(at: number) {
    const last = this.points.at(-1);
    if (last && last.discoveries === null)
      return;
    this.points = [
      ...this.points,
      { at, discoveries: null, commits: null },
    ].slice(-ThroughputSeries.LIMIT);
  }

  clear() {
    this.points = [];
  }
}
