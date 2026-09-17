import { expect, it } from "vitest";
import { eventSchema, snapshotSchema } from "@/lib/observation/contracts";
import {
  AnimationClock,
  discoveryPerMinute,
  durations,
  funnel,
  isCommitApplied,
  isCommitFinished,
  jobStates,
  ParticlePool,
  quantileLabel,
  STAGES,
  summarizeResults,
  ThroughputSeries,
} from "./model";

function event(
  sequence: number,
  kind: string,
  step: string,
  result: string,
  options: { hash?: string; data?: Record<string, unknown>; at?: number } = {},
) {
  return eventSchema.parse({
    schema_version: 1,
    run_id: "run",
    sequence: String(sequence),
    at_ms: options.at ?? 1000 + sequence,
    kind,
    step,
    result,
    context: { hash: options.hash ?? "a".repeat(40) },
    data: options.data ?? {},
    truncated: false,
  });
}
function snapshot(database: unknown, durationsValue?: unknown) {
  return snapshotSchema.parse({
    schema_version: 1,
    window: {
      run_id: "run",
      oldest: "1",
      latest: "3",
      retained: 3,
      bytes: 1000,
      evicted: 0,
      truncated: 0,
    },
    runtime: {
      sources: {
        collector: {
          observed_at_ms: 1000,
          value: { metrics: { durations: durationsValue ?? [] } },
        },
      },
      active: [],
    },
    cached: database === undefined ? {} : { database },
  });
}
const other = "b".repeat(40);

it("发现诞生粒子并按泳道推进，来源着色只依据明确字段", () => {
  let now = 5000;
  const pool = new ParticlePool(() => now);
  let cursor = "0";
  cursor = pool.consume(
    [event(1, "discovery", "hash_saved", "new")],
    cursor,
  );
  expect(cursor).toBe("1");
  const particle = pool.particles.get("a".repeat(40))!;
  expect(particle.stage).toBe(0);
  expect(particle.source).toBe("sample");
  now += 100;
  pool.consume(
    [
      event(2, "admission", "announce_save", "applied"),
      event(3, "job", "claim", "applied", { data: { attempt_kind: "first" } }),
      event(4, "lookup", "lookup", "started"),
      event(5, "peer", "connect", "started"),
      event(6, "piece", "receive", "accepted"),
      event(7, "validation", "validate", "validated"),
    ],
    cursor,
  );
  expect(particle.stage).toBe(5);
  expect(particle.source).toBe("sample");
  expect(particle.falling).toBe(false);
});

it("announce 与重复领取着色，未知步骤回退中性", () => {
  const pool = new ParticlePool(() => 0);
  pool.consume([event(1, "discovery", "announce", "received")], "0");
  expect(pool.particles.get("a".repeat(40))!.source).toBe("announce");
  pool.consume(
    [event(2, "job", "claim", "applied", { data: { attempt_kind: "repeat" } })],
    "1",
  );
  expect(pool.particles.get("a".repeat(40))!.source).toBe("backfill");
  pool.consume(
    [event(3, "discovery", "save", "applied", { hash: other })],
    "2",
  );
  expect(pool.particles.get(other)!.source).toBe("neutral");
});

it("失败坠落、重试与再次领取复活、applied 提交计数并离场", () => {
  let now = 1000;
  const pool = new ParticlePool(() => now);
  let cursor = pool.consume(
    [
      event(1, "discovery", "hash_saved", "new"),
      event(2, "peer", "handshake", "failed"),
    ],
    "0",
  );
  const particle = pool.particles.get("a".repeat(40))!;
  expect(particle.falling).toBe(true);
  expect(particle.stage).toBe(4);
  // 坠落中同 hash 的非复活事件不改变状态
  cursor = pool.consume([event(3, "lookup", "lookup", "started")], cursor);
  expect(particle.falling).toBe(true);
  // retry 重回领取道
  now += 100;
  cursor = pool.consume([event(4, "retry", "schedule", "applied")], cursor);
  expect(particle.falling).toBe(false);
  expect(particle.stage).toBe(2);
  // 校验失败再次坠落
  cursor = pool.consume(
    [event(5, "validation", "validate", "hash_mismatch")],
    cursor,
  );
  expect(particle.falling).toBe(true);
  // 再次领取复活
  cursor = pool.consume(
    [event(6, "job", "claim", "applied", { data: { attempt_kind: "repeat" } })],
    cursor,
  );
  expect(particle.falling).toBe(false);
  expect(particle.stage).toBe(2);
  // 非 applied 提交坠落且不计数
  cursor = pool.consume([event(7, "commit", "complete_transaction", "stale")], cursor);
  expect(particle.falling).toBe(true);
  expect(pool.committed).toBe(0);
  // applied 提交计数并标记离场，sweep 回收
  cursor = pool.consume(
    [event(8, "job", "claim", "applied", { data: { attempt_kind: "repeat" } })],
    cursor,
  );
  pool.consume([event(9, "commit", "complete_transaction", "applied")], cursor);
  expect(pool.committed).toBe(1);
  expect(particle.leaving).toBe(true);
  expect(pool.sweep(now + 100)).toBe(false);
  expect(pool.particles.size).toBe(1);
  expect(pool.sweep(now + 10_000)).toBe(true);
  expect(pool.particles.size).toBe(0);
});

it("坠落与空闲粒子由 sweep 回收", () => {
  const pool = new ParticlePool(() => 0);
  pool.consume(
    [
      event(1, "discovery", "hash_saved", "new"),
      event(2, "peer", "connect", "timeout", { hash: other }),
    ],
    "0",
  );
  expect(pool.sweep(3999)).toBe(false);
  expect(pool.particles.size).toBe(2);
  expect(pool.sweep(4001)).toBe(true);
  expect(pool.particles.get(other)).toBeUndefined();
  expect(pool.sweep(30_001)).toBe(true);
  expect(pool.particles.size).toBe(0);
});

it("泳道事件按字符串序号 BigInt 增量消费，超出单批上限丢弃最旧", () => {
  const pool = new ParticlePool(() => 0, 500, 5);
  const events = Array.from({ length: 12 }, (_, i) =>
    event(i + 1, "discovery", "hash_saved", "new", {
      hash: String(i).padStart(40, "0"),
    }));
  const cursor = pool.consume(events.slice(0, 9), "0");
  expect(cursor).toBe("9");
  expect(pool.particles.size).toBe(5);
  expect(pool.dropped).toBe(4);
  // "10" 必须大于 "9"，不能按字符串字典序比较
  const next = pool.consume(events.slice(9), cursor);
  expect(next).toBe("12");
  expect(pool.dropped).toBe(4);
  expect(pool.consume([], next)).toBe("12");
});

it("粒子池连续洪峰仍严格有界，淘汰最早进入的粒子", () => {
  let now = 0;
  const pool = new ParticlePool(() => now);
  let cursor = "0";
  for (let batch = 0; batch < 160; batch++) {
    now = batch * 200;
    const events = Array.from({ length: 100 }, (_, i) => {
      const sequence = batch * 100 + i + 1;
      return event(sequence, "discovery", "hash_saved", "new", {
        hash: sequence.toString(16).padStart(40, "0"),
      });
    });
    cursor = pool.consume(events, cursor);
    expect(pool.particles.size).toBeLessThanOrEqual(500);
    pool.sweep(now);
    expect(cursor).toBe(String((batch + 1) * 100));
  }
  expect(pool.particles.size).toBe(500);
  expect(pool.particles.has("1".padStart(40, "0"))).toBe(false);
  expect(pool.particles.has((16000).toString(16).padStart(40, "0"))).toBe(true);
  const small = new ParticlePool(() => 0, 3);
  small.consume(Array.from({ length: 10 }, (_, i) => event(i + 1, "discovery", "hash_saved", "new", { hash: String(i) })), "0");
  expect([...small.particles.keys()]).toEqual(["7", "8", "9"]);
});

it("无 hash 事件不形成粒子，但 applied 提交仍计数", () => {
  const pool = new ParticlePool(() => 0);
  pool.consume(
    [
      event(1, "sampling", "batch_segment", "sent", { hash: "" }),
      event(2, "commit", "complete_transaction", "applied", { hash: "" }),
      event(3, "lifecycle", "start", "started", { hash: "" }),
    ],
    "0",
  );
  expect(pool.particles.size).toBe(0);
  expect(pool.committed).toBe(1);
});

it("漏斗为空缓冲返回 undefined，否则给四级窗口计数", () => {
  expect(funnel([])).toBeUndefined();
  const result = funnel([
    event(1, "discovery", "hash_saved", "new"),
    event(2, "discovery", "announce", "received", { hash: other }),
    event(3, "admission", "announce_save", "applied"),
    event(4, "admission", "announce_save", "capacity", { hash: other }),
    event(5, "job", "claim", "applied"),
    event(6, "job", "claim", "stale", { hash: other }),
    event(7, "commit", "complete_transaction", "applied"),
  ])!;
  expect(result.levels.map(l => l.count)).toEqual([2, 1, 1, 1]);
});

it("任务状态透传数据库统计，数据库缺失返回 undefined", () => {
  expect(jobStates(undefined)).toBeUndefined();
  expect(jobStates(snapshot(undefined))).toBeUndefined();
  const result = jobStates(
    snapshot({
      available: true,
      stale: true,
      observed_at_ms: 1234,
      value: { jobs: { pending: 2, running: 1, retry_wait: 3, dormant: 0, succeeded: 4 } },
    }),
  )!;
  expect(result.stale).toBe(true);
  expect(result.observedAt).toBe(1234);
  expect(result.states.map(s => s.count)).toEqual([2, 1, 3, 0, 4]);
});

it("耗时分位透传桶上界与溢出语义，空样本不伪造数值", () => {
  const entries = durations(
    snapshot(undefined, [
      {
        timing: "lookup",
        count: 12,
        p50: { upper_bound_ms: 500, exceeds_ms: null },
        p95: { upper_bound_ms: null, exceeds_ms: 60000 },
        p99: { upper_bound_ms: null, exceeds_ms: null },
      },
    ]),
  );
  expect(entries).toHaveLength(1);
  expect(entries[0].timing).toBe("lookup");
  expect(quantileLabel(entries[0].p50)).toBe("≤500 ms");
  expect(quantileLabel(entries[0].p95)).toBe(">60.00 s");
  expect(quantileLabel(entries[0].p99)).toBe("无样本");
  expect(durations(snapshot(undefined))).toEqual([]);
});

it("结果汇总按健康度归类并列出主要失败原因", () => {
  const events = [
    event(1, "discovery", "hash_saved", "new"),
    event(2, "commit", "complete_transaction", "applied"),
    event(3, "commit", "complete_transaction", "applied"),
    event(4, "commit", "complete_transaction", "failed"),
    event(5, "rpc", "query", "sent"),
    event(6, "lookup", "lookup", "timeout"),
  ];
  expect(summarizeResults([], null)).toBeUndefined();
  expect(summarizeResults(events, null)).toEqual({
    total: 6,
    failed: 2,
    succeeded: 2,
    other: 2,
    failures: [
      { result: "failed", count: 1 },
      { result: "timeout", count: 1 },
    ],
  });
  expect(summarizeResults(events, ["commit"])).toEqual({
    total: 3,
    failed: 1,
    succeeded: 2,
    other: 0,
    failures: [{ result: "failed", count: 1 }],
  });
  expect(summarizeResults(events, ["validation"])).toBeUndefined();
});

it("发现速率只统计最近 60 秒窗口", () => {
  const now = 120_000;
  const events = [
    event(1, "discovery", "hash_saved", "new", { at: now - 59_000 }),
    event(2, "discovery", "hash_saved", "new", { at: now - 61_000 }),
    event(3, "commit", "complete_transaction", "applied", { at: now - 1000 }),
  ];
  expect(discoveryPerMinute(events, now)).toBe(1);
});

it("吞吐序列同秒去重、gap 留空、900 点上限", () => {
  const series = new ThroughputSeries();
  series.add(1000, 10, 0.5);
  series.add(1500, 12, 0.6);
  expect(series.points).toHaveLength(1);
  expect(series.points[0]).toEqual({ at: 1500, discoveries: 12, commits: 0.6 });
  series.gap(2000);
  series.gap(3000);
  expect(series.points).toHaveLength(2);
  expect(series.points[1].discoveries).toBeNull();
  series.add(4000, 5, null);
  expect(series.points).toHaveLength(3);
  for (let i = 0; i < 2000; i++) series.add(10_000 + i * 1000, i, null);
  expect(series.points.length).toBe(900);
});

it("七泳道定义与事件领域映射保持契约", () => {
  expect(STAGES.map(s => s.id)).toEqual([
    "discovery",
    "admission",
    "claim",
    "lookup",
    "transfer",
    "validation",
    "commit",
  ]);
  const kinds = STAGES.flatMap(s => s.kinds);
  for (const kind of [
    "sampling",
    "discovery",
    "admission",
    "job",
    "lookup",
    "rpc",
    "peer",
    "piece",
    "validation",
    "commit",
  ]) expect(kinds).toContain(kind);
});

it("提交只计事务终态，不重复计算 metadata 证据或 hashes 保存", () => {
  const events = [
    event(1, "commit", "complete_transaction", "started"),
    event(2, "commit", "metadata", "applied"),
    event(3, "commit", "complete_transaction", "applied"),
    event(4, "commit", "complete_transaction", "failed"),
    event(5, "commit", "hashes", "applied"),
  ];
  expect(events.filter(isCommitApplied).length / events.filter(isCommitFinished).length).toBe(0.5);
  expect(funnel(events)!.levels.at(-1)!.count).toBe(1);
  const pool = new ParticlePool(() => 0);
  pool.consume(events.slice(0, 3), "0");
  const particle = pool.particles.values().next().value!;
  pool.consume([event(6, "commit", "metadata", "applied")], "3");
  expect(pool.particles.values().next().value).toBe(particle);
  expect(pool.committed).toBe(1);
  expect(particle.leaving).toBe(true);
  for (const result of ["cancelled", "stale", "conflict"]) {
    expect(isCommitFinished(event(7, "commit", "complete_transaction", result))).toBe(true);
    expect(isCommitApplied(event(7, "commit", "complete_transaction", result))).toBe(false);
  }
  const next = event(8, "commit", "complete_transaction", "applied");
  events[2].context.generation = 1;
  next.context.generation = 2;
  expect(funnel([...events, next])!.levels.at(-1)!.count).toBe(2);
});

it("数据库只有完整有效统计才可绘图，未知值不补零", () => {
  const jobs = { pending: 0, running: 0, retry_wait: 0, dormant: 0, succeeded: 0 };
  expect(jobStates(snapshot({ available: false }))).toBeUndefined();
  expect(jobStates(snapshot({ available: true }))).toBeUndefined();
  expect(jobStates(snapshot({ available: true, value: { jobs: { pending: 0 } } }))).toBeUndefined();
  expect(jobStates(snapshot({ available: true, value: { jobs } }))!.states.map(s => s.count)).toEqual([0, 0, 0, 0, 0]);
  for (const invalid of [-1, 0.5, Number.NaN, Infinity, "0", null]) {
    expect(jobStates(snapshot({ available: true, value: { jobs: { ...jobs, pending: invalid } } }))).toBeUndefined();
  }
});

it("冻结时动画时间与粒子寿命停止，恢复后延续而非补跑冻结时长", () => {
  const clock = new AnimationClock();
  const pool = new ParticlePool(() => clock.time);
  clock.tick(1000);
  pool.consume([
    event(1, "lookup", "lookup", "started", { hash: "moving" }),
    event(2, "peer", "connect", "failed", { hash: "falling" }),
    event(3, "commit", "complete_transaction", "applied", { hash: "leaving" }),
  ], "0");
  clock.tick(1100);
  clock.setPaused(true);
  expect(clock.tick(61_100)).toBe(100);
  pool.sweep(clock.time);
  expect(pool.particles.size).toBe(3);
  clock.setPaused(false);
  expect(clock.tick(61_200)).toBe(100);
  pool.sweep(clock.tick(62_001));
  expect(pool.particles.has("leaving")).toBe(false);
  expect(pool.particles.has("falling")).toBe(true);
  pool.sweep(clock.tick(66_000));
  expect(pool.particles.has("falling")).toBe(false);
  expect(pool.particles.has("moving")).toBe(true);
});

it("正常离场和坠落粒子也占用容量，容量淘汰无需等待 sweep", () => {
  const pool = new ParticlePool(() => 0, 3);
  pool.consume([
    event(1, "commit", "complete_transaction", "applied", { hash: "leaving" }),
    event(2, "peer", "connect", "failed", { hash: "falling" }),
    event(3, "lookup", "lookup", "started", { hash: "active" }),
  ], "0");
  expect(pool.particles.size).toBe(3);
  expect(pool.particles.get("leaving")!.leaving).toBe(true);
  pool.consume([event(4, "discovery", "hash_saved", "new", { hash: "new" })], "3");
  expect([...pool.particles.keys()]).toEqual(["falling", "active", "new"]);
});
