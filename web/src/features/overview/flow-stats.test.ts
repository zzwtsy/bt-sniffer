import { expect, it } from "vitest";
import { event, snapshot } from "../../../tests/fixtures";
import { flowStats } from "./flow-stats";

it("空缓冲不补零，未知字段保持未知", () => {
  const stats = flowStats(undefined, []);
  expect(stats.discovery.count).toBeUndefined();
  expect(stats.discovery.sample).toBeUndefined();
  expect(stats.schedule.runningWorkers).toBeUndefined();
  expect(stats.lookup.active).toBeUndefined();
  expect(stats.commit.total).toBeUndefined();
});
it("按 kind 与 step 分类计数，暂停状态映射为状态值", () => {
  const snap = {
    ...snapshot(),
    runtime: {
      sources: {
        config: { observed_at_ms: 1000, value: { sample: true } },
        collector: {
          observed_at_ms: 1000,
          value: { running_workers: 3, capacity_paused: true },
        },
      },
      active: [
        { step: "lookup", context: {}, since_ms: 900 },
        { step: "transfer", context: {}, since_ms: 900 },
      ],
    },
    cached: { database: { value: { jobs: { retry_wait: 2 }, metadata_count: 7 } } },
  };
  const events = [
    event(1),
    { ...event(2), kind: "discovery" },
    { ...event(3), kind: "commit", result: "applied" },
    { ...event(4), kind: "commit", result: "stale" },
    { ...event(5), kind: "lookup", result: "no_route" },
  ];
  const stats = flowStats(snap, events);
  expect(stats.discovery).toEqual({ count: 1, sample: true });
  expect(stats.schedule).toEqual({
    runningWorkers: 3,
    retryWait: 2,
    paused: ["capacity"],
  });
  expect(stats.lookup).toEqual({ active: 1, completed: 1, noRoute: 1 });
  expect(stats.transfer).toEqual({ active: 1, accepted: 1 });
  expect(stats.commit).toEqual({ total: 7, notApplied: 1 });
});
