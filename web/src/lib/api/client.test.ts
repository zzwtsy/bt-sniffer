import { afterEach, expect, it, vi } from "vitest";
import { ApiError, RequestScheduler, retry } from "./client";

afterEach(() => vi.useRealTimers());
it("合并订阅、数据库串行，取消通知不提前释放执行许可", async () => {
  const scheduler = new RequestScheduler();
  const first = new AbortController();
  const second = new AbortController();
  let finish!: (v: number) => void;
  const work = vi.fn(
    async () =>
      new Promise<number>((resolve) => {
        finish = resolve;
      }),
  );
  const a = scheduler.request("a", true, first.signal, work);
  const b = scheduler.request("a", true, second.signal, work);
  await Promise.resolve();
  expect(work).toHaveBeenCalledTimes(1);
  first.abort();
  await expect(a).rejects.toMatchObject({ name: "AbortError" });
  const later = vi.fn(async () => 2);
  const c = scheduler.request("b", true, new AbortController().signal, later);
  expect(later).not.toHaveBeenCalled();
  expect(scheduler.stats.database).toBe(1);
  finish(1);
  await expect(b).resolves.toBe(1);
  await expect(c).resolves.toBe(2);
  scheduler.cancelAll();
});
it("请求速率、并发和排队均受限，取消后清理队列", async () => {
  vi.useFakeTimers();
  const scheduler = new RequestScheduler();
  const controller = new AbortController();
  const finish: (() => void)[] = [];
  const tasks = Array.from({ length: 22 }, async (_, i) =>
    scheduler
      .request(
        String(i),
        false,
        controller.signal,
        async () => new Promise<void>(resolve => finish.push(resolve)),
      )
      .catch((e: unknown) => e));
  await Promise.resolve();
  expect(scheduler.stats).toEqual({ active: 2, database: 0, queued: 20 });
  await expect(
    scheduler.request("overflow", false, controller.signal, async () => 0),
  ).rejects.toMatchObject({ code: "client_capacity" });
  controller.abort();
  expect(scheduler.stats.active).toBe(2);
  expect(scheduler.stats.queued).toBe(0);
  finish.forEach(resolve => resolve());
  await Promise.all(tasks);
  await Promise.resolve();
  scheduler.cancelAll();
});
it("仅对可恢复错误有限重试", () => {
  expect(retry(0, new ApiError(400, "invalid", ""))).toBe(false);
  expect(retry(1, new ApiError(429, "busy", ""))).toBe(true);
  expect(retry(2, new ApiError(503, "unavailable", ""))).toBe(false);
});
