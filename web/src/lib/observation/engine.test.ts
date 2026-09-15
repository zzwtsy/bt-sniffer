import { QueryClient } from "@tanstack/react-query";
import { afterEach, expect, it, vi } from "vitest";
import { event, snapshot } from "../../../tests/fixtures";
import { MonitorEngine } from "./engine";

class Stream extends EventTarget {
  onerror: (() => void) | null = null;
  closed = false;
  close() {
    this.closed = true;
  }

  send(name: string, value: unknown) {
    this.dispatchEvent(new MessageEvent(name, { data: JSON.stringify(value) }));
  }
}
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});
async function fixture() {
  vi.useFakeTimers();
  const streams: { url: string; stream: Stream }[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () => new Response(JSON.stringify(snapshot()), { status: 200 }),
    ),
  );
  const engine = new MonitorEngine(new QueryClient(), (url) => {
    const stream = new Stream();
    streams.push({ url, stream });
    return stream as unknown as EventSource;
  });
  engine.start();
  await vi.advanceTimersByTimeAsync(1000);
  return { engine, streams };
}
it("snapshot 不推进事件游标，断线关闭旧连接并从接纳批次续传", async () => {
  const { engine, streams } = await fixture();
  expect(streams[0].url).toContain("run%3A3");
  const stream = streams[0].stream;
  stream.send("hello", snapshot().window);
  stream.send("snapshot", {
    ...snapshot(),
    window: { ...snapshot().window, latest: "100" },
  });
  stream.send("events", {
    events: [event(4)],
    next: "4",
    window: snapshot().window,
    completeness: "complete",
  });
  stream.send("events", {
    events: [event(4)],
    next: "4",
    window: snapshot().window,
    completeness: "complete",
  });
  expect(engine.buffer.size).toBe(1);
  stream.onerror?.();
  expect(stream.closed).toBe(true);
  await vi.advanceTimersByTimeAsync(1200);
  expect(streams[1].url).toContain("run%3A4");
  engine.stop();
  expect(streams[1].stream.closed).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});
it("容量 reset 停止循环，普通 reset 清理缓存，旧回调不会覆盖新运行", async () => {
  const { engine, streams } = await fixture();
  const old = streams[0].stream;
  old.send("events", {
    events: [event(4)],
    next: "4",
    window: snapshot().window,
    completeness: "complete",
  });
  old.send("reset", snapshot().window);
  await vi.advanceTimersByTimeAsync(1000);
  expect(engine.buffer.size).toBe(0);
  old.send("events", { invalid: true });
  expect(engine.getSnapshot().phase).not.toBe("unavailable");
  streams[1].stream.send("reset", { reason: "response_capacity" });
  expect(engine.getSnapshot().phase).toBe("unavailable");
  await vi.advanceTimersByTimeAsync(60_000);
  expect(streams).toHaveLength(2);
  engine.stop();
});
it("格式不兼容停止自动同步，重复挂载不会留下连接", async () => {
  const { engine, streams } = await fixture();
  engine.stop();
  engine.start();
  await vi.advanceTimersByTimeAsync(1000);
  expect(streams[0].stream.closed).toBe(true);
  streams[1].stream.send("snapshot", { schema_version: 2 });
  expect(engine.getSnapshot().phase).toBe("unavailable");
  engine.stop();
});

it("静默断线期间仍按时间淘汰，重复 start 和停止不遗留定时器", async () => {
  const { engine, streams } = await fixture();
  const window = { ...snapshot().window, latest: "4" };
  streams[0].stream.send("events", { events: [event(4)], next: "4", window, completeness: "complete" });
  engine.start();
  expect(streams).toHaveLength(1);
  await vi.advanceTimersByTimeAsync(901_000);
  expect(engine.buffer.size).toBe(0);
  expect(engine.buffer.indexEntries).toBe(0);
  engine.stop();
  expect(vi.getTimerCount()).toBe(0);
});

it("根控制器停止时回收查询缓存及其 GC 定时器", () => {
  vi.useFakeTimers();
  const queries = new QueryClient({ defaultOptions: { queries: { gcTime: 60_000 } } });
  queries.setQueryData(["retained-page"], { items: [] });
  const engine = new MonitorEngine(queries);
  engine.stop();
  expect(queries.getQueryCache().getAll()).toHaveLength(0);
  expect(vi.getTimerCount()).toBe(0);
});
