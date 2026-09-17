import type { ThroughputPoint } from "./model";
import type { ObservationEvent } from "@/lib/observation/contracts";
import type { Phase } from "@/lib/observation/engine";
import { act, render } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { DisplayStore } from "@/lib/observation/context";
import { eventSchema } from "@/lib/observation/contracts";
import { DisplayProvider } from "@/lib/observation/display-provider";
import { Throughput } from "./throughput";

const { draw } = vi.hoisted(() => ({ draw: vi.fn((_props: { points: ThroughputPoint[] }) => null) }));
vi.mock("./charts", async () => {
  const { memo } = await import("react");
  return { ThroughputChart: memo(draw) };
});
afterEach(() => {
  vi.useRealTimers();
  draw.mockClear();
});
const latest = () => draw.mock.lastCall![0].points;
function setup() {
  vi.useFakeTimers();
  vi.setSystemTime(100_000);
  const store = new DisplayStore();
  let props = { events: [] as ObservationEvent[], phase: "live" as Phase, epoch: 0, commits: 2 as number | null };
  const ui = () => <DisplayProvider store={store}><Throughput {...props} /></DisplayProvider>;
  const view = render(ui());
  return {
    store,
    view,
    update: (next: Partial<typeof props>) => {
      props = { ...props, ...next };
      view.rerender(ui());
    },
  };
}
it("每秒采样最新输入，事件自然过期；卸载取消定时器", () => {
  const { update, view } = setup();
  const discovery = eventSchema.parse({ schema_version: 1, run_id: "run", sequence: "1", at_ms: 41_000, kind: "discovery", step: "hash_saved", result: "new", context: {}, data: {}, truncated: false });
  const initial = draw.mock.calls.length;
  update({ events: [discovery], commits: 3 });
  act(() => {
    vi.advanceTimersByTime(999);
  });
  expect(draw).toHaveBeenCalledTimes(initial);
  act(() => {
    vi.advanceTimersByTime(1);
  });
  expect(latest().at(-1)).toEqual({ at: 101_000, discoveries: 1, commits: 3 });
  act(() => {
    vi.advanceTimersByTime(1000);
  });
  expect(latest().at(-1)?.discoveries).toBe(0);
  view.unmount();
  expect(vi.getTimerCount()).toBe(0);
});
it("断线只留一个 gap，恢复和 epoch 切换立即处理", () => {
  const { update } = setup();
  update({ phase: "reconnecting" });
  expect(latest().at(-1)?.discoveries).toBeNull();
  const count = latest().length;
  act(() => {
    vi.advanceTimersByTime(3000);
  });
  expect(latest()).toHaveLength(count);
  update({ phase: "live", commits: 4 });
  expect(latest().at(-1)?.commits).toBe(4);
  update({ epoch: 1, phase: "connecting", commits: null });
  expect(latest()).toEqual([{ at: 103_000, discoveries: null, commits: null }]);
});
it("冻结显示不变，内部继续采样及清除旧 epoch，恢复展示最新序列", () => {
  const { store, update } = setup();
  act(() => store.toggle());
  const frozen = latest();
  act(() => {
    vi.advanceTimersByTime(5000);
  });
  expect(latest()).toBe(frozen);
  update({ epoch: 1, commits: 7 });
  act(() => {
    vi.advanceTimersByTime(2000);
  });
  expect(latest()).toBe(frozen);
  act(() => store.resume());
  expect(latest()).toHaveLength(3);
  expect(latest().at(-1)).toEqual({ at: 107_000, discoveries: 0, commits: 7 });
});
