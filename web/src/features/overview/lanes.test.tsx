import { fireEvent, render } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import { eventSchema } from "@/lib/observation/contracts";
import { Pipeline } from "./lanes";

const { display, monitor } = vi.hoisted(() => ({
  display: { frozen: false },
  monitor: { revision: 1, epoch: 0 },
}));
vi.mock("@/lib/observation/context", () => ({
  useMonitor: () => monitor,
  useDisplay: () => display,
}));

interface AnimationCall {
  keyframes: Keyframe[];
  options: KeyframeAnimationOptions;
  animation: Animation;
  pause: ReturnType<typeof vi.fn>;
  play: ReturnType<typeof vi.fn>;
}

const animationCalls: AnimationCall[] = [];
let reducedMotion = false;
const mediaListeners = new Set<() => void>();

beforeEach(() => {
  display.frozen = false;
  monitor.revision = 1;
  monitor.epoch = 0;
  reducedMotion = false;
  mediaListeners.clear();
  animationCalls.length = 0;
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: () => ({
      get matches() { return reducedMotion; },
      addEventListener: (_name: string, listener: () => void) => mediaListeners.add(listener),
      removeEventListener: (_name: string, listener: () => void) => mediaListeners.delete(listener),
    }),
  });
  Object.defineProperty(Element.prototype, "animate", {
    configurable: true,
    value(keyframes: Keyframe[], options: KeyframeAnimationOptions) {
      const pause = vi.fn();
      const play = vi.fn();
      const animation = {
        cancel: vi.fn(),
        pause,
        play,
        finished: new Promise(() => {}),
      } as unknown as Animation;
      animationCalls.push({ keyframes, options, animation, pause, play });
      return animation;
    },
  });
  Object.defineProperty(Element.prototype, "getAnimations", {
    configurable: true,
    value: () => animationCalls.map(call => call.animation),
  });
});

function event(
  sequence: number,
  kind: string,
  step: string,
  result: string,
  hash = "a".repeat(40),
) {
  return eventSchema.parse({
    schema_version: 1,
    run_id: "run",
    sequence: String(sequence),
    at_ms: 1000 + sequence,
    kind,
    step,
    result,
    context: { hash },
    data: {},
    truncated: false,
  });
}

function pipeline(events: ReturnType<typeof event>[]) {
  return (
    <Pipeline
      events={events}
      snapshot={undefined}
      lanes={{}}
      selected={null}
      onSelect={() => {}}
    />
  );
}

it("粒子保持可观察的视觉标识，但不再导航到 hash 详情", () => {
  const hash = "0123456789abcdef0123456789abcdef01234567";
  const screen = render(pipeline([event(1, "discovery", "hash_saved", "new", hash)]));
  const particle = screen.getByTestId("pipeline-particle");
  expect(particle.dataset.hash).toBe(hash);
  fireEvent.mouseEnter(particle);
  expect(screen.getByText(/发现 · 主动采样/)).toBeVisible();
  expect(particle.tagName).toBe("SPAN");
});

it("普通移动只生成 transform/opacity keyframe，时长为 180ms", () => {
  const hash = "d".repeat(40);
  const first = event(1, "discovery", "hash_saved", "new", hash);
  const screen = render(pipeline([first]));
  screen.rerender(pipeline([first, event(2, "lookup", "lookup", "started", hash)]));
  const movement = animationCalls.find(call => call.options.duration === 180)!;
  expect(movement).toBeDefined();
  expect(movement.keyframes.every((frame) => {
    const keys = Object.keys(frame);
    return keys.every(key => key === "transform" || key === "opacity");
  })).toBe(true);
});

it("新失败粒子从正常位置坠落，使用危险色并延迟淡出", () => {
  const hash = "b".repeat(40);
  const screen = render(pipeline([event(1, "peer", "connect", "failed", hash)]));
  const particle = screen.getByTestId("pipeline-particle");
  const motion = particle.parentElement!;
  expect(motion.style.opacity).toBe("0");
  const fall = animationCalls.find(call => call.options.duration === 600)!;
  const fade = animationCalls.find(call => call.options.duration === 2400)!;
  expect(fall.keyframes[0].transform).toContain("translate(");
  expect(fall.keyframes[0].transform).not.toBe("translate(0, 0)");
  expect(fade.keyframes[0].opacity).toBe(1);
  expect(fade.options.delay).toBe(1600);
  expect(particle.querySelector("span")).toHaveStyle({
    backgroundColor: "var(--status-danger)",
  });
});

it("新提交粒子先位于提交泳道正常位置再离场", () => {
  const hash = "c".repeat(40);
  render(pipeline([event(1, "commit", "complete_transaction", "applied", hash)]));
  const leave = animationCalls.find(call => call.options.duration === 700)!;
  expect(leave.keyframes[0].opacity).toBe(1);
  expect(leave.keyframes[0].transform).toContain("cqw");
  expect(leave.keyframes[1]).toEqual({ transform: "translate(0, 0)", opacity: 0.4 });
});

it("reduced motion 取消动画并直接展示目标状态", () => {
  reducedMotion = true;
  const screen = render(pipeline([event(1, "peer", "connect", "failed")]));
  expect(animationCalls).toHaveLength(0);
  const particle = screen.getByTestId("pipeline-particle");
  expect(particle.parentElement).toHaveStyle({ opacity: "0" });
});

it("冻结暂停已有 WAAPI 动画，恢复后从原进度播放", () => {
  const hash = "e".repeat(40);
  const first = event(1, "discovery", "hash_saved", "new", hash);
  const screen = render(pipeline([first]));
  const progressed = [first, event(2, "lookup", "lookup", "started", hash)];
  screen.rerender(pipeline(progressed));
  const movement = animationCalls.find(call => call.options.duration === 180)!;
  movement.pause.mockClear();
  movement.play.mockClear();
  display.frozen = true;
  screen.rerender(pipeline(progressed));
  expect(movement.pause).toHaveBeenCalled();
  display.frozen = false;
  screen.rerender(pipeline(progressed));
  expect(movement.play).toHaveBeenCalled();
});
