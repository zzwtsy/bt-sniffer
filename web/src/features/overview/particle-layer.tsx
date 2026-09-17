import type { CSSProperties } from "react";
import type { Source } from "./model";
import { memo, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { shortHash } from "@/lib/observation/format";
import { STAGES } from "./model";

const HEIGHT = 240;
const SLOTS = 12;
const MOVE_MS = 180;
const FALL_MS = 600;
const LEAVE_MS = 700;
const FALL_FADE_DELAY_MS = 1600;
const FALL_FADE_MS = 2400;
const EASE_OUT = "cubic-bezier(0.33, 1, 0.68, 1)";
const EASE_IN = "cubic-bezier(0.4, 0, 1, 1)";

const sourceLabels: Record<Source, string> = {
  sample: "主动采样",
  announce: "announce",
  backfill: "历史回填",
  neutral: "来源未标明",
};
const sourceColors: Record<Source, string> = {
  sample: "var(--chart-1)",
  announce: "var(--chart-4)",
  backfill: "var(--status-warning)",
  neutral: "var(--muted-foreground)",
};

function hashSlot(hash: string) {
  let code = 0;
  for (let i = 0; i < hash.length; i++)
    code = (code * 31 + hash.charCodeAt(i)) >>> 0;
  return code % SLOTS;
}

interface Point {
  xCqw: number;
  xPx: number;
  yPx: number;
  opacity: number;
}

function normalPoint(hash: string, stage: number): Point {
  return {
    xCqw: ((stage + 0.5) / STAGES.length) * 100,
    xPx: 0,
    yPx: 24 + hashSlot(hash) * ((HEIGHT - 80) / SLOTS),
    opacity: 1,
  };
}

function targetPoint(hash: string, stage: number, falling: boolean, leaving: boolean): Point {
  if (falling)
    return { ...normalPoint(hash, stage), yPx: HEIGHT - 30, opacity: 0 };
  if (leaving)
    return { xCqw: 100, xPx: -16, yPx: 14, opacity: 0.4 };
  return normalPoint(hash, stage);
}

function anchorStyle(point: Point): CSSProperties {
  return {
    left: point.xPx === 0
      ? `${point.xCqw}%`
      : `calc(${point.xCqw}% + ${point.xPx}px)`,
    top: point.yPx,
  };
}

function offsetTransform(from: Point, to: Point, current = "none") {
  const xCqw = from.xCqw - to.xCqw;
  const xPx = from.xPx - to.xPx;
  const yPx = from.yPx - to.yPx;
  const prior = current === "none" ? "" : ` ${current}`;
  return `translate(calc(${xCqw}cqw + ${xPx}px), ${yPx}px)${prior}`;
}

function samePoint(left: Point, right: Point) {
  return left.xCqw === right.xCqw
    && left.xPx === right.xPx
    && left.yPx === right.yPx
    && left.opacity === right.opacity;
}

interface ParticleNodeProps {
  hash: string;
  stage: number;
  source: Source;
  falling: boolean;
  leaving: boolean;
  reducedMotion: boolean;
  onActivate: (hash: string) => void;
}

const ParticleNode = memo(({
  hash,
  stage,
  source,
  falling,
  leaving,
  reducedMotion,
  onActivate,
}: ParticleNodeProps) => {
  const motionRef = useRef<HTMLDivElement>(null);
  const previousRef = useRef<Point | undefined>(undefined);
  const animationsRef = useRef<Animation[]>([]);
  const [tooltip, setTooltip] = useState(false);
  const target = useMemo(
    () => targetPoint(hash, stage, falling, leaving),
    [falling, hash, leaving, stage],
  );

  useLayoutEffect(() => {
    const node = motionRef.current;
    if (!node)
      return;
    const isNew = previousRef.current === undefined;
    const previous = previousRef.current
      ?? (falling || leaving ? normalPoint(hash, stage) : target);
    previousRef.current = target;

    if (reducedMotion || samePoint(previous, target)) {
      for (const animation of animationsRef.current) animation.cancel();
      animationsRef.current = [];
      return;
    }

    const interrupted = animationsRef.current.some(animation =>
      animation.playState === "running"
      || animation.playState === "paused",
    );
    const computed = interrupted ? getComputedStyle(node) : undefined;
    const currentTransform = computed?.transform ?? "none";
    const parsedOpacity = Number.parseFloat(computed?.opacity ?? "");
    const currentOpacity = !isNew && !Number.isNaN(parsedOpacity)
      ? parsedOpacity
      : previous.opacity;
    for (const animation of animationsRef.current) animation.cancel();

    const startTransform = offsetTransform(previous, target, currentTransform);
    const animations: Animation[] = [];
    const add = (animation: Animation) => {
      animations.push(animation);
      void animation.finished.then(() => animation.cancel(), () => {});
    };
    if (falling) {
      add(node.animate(
        [{ transform: startTransform }, { transform: "translate(0, 0)" }],
        { duration: FALL_MS, easing: EASE_IN, fill: "both" },
      ));
      add(node.animate(
        [{ opacity: currentOpacity }, { opacity: target.opacity }],
        { delay: FALL_FADE_DELAY_MS, duration: FALL_FADE_MS, easing: "linear", fill: "both" },
      ));
    } else {
      const duration = leaving ? LEAVE_MS : MOVE_MS;
      add(node.animate(
        [
          { transform: startTransform, opacity: currentOpacity },
          { transform: "translate(0, 0)", opacity: target.opacity },
        ],
        { duration, easing: EASE_OUT, fill: "both" },
      ));
    }
    animationsRef.current = animations;
  }, [falling, hash, leaving, reducedMotion, stage, target]);

  useEffect(() => () => {
    for (const animation of animationsRef.current) animation.cancel();
  }, []);

  return (
    <div className="absolute" style={anchorStyle(target)}>
      <div
        ref={motionRef}
        data-slot="pipeline-particle-motion"
        style={{ opacity: target.opacity }}
      >
        <button
          type="button"
          data-slot="pipeline-particle"
          data-hash={hash}
          aria-label={`${shortHash(hash)} · ${STAGES[stage].title}`}
          className="absolute flex size-4.5 -translate-x-1/2 -translate-y-1/2 cursor-pointer items-center justify-center focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          onClick={() => onActivate(hash)}
          onMouseEnter={() => setTooltip(true)}
          onMouseLeave={() => setTooltip(false)}
          onFocus={() => setTooltip(true)}
          onBlur={() => setTooltip(false)}
        >
          <span
            className="size-1.75 rounded-full"
            style={{
              backgroundColor: falling
                ? "var(--status-danger)"
                : sourceColors[source],
            }}
          />
        </button>
        {tooltip && (
          <div className="pointer-events-none absolute top-3 left-3 z-10 rounded-md border bg-popover px-2 py-1 text-[11px] whitespace-nowrap text-popover-foreground">
            <code>{shortHash(hash)}</code>
            {` · ${STAGES[stage].title} · ${sourceLabels[source]}`}
          </div>
        )}
      </div>
    </div>
  );
});

export interface ParticleView {
  hash: string;
  stage: number;
  source: Source;
  falling: boolean;
  leaving: boolean;
}

export function ParticleLayer({
  particles,
  frozen,
  onActivate,
}: {
  particles: ParticleView[];
  frozen: boolean;
  onActivate: (hash: string) => void;
}) {
  const layerRef = useRef<HTMLDivElement>(null);
  const [reducedMotion, setReducedMotion] = useState(
    () => window.matchMedia("(prefers-reduced-motion: reduce)").matches,
  );

  useEffect(() => {
    const query = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setReducedMotion(query.matches);
    query.addEventListener("change", update);
    return () => query.removeEventListener("change", update);
  }, []);

  useEffect(() => {
    const animations = layerRef.current?.getAnimations({ subtree: true }) ?? [];
    for (const animation of animations) {
      if (reducedMotion)
        animation.cancel();
      else if (frozen)
        animation.pause();
      else
        animation.play();
    }
  }, [frozen, reducedMotion]);

  return (
    <div
      ref={layerRef}
      role="group"
      aria-label="粒子流水线动画，仅为窗口内示意"
      data-slot="pipeline-stage"
      className="relative min-w-140"
      style={{ height: HEIGHT, containerType: "inline-size" }}
    >
      {STAGES.slice(1).map((stage, index) => (
        <div
          key={stage.id}
          aria-hidden
          className="absolute top-2 bottom-2 border-l border-border opacity-60"
          style={{ left: `${(((index + 1) / STAGES.length) * 100).toFixed(4)}%` }}
        >
          <span className="absolute top-1/2 left-0 size-0 -translate-x-1/2 -translate-y-1/2 border-y-5 border-l-9 border-y-transparent border-l-muted-foreground" />
        </div>
      ))}
      <div
        aria-hidden
        className="absolute right-0 bottom-6 left-0 border-t border-dashed border-status-danger opacity-50"
      />
      {particles.map(particle => (
        <ParticleNode
          key={particle.hash}
          {...particle}
          reducedMotion={reducedMotion}
          onActivate={onActivate}
        />
      ))}
    </div>
  );
}
