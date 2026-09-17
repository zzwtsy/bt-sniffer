import type { ThroughputPoint } from "./model";
import type { ObservationEvent } from "@/lib/observation/contracts";
import type { Phase } from "@/lib/observation/engine";
import { useEffect, useEffectEvent, useRef, useState } from "react";
import { useDisplayed } from "@/lib/observation/context";
import { ThroughputChart } from "./charts";
import { discoveryPerMinute, ThroughputSeries } from "./model";

/** 接收不节流；只有曲线采样每秒一次，冻结保留显示副本而非停止采样。 */
export function Throughput({ events, phase, epoch, commits }: {
  events: ObservationEvent[];
  phase: Phase;
  epoch: number;
  commits: number | null;
}) {
  const [model] = useState(() => new ThroughputSeries());
  const [points, setPoints] = useState<ThroughputPoint[]>([]);
  const previousEpochRef = useRef(epoch);
  const sample = useEffectEvent(() => {
    const now = Date.now();
    if (phase === "live")
      model.add(now, discoveryPerMinute(events, now), commits);
    else
      model.gap(now);
    setPoints(model.points);
  });
  useEffect(() => {
    if (previousEpochRef.current !== epoch) {
      previousEpochRef.current = epoch;
      model.clear();
    }
    sample();
    const timer = setInterval(sample, 1000);
    return () => clearInterval(timer);
  }, [epoch, phase, model]);
  const displayed = useDisplayed("overview:series", points);
  return <ThroughputChart points={displayed} />;
}
