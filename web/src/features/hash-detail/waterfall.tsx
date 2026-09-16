import type { spans } from "./model";
import { Empty } from "@/components/observation/common";
import { duration, label } from "@/lib/observation/format";

type Span = ReturnType<typeof spans>[number];
function tick(at: number) {
  return new Date(at).toLocaleTimeString("zh-CN", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
/** 阶段瀑布图：时间轴取已配对跨度的真实起止；未配对起点向当前时间延伸并标注进行中。 */
export function Waterfall({ track }: { track: Span[] }) {
  if (track.length === 0) {
    return <Empty>尚未载入可配对的阶段起止事件；不能据此断言未执行。</Empty>;
  }
  const latest = Math.max(...track.map(t => t.at));
  const start = Math.min(...track.map(t => t.at));
  const end = Math.max(
    start + 1,
    ...track.map(t => t.at + (t.elapsed ?? Math.max(1, latest - t.at))),
  );
  const span = end - start;
  const depthOf = new Map<string, number>();
  const byId = new Map(track.map(t => [t.id, t]));
  for (const t of track) {
    let depth = 0;
    let cursor = t.parent;
    while (cursor !== undefined && byId.has(cursor) && depth < 8) {
      depth++;
      cursor = byId.get(cursor)?.parent;
    }
    depthOf.set(t.id, depth);
  }
  const ticks = [0, 1, 2, 3].map(i => start + (span * i) / 3);
  return (
    <div className="waterfall" role="list" aria-label="阶段时间轴">
      <div className="waterfall-axis" aria-hidden="true">
        {ticks.map(t => (
          <span key={t}>{tick(t)}</span>
        ))}
      </div>
      {track.map((t) => {
        const open = t.elapsed === undefined;
        const left = ((t.at - start) * 100) / span;
        const width = open
          ? 100 - left
          : Math.max(1, ((t.elapsed ?? 0) * 100) / span);
        return (
          <div className="waterfall-row" role="listitem" key={t.id}>
            <div
              className="waterfall-label"
              style={{ paddingLeft: `${(depthOf.get(t.id) ?? 0) * 14}px` }}
            >
              <strong>
                {label(t.kind)}
                {" "}
                /
                {label(t.step)}
              </strong>
              <small>
                {open ? "进行中" : duration(t.elapsed)}
                {" "}
                ·
                {label(t.result)}
              </small>
            </div>
            <div className="waterfall-track">
              <span
                className={`waterfall-bar ${open ? "open" : ""}`}
                style={{ left: `${left}%`, width: `${width}%` }}
                title={`${label(t.kind)} / ${label(t.step)} · ${open ? "进行中" : duration(t.elapsed)} · ${label(t.result)}`}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
