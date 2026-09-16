import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { cn } from "cn";
import { Status } from "@/components/observation/common";

function Arrow() {
  return <span aria-hidden="true" className="step-arrow">→</span>;
}
function Chip({
  selected,
  onClick,
  children,
}: {
  selected: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      className={cn("chip", selected && "selected")}
      aria-pressed={selected}
      onClick={onClick}
    >
      {children}
    </button>
  );
}
/** 单个 hash 的链路步骤条：领取与 peer 用 chip 直选，写回 URL search。 */
export function Stepper({
  origin,
  generations,
  generation,
  peers,
  peer,
  transfer,
  commit,
  onSelect,
}: {
  origin?: { id: string; batch: boolean };
  generations: { value: number; lastResult?: unknown }[];
  generation?: number;
  peers: string[];
  peer?: string;
  transfer: ReactNode;
  commit: ReactNode;
  onSelect: (next: { generation?: number; peer?: string }) => void;
}) {
  return (
    <div className="stepper" aria-label="采集链路">
      <div className="step">
        <span className="step-label">发现</span>
        {origin
          ? (
              <Link to="/discoveries/$id" params={{ id: origin.id }}>
                {origin.batch ? "采样批次" : "announce 观察"}
                {" "}
                {origin.id}
              </Link>
            )
          : (
              <span className="muted">来源未知或未载入</span>
            )}
      </div>
      <Arrow />
      <div className="step">
        <span className="step-label">领取</span>
        {generations.length === 0
          ? (
              <span className="muted">未知</span>
            )
          : (
              <div className="chips" role="group" aria-label="领取 generation">
                {generations.map(g => (
                  <Chip
                    key={g.value}
                    selected={g.value === generation}
                    onClick={() => onSelect({ generation: g.value, peer: undefined })}
                  >
                    generation
                    {" "}
                    {g.value}
                    {g.lastResult !== undefined && g.value === generation && (
                      <Status value={g.lastResult} />
                    )}
                  </Chip>
                ))}
              </div>
            )}
      </div>
      <Arrow />
      <div className="step">
        <span className="step-label">peer</span>
        {peers.length === 0
          ? (
              <span className="muted">无保留尝试</span>
            )
          : (
              <div className="chips" role="group" aria-label="peer 尝试">
                {peers.map(p => (
                  <Chip
                    key={p}
                    selected={p === peer}
                    onClick={() => onSelect({ peer: p })}
                  >
                    <code>{p}</code>
                  </Chip>
                ))}
              </div>
            )}
      </div>
      <Arrow />
      <div className="step">
        <span className="step-label">传输</span>
        {transfer}
      </div>
      <Arrow />
      <div className="step">
        <span className="step-label">校验与提交</span>
        {commit}
      </div>
    </div>
  );
}
