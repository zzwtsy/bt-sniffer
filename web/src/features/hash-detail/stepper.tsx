import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { Chip, Status } from "@/components/observation/common";

function Arrow() {
  return (
    <span
      aria-hidden="true"
      className="self-center text-muted-foreground max-[760px]:hidden"
    >
      →
    </span>
  );
}
const stepClass
  = "flex min-w-[140px] flex-1 flex-col gap-1.5 rounded-md border px-3 py-2.5 text-xs max-[760px]:flex-[1_1_100%]";
const stepLabelClass = "text-[11px] font-semibold text-muted-foreground";
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
    <div className="flex flex-wrap items-stretch gap-2.5" aria-label="采集链路">
      <div className={stepClass}>
        <span className={stepLabelClass}>发现</span>
        {origin
          ? (
              <Link to="/discoveries/$id" params={{ id: origin.id }}>
                {origin.batch ? "采样批次" : "announce 观察"}
                {" "}
                {origin.id}
              </Link>
            )
          : (
              <span className="text-xs text-muted-foreground">来源未知或未载入</span>
            )}
      </div>
      <Arrow />
      <div className={stepClass}>
        <span className={stepLabelClass}>领取</span>
        {generations.length === 0
          ? (
              <span className="text-xs text-muted-foreground">未知</span>
            )
          : (
              <div className="flex flex-wrap gap-1.5" role="group" aria-label="领取 generation">
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
      <div className={stepClass}>
        <span className={stepLabelClass}>peer</span>
        {peers.length === 0
          ? (
              <span className="text-xs text-muted-foreground">无保留尝试</span>
            )
          : (
              <div className="flex flex-wrap gap-1.5" role="group" aria-label="peer 尝试">
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
      <div className={stepClass}>
        <span className={stepLabelClass}>传输</span>
        {transfer}
      </div>
      <Arrow />
      <div className={stepClass}>
        <span className={stepLabelClass}>校验与提交</span>
        {commit}
      </div>
    </div>
  );
}
