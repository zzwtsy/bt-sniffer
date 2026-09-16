import type { ObservationEvent, Snapshot } from "@/lib/observation/contracts";
import { number, record, source } from "@/lib/observation/contracts";

export interface FlowStats {
  discovery: { count?: number; sample?: boolean };
  schedule: { runningWorkers?: number; retryWait?: number; paused: string[] };
  lookup: { active?: number; completed?: number; noRoute?: number };
  transfer: { active?: number; accepted?: number };
  commit: { total?: number; notApplied?: number };
}
const transferSteps = ["connect", "handshake", "extension", "transfer"];
/** 窗口计数只在事件缓冲非空时给出；空缓冲表示尚未接收到事件，不补零。 */
function windowCount(
  events: ObservationEvent[],
  match: (event: ObservationEvent) => boolean,
): number | undefined {
  if (events.length === 0)
    return undefined;
  return events.filter(match).length;
}
export function flowStats(
  snapshot: Snapshot | undefined,
  events: ObservationEvent[],
): FlowStats {
  const config = source(snapshot, "config");
  const collector = source(snapshot, "collector");
  const facts = record(record(snapshot?.cached.database).value);
  const jobs = record(facts.jobs);
  const active = snapshot?.runtime.active;
  const paused: string[] = [];
  if (collector.capacity_paused === true)
    paused.push("capacity");
  if (collector.backlog_paused === true)
    paused.push("local_wait");
  if (collector.storage_paused === true)
    paused.push("failed");
  const activeCount = (steps: string[]) =>
    active?.filter(a => steps.includes(String(a.step))).length;
  return {
    discovery: {
      count: windowCount(events, e => e.kind === "discovery"),
      sample: typeof config.sample === "boolean" ? config.sample : undefined,
    },
    schedule: {
      runningWorkers: number(collector.running_workers),
      retryWait: number(jobs.retry_wait),
      paused,
    },
    lookup: {
      active: activeCount(["lookup"]),
      completed: windowCount(
        events,
        e =>
          e.kind === "lookup" && e.result !== "started" && e.result !== "waiting",
      ),
      noRoute: windowCount(
        events,
        e => e.kind === "lookup" && e.result === "no_route",
      ),
    },
    transfer: {
      active: activeCount(transferSteps),
      accepted: windowCount(
        events,
        e => e.kind === "piece" && e.result === "accepted",
      ),
    },
    commit: {
      total: number(facts.metadata_count),
      notApplied: windowCount(
        events,
        e =>
          e.kind === "commit" && e.result !== "applied" && e.result !== "started",
      ),
    },
  };
}
