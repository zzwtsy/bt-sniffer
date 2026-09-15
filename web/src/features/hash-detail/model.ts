import type { Fields, ObservationEvent } from "@/lib/observation/contracts";
import { number, record } from "@/lib/observation/contracts";

export function generations(
  events: ObservationEvent[],
  current?: number,
): number[] {
  return [
    ...new Set([
      ...events
        .map(e => e.context.generation)
        .filter((n): n is number => n !== undefined),
      ...(current === undefined ? [] : [current]),
    ]),
  ].sort((a, b) => b - a);
}
export function peerEvents(
  events: ObservationEvent[],
  generation: number,
  peer: string,
) {
  return events.filter(
    e =>
      e.context.generation === generation && e.context.peer_attempt_id === peer,
  );
}
export function pieces(events: ObservationEvent[], active?: Fields) {
  const data = record(active?.data);
  const complete = Array.isArray(data.complete) ? data.complete : undefined;
  let total = complete?.length;
  let received = number(data.received_pieces);
  let duplicates = 0;
  const states = new Map<number, string>();
  for (const e of events) {
    const d = record(e.data);
    if (e.kind !== "piece")
      continue;
    total ??= number(d.total_pieces);
    if (!complete && number(d.received_pieces) !== undefined)
      received = number(d.received_pieces);
    const index = number(d.piece);
    if (index === undefined)
      continue;
    if (e.result === "duplicate")
      duplicates++;
    else if (e.result === "accepted")
      states.set(index, "received");
    else if (e.result === "invalid" || e.result === "rejected")
      states.set(index, "invalid");
    else if (e.step === "request")
      states.set(index, "requested");
  }
  if (complete) {
    complete.forEach((done, i) =>
      states.set(i, done === true ? "received" : "pending"),
    );
  }
  return { total, received, duplicates, states };
}
/** 时间轨道用 UTC 起点和后端单调耗时；缺少起点时不补造跨度。 */
export function spans(events: ObservationEvent[]) {
  const starts = new Map<string, ObservationEvent>();
  const result: {
    id: string;
    kind: string;
    step: string;
    at: number;
    elapsed?: number;
    result: string;
  }[] = [];
  for (const event of events) {
    const id = event.context.span_id;
    if (id === undefined || id === "")
      continue;
    if (event.result === "started") {
      starts.set(id, event);
    } else if (number(record(event.data).elapsed_ms) !== undefined) {
      const start = starts.get(id);
      if (start) {
        result.push({
          id,
          kind: start.kind,
          step: start.step,
          at: start.at_ms,
          elapsed: number(record(event.data).elapsed_ms),
          result: event.result,
        });
        starts.delete(id);
      }
    }
  }
  for (const [id, start] of starts) {
    result.push({
      id,
      kind: start.kind,
      step: start.step,
      at: start.at_ms,
      result: "结束未知",
    });
  }
  return result.sort((a, b) => a.at - b.at);
}
