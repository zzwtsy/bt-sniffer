import { eventSchema, snapshotSchema } from "../src/lib/observation/contracts";

export function event(sequence: number, data: Record<string, unknown> = {}) {
  return eventSchema.parse({
    schema_version: 1,
    run_id: "run",
    sequence: String(sequence),
    at_ms: 1000 + sequence,
    kind: "piece",
    step: "receive",
    result: "accepted",
    context: { hash: "a".repeat(40), generation: 3, peer_attempt_id: "p1" },
    data,
    truncated: false,
  });
}
export function snapshot(at = 1000, count = 1, run = "run") {
  return snapshotSchema.parse({
    schema_version: 1,
    window: {
      run_id: run,
      oldest: "1",
      latest: "3",
      retained: 3,
      bytes: 1000,
      evicted: 0,
      truncated: 0,
    },
    runtime: {
      sources: {
        collector: {
          observed_at_ms: at,
          value: {
            metrics: {
              counters: [{ counter: "metadata_committed", value: count }],
            },
          },
        },
      },
      active: [],
    },
    cached: { nodes: [] },
  });
}
