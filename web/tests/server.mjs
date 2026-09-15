/** 独立本机协议夹具；控制端点仅存在于测试进程。 */
import { createServer } from "node:http";
import process from "node:process";

const hash = "a".repeat(40);
let run = "fixture";
let latest = 0;
let committed = 1;
const streams = new Set();
const cursors = [];
const events = [];
function window() {
  return {
    run_id: run,
    oldest: String(Math.max(1, latest - 65535)),
    latest: String(latest),
    retained: Math.min(latest, 65536),
    bytes: events.length * 250,
    evicted: Math.max(0, latest - 65536),
    truncated: 0,
  };
}
const node = {
  id: "0",
  family: "ipv4",
  available: true,
  node_id: "b".repeat(40),
  address: "127.0.0.1:6881",
  pending: 1,
  queued: 0,
  observed_at_ms: Date.now(),
  routing: { contact_count: 1 },
  sampling: { running: false },
  rpc: { pending: [], queued: [] },
};
const job = {
  hash,
  state: "succeeded",
  generation: 2,
  remote_failures: 1,
  due_at_ms: 1000,
  updated_at_ms: 2000,
  error: null,
};
const metadata = {
  hash,
  bytes: 32768,
  fetched_at_ms: 2000,
  verification: "validated_before_commit",
  content_rechecked: false,
};
const fact = {
  hash,
  first_seen_ms: 1000,
  last_seen_ms: 2000,
  job,
  metadata,
  peer_hints: [],
  original_source: null,
};
function snapshot() {
  return {
    schema_version: 1,
    window: window(),
    runtime: {
      sources: {
        config: {
          observed_at_ms: Date.now(),
          value: { fetch: true, sample: true },
        },
        collector: {
          observed_at_ms: Date.now(),
          value: {
            running_workers: 1,
            capacity_paused: false,
            metrics: {
              counters: [{ counter: "metadata_committed", value: committed }],
            },
          },
        },
      },
      active: [],
    },
    cached: {
      nodes: [node],
      database: {
        available: true,
        stale: false,
        observed_at_ms: Date.now(),
        value: {
          jobs: { retry_wait: 1 },
          metadata_count: committed,
          metadata_bytes: 32768,
        },
      },
    },
  };
}
function send(stream, name, data, id) {
  stream.write(
    `${id ? `id: ${id}\n` : ""}event: ${name}\ndata: ${JSON.stringify(data)}\n\n`,
  );
}
function broadcast(name, value, id) {
  for (const stream of streams) send(stream, name, value, id);
}
function add(kind, step, result, data = {}, context = {}) {
  const event = {
    schema_version: 1,
    run_id: run,
    sequence: String(++latest),
    at_ms: Date.now(),
    kind,
    step,
    result,
    context: { hash, generation: 2, batch_id: "batch-1", ...context },
    data,
    truncated: false,
  };
  events.push(event);
  if (events.length > 65536)
    events.shift();
  return event;
}
function seed() {
  add("discovery", "save", "applied");
  add("job", "claim", "applied", {
    attempt_kind: "repeat",
    failed_attempts_before: 1,
  });
  add(
    "peer",
    "handshake",
    "failed",
    {},
    { peer_attempt_id: "peer-1", generation: 1 },
  );
  add(
    "piece",
    "receive",
    "accepted",
    { piece: 0, received_pieces: 1, total_pieces: 2 },
    { peer_attempt_id: "peer-2" },
  );
  add(
    "piece",
    "receive",
    "duplicate",
    { piece: 0, received_pieces: 1, total_pieces: 2 },
    { peer_attempt_id: "peer-2" },
  );
  add("commit", "commit", "applied");
}
seed();
const server = createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  const json = (value) => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(value));
  };
  const page = items => ({ items, next: null });
  if (url.pathname.startsWith("/control/")) {
    if (url.pathname === "/control/disconnect") {
      for (const stream of streams) stream.end();
    }
    if (url.pathname === "/control/reset") {
      run = `fixture-${Date.now()}`;
      latest = 0;
      events.length = 0;
      seed();
      broadcast("reset", window());
    }
    if (url.pathname === "/control/capacity")
      broadcast("reset", { reason: "response_capacity" });
    if (url.pathname === "/control/update") {
      committed++;
      broadcast("snapshot", snapshot());
    }
    if (url.pathname === "/control/load") {
      for (let batch = 0; batch < 512; batch++) {
        const chunk = Array.from({ length: 100 }, () =>
          add(
            "piece",
            "receive",
            "accepted",
            { piece: 0 },
            { batch_id: `batch-${batch % 100}` },
          ));
        broadcast(
          "events",
          {
            events: chunk,
            next: String(latest),
            window: window(),
            completeness: "complete",
          },
          `${run}:${latest}`,
        );
        await new Promise(resolve => setTimeout(resolve, 1));
      }
    }
    return json({ streams: streams.size, cursors, latest });
  }
  if (url.pathname === "/api/v1/stream") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
    });
    streams.add(res);
    cursors.push(url.searchParams.get("after"));
    if (cursors.length > 20)
      cursors.shift();
    send(res, "hello", window());
    const heartbeat = setInterval(
      () => send(res, "snapshot", snapshot()),
      1000,
    );
    req.on("close", () => {
      streams.delete(res);
      clearInterval(heartbeat);
    });
    return;
  }
  if (url.pathname === "/api/v1/health")
    return json({ phase: "running" });
  if (url.pathname === "/api/v1/snapshot")
    return json(snapshot());
  if (url.pathname === "/api/v1/hashes") {
    return json({
      items: [
        {
          hash: url.searchParams.has("after") ? "b".repeat(40) : hash,
          first_seen_ms: 1000,
          last_seen_ms: 2000,
        },
      ],
      next: url.searchParams.has("after") ? null : hash,
    });
  }
  if (url.pathname === `/api/v1/hashes/${hash}`)
    return json(fact);
  if (url.pathname.endsWith("/attempts")) {
    return json({
      ...page(
        [1, 2].map(generation => ({
          generation,
          claim: { attempt_kind: "repeat", failed_attempts_before: 1 },
          peers: [{ peer_attempt_id: `peer-${generation}` }],
          last_result: generation === 2 ? "applied" : "failed",
        })),
      ),
      window: window(),
      completeness: "partial",
    });
  }
  if (url.pathname === "/api/v1/jobs")
    return json(page([job]));
  if (url.pathname === "/api/v1/metadata")
    return json(page([metadata]));
  if (url.pathname === "/api/v1/discoveries") {
    return json({
      ...page([
        {
          id: "batch-1",
          source: "sample",
          first_sequence: "1",
          last_sequence: "6",
          first_retained_at_ms: 1000,
          last_retained_at_ms: 2000,
          last_step: "save",
          last_result: "applied",
          completeness: "complete",
        },
      ]),
      window: window(),
    });
  }
  if (url.pathname === "/api/v1/dht/nodes")
    return json([node]);
  if (url.pathname.endsWith("/routing")) {
    return json({
      ...page([
        {
          node_id: node.node_id,
          address: node.address,
          status: "good",
          last_response_age_ms: 100,
        },
      ]),
      observed_at_ms: Date.now(),
      buckets: [{ id: "0", count: 1, capacity: 8 }],
    });
  }
  if (
    url.pathname === "/api/v1/events"
    || url.pathname.startsWith("/api/v1/discoveries/")
  ) {
    const selected = events
      .filter(
        e =>
          BigInt(e.sequence) > BigInt(url.searchParams.get("after") || "0")
          && (!url.searchParams.get("kind")
            || e.kind === url.searchParams.get("kind")),
      )
      .slice(0, Number(url.searchParams.get("limit") || 50));
    return json({
      events: selected,
      next: selected.at(-1)?.sequence ?? String(latest),
      window: window(),
      completeness: "complete",
    });
  }
  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({ error: { code: "not_found", message: "记录不存在" } }),
  );
});
server.listen(4311, "127.0.0.1");
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    for (const stream of streams) stream.end();
    server.close();
  });
}
