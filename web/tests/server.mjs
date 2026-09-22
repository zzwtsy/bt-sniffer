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
let databaseObservedAt = Date.now();
let replayTimer;
let replayBatches = 0;
function stopReplay() {
  clearInterval(replayTimer);
  replayTimer = undefined;
}
function replay(count) {
  stopReplay();
  replayBatches = 0;
  replayTimer = setInterval(() => {
    const steps = [
      ["discovery", "hash_saved", "new"],
      ["admission", "admission", "applied"],
      ["job", "claim", "applied"],
      ["lookup", "lookup", "started"],
      ["peer", "connect", "started"],
      ["piece", "receive", "accepted"],
      ["validation", "metadata", "validated"],
      ["commit", "complete_transaction", "started"],
      ["commit", "metadata", "applied"],
      ["commit", "complete_transaction", "applied"],
    ];
    const batch = Array.from({ length: count }, (_, i) => {
      const transaction = Math.floor((replayBatches * count + i) / 10);
      const [kind, step, result] = steps[i % steps.length];
      const outcome = i % 10 === 9 && transaction % 5 === 0 ? "failed" : result;
      if (kind === "commit" && step === "complete_transaction" && outcome === "applied")
        committed++;
      return add(kind, step, outcome, {}, { hash: (transaction + 1).toString(16).padStart(40, "0") });
    });
    broadcast("events", { events: batch, next: String(latest), window: window(), completeness: "complete" }, `${run}:${latest}`);
    if (++replayBatches >= 175)
      stopReplay();
  }, 200);
}
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
function snapshot() {
  if (Date.now() - databaseObservedAt >= 30_000)
    databaseObservedAt = Date.now();
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
        observed_at_ms: databaseObservedAt,
        value: {
          jobs: { pending: 0, running: 0, retry_wait: 1, dormant: 0, succeeded: 0 },
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
function animationHash(index) {
  return index === 0 ? hash : index.toString(16).padStart(40, "0");
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
  add("commit", "metadata", "applied");
  add("commit", "complete_transaction", "applied");
}
seed();
const server = createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  const json = (value) => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(value));
  };
  if (url.pathname.startsWith("/control/")) {
    if (url.pathname === "/control/replay") {
      replay(url.searchParams.get("count") === "100" ? 100 : 20);
    }
    if (url.pathname === "/control/replay-stop")
      stopReplay();
    if (url.pathname === "/control/disconnect") {
      for (const stream of streams) stream.end();
    }
    if (url.pathname === "/control/reset") {
      stopReplay();
      replayBatches = 0;
      committed = 1;
      databaseObservedAt = Date.now();
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
      const batch = [
        add("discovery", "hash_saved", "new"),
        add("commit", "complete_transaction", "started"),
        add("commit", "metadata", "applied"),
        add("commit", "complete_transaction", "applied"),
      ];
      if (url.searchParams.has("failed"))
        batch.push(add("commit", "complete_transaction", "failed", {}, { generation: 3 }));
      broadcast(
        "events",
        {
          events: batch,
          next: String(latest),
          window: window(),
          completeness: "complete",
        },
        `${run}:${latest}`,
      );
      broadcast("snapshot", snapshot());
    }
    if (url.pathname === "/control/animation") {
      const batch = [
        add("discovery", "hash_saved", "new"),
        add("lookup", "lookup", "started"),
        add("peer", "connect", "failed", {}, { hash: "b".repeat(40) }),
        add("commit", "complete_transaction", "applied", {}, { hash: "c".repeat(40) }),
      ];
      broadcast("events", { events: batch, next: String(latest), window: window(), completeness: "complete" }, `${run}:${latest}`);
    }
    if (url.pathname === "/control/animation-load") {
      const offset = Number(url.searchParams.get("offset") ?? 0);
      const stage = url.searchParams.get("stage") === "lookup" ? "lookup" : "discovery";
      const batch = Array.from({ length: 100 }, (_, index) =>
        stage === "lookup"
          ? add("lookup", "lookup", "started", {}, { hash: animationHash(offset + index) })
          : add("discovery", "hash_saved", "new", {}, { hash: animationHash(offset + index) }));
      broadcast("events", { events: batch, next: String(latest), window: window(), completeness: "complete" }, `${run}:${latest}`);
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
    return json({ streams: streams.size, cursors, latest, replayBatches });
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
  res.writeHead(404, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({ error: { code: "not_found", message: "记录不存在" } }),
  );
});
server.listen(4311, "127.0.0.1");
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    stopReplay();
    for (const stream of streams) stream.end();
    server.close();
  });
}
