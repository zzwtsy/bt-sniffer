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
const torrent = {
  hash,
  format: "v1",
  semantic_status: "valid",
  semantic_reason: null,
  identities: [],
  verification: ["v1_full"],
  validation_scope: "info_only",
  piece_layers: "not_fetched",
  piece_space_length: "9007199254740993",
  padding_length: "0",
  parse_status: "parsed",
  name: "Fixture Torrent",
  name_truncated: false,
  encoding_lossy: false,
  total_length: "9007199254740993",
  file_count: 2,
  piece_length: "16384",
  piece_count: 2,
  private: false,
  fetched_at_ms: Date.now(),
};
const archive = {
  hash: "b".repeat(40),
  format: "v1",
  semantic_status: "valid",
  semantic_reason: null,
  identities: [],
  verification: ["v1_full"],
  validation_scope: "info_only",
  piece_layers: "not_fetched",
  piece_space_length: "9007199254740993",
  padding_length: "0",
  parse_status: "parsed",
  name: "Fixture Archive",
  name_truncated: false,
  encoding_lossy: false,
  total_length: "1024",
  file_count: 1,
  piece_length: "16384",
  piece_count: 1,
  private: false,
  fetched_at_ms: Date.now() - 60_000,
};
const big = {
  hash: "c".repeat(40),
  format: "v1",
  semantic_status: "valid",
  semantic_reason: null,
  identities: [],
  verification: ["v1_full"],
  validation_scope: "info_only",
  piece_layers: "not_fetched",
  piece_space_length: "9007199254740993",
  padding_length: "0",
  parse_status: "parsed",
  name: "Fixture Big",
  name_truncated: false,
  encoding_lossy: false,
  total_length: "1048576",
  file_count: 8000,
  piece_length: "16384",
  piece_count: 64,
  private: false,
  fetched_at_ms: Date.now() - 120_000,
};
/** 无空格长名称 fixture：覆盖结果表名称列的换行与两行截断。 */
const long = {
  hash: "d".repeat(40),
  format: "v1",
  semantic_status: "valid",
  semantic_reason: null,
  identities: [],
  verification: ["v1_full"],
  validation_scope: "info_only",
  piece_layers: "not_fetched",
  piece_space_length: "9007199254740993",
  padding_length: "0",
  parse_status: "parsed",
  name: "NEW.Anna.Ralphs.Riding.After.Erotic.Massage.sxyprn.amateur.ass.bigass.bigtits.boobs.deepthroat.hardcore.hot.onlyfans.porn.hub.sexy.mp4",
  name_truncated: false,
  encoding_lossy: false,
  total_length: "219785428",
  file_count: 1,
  piece_length: "16384",
  piece_count: 13415,
  private: false,
  fetched_at_ms: Date.now() - 180_000,
};
const records = [torrent, archive, big, long];
/** 填充记录：把目录撑到两页以覆盖页码分页；名称含 Fixture，在 fixture 搜索中同样出现。 */
const fillers = Array.from({ length: 96 }, (_, index) => ({
  hash: (index + 16).toString(16).padStart(40, "0"),
  format: "v1",
  semantic_status: "valid",
  semantic_reason: null,
  identities: [],
  verification: ["v1_full"],
  validation_scope: "info_only",
  piece_layers: "not_fetched",
  piece_space_length: "9007199254740993",
  padding_length: "0",
  parse_status: "parsed",
  name: `Fixture Filler ${String(index).padStart(3, "0")}`,
  name_truncated: false,
  encoding_lossy: false,
  total_length: "64",
  file_count: 1,
  piece_length: "16384",
  piece_count: 1,
  private: false,
  fetched_at_ms: Date.now() - 240_000 - index * 60_000,
}));
/** 目录顺序（采集时间倒序）：空 q 与 fixture 搜索下 torrent 都在第 1 页、archive 都在第 2 页。 */
for (const record of [...records, ...fillers]) record.identities = [{ kind: "v1", hash: record.hash }];
const catalogOrder = [torrent, long, ...fillers.slice(0, 49), archive, big, ...fillers.slice(49)];
/** torrent 250 个文件覆盖嵌套目录与三层深度；archive 两个文件单页。 */
function fixtureFiles(record) {
  const files = [
    { index: 0, path: `${record.name}/folder/movie.mkv`, kind: "file", hidden: false, executable: false, symlink_path: null, sha1: null, path_truncated: false, encoding_lossy: false, length: "9007199254740000" },
    { index: 1, path: `${record.name}/readme.txt`, kind: "file", hidden: false, executable: false, symlink_path: null, sha1: null, path_truncated: false, encoding_lossy: false, length: "993" },
  ];
  if (record !== torrent)
    return files;
  for (let index = 2; index < 250; index++) {
    const bucket = index % 3;
    const path = bucket === 0
      ? `${record.name}/folder/docs/doc-${index}.txt`
      : bucket === 1
        ? `${record.name}/photos/2024/raw/img-${index}.jpg`
        : `${record.name}/extras/file-${index}.bin`;
    files.push({ index, path, kind: "file", hidden: false, executable: false, symlink_path: null, sha1: null, path_truncated: false, encoding_lossy: false, length: String(1000 + index) });
  }
  return files;
}
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
      return add(kind, step, outcome, {}, { swarm_key: (transaction + 1).toString(16).padStart(40, "0") });
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
    schema_version: 2,
    run_id: run,
    sequence: String(++latest),
    at_ms: Date.now(),
    kind,
    step,
    result,
    context: { swarm_key: hash, generation: 2, batch_id: "batch-1", ...context },
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
        add("peer", "connect", "failed", {}, { swarm_key: "b".repeat(40) }),
        add("commit", "complete_transaction", "applied", {}, { swarm_key: "c".repeat(40) }),
      ];
      broadcast("events", { events: batch, next: String(latest), window: window(), completeness: "complete" }, `${run}:${latest}`);
    }
    if (url.pathname === "/control/animation-load") {
      const offset = Number(url.searchParams.get("offset") ?? 0);
      const stage = url.searchParams.get("stage") === "lookup" ? "lookup" : "discovery";
      const batch = Array.from({ length: 100 }, (_, index) =>
        stage === "lookup"
          ? add("lookup", "lookup", "started", {}, { swarm_key: animationHash(offset + index) })
          : add("discovery", "hash_saved", "new", {}, { swarm_key: animationHash(offset + index) }));
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
  if (url.pathname === "/api/v1/torrents") {
    const query = url.searchParams.get("q")?.toLowerCase();
    const page = Math.max(1, Number(url.searchParams.get("page") ?? 1) || 1);
    const limit = Math.max(1, Number(url.searchParams.get("limit") ?? 50) || 50);
    const path = "Fixture Torrent/folder/movie.mkv".toLowerCase();
    const matched = catalogOrder.filter(item =>
      query == null
      || item.name.toLowerCase().includes(query)
      || (item === torrent && path.includes(query)));
    const items = matched.map(item =>
      query != null && item === torrent && path.includes(query)
        ? { ...item, match_excerpt: "Fixture Torrent/folder/movie.mkv" }
        : item);
    return json({
      items: items.slice((page - 1) * limit, page * limit),
      total: items.length,
      page,
      index: query === "missing"
        ? { indexed: 2, total: 2, complete: true, search_complete: false }
        : { indexed: 1, total: 2, complete: false, search_complete: false },
    });
  }
  const detailMatch = /^\/api\/v1\/torrents\/([0-9a-f]{40})$/.exec(url.pathname);
  if (detailMatch) {
    const record = records.find(item => item.hash === detailMatch[1]);
    if (record != null)
      return json(record);
  }
  const filesMatch = /^\/api\/v1\/torrents\/([0-9a-f]{40})\/files$/.exec(url.pathname);
  if (filesMatch) {
    const record = records.find(item => item.hash === filesMatch[1]);
    if (record != null) {
      const start = Number(url.searchParams.get("after") ?? 0);
      const limit = Number(url.searchParams.get("limit") ?? 100);
      if (record === big) {
        // 无限页 fixture：供前端全量拉取上限（50 页 / 5000 条）测试
        const items = Array.from({ length: limit }, (_, offset) => {
          const index = start + offset;
          return { index, path: `${record.name}/group/sub-${Math.floor(index / 100)}/file-${index}.bin`, kind: "file", hidden: false, executable: false, symlink_path: null, sha1: null, path_truncated: false, encoding_lossy: false, length: "128" };
        });
        return json({ available: true, items, next: String(start + limit) });
      }
      const all = fixtureFiles(record);
      return json({
        available: true,
        items: all.slice(start, start + limit),
        next: start + limit < all.length ? String(start + limit) : null,
      });
    }
  }
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
