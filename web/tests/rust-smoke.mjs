/** 在全新目录启动无引导节点，验证生产构建、同源代理和真实 SSE。 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { chromium } from "@playwright/test";
import {
  snapshotSchema,
  windowSchema,
} from "../src/lib/observation/contracts.ts";

async function* sseEvents(body) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      let event = "message";
      const data = [];
      for (const line of block.split("\n")) {
        if (line.startsWith("event:"))
          event = line.slice(6).trim();
        else if (line.startsWith("data:"))
          data.push(line.slice(5).trimStart());
      }
      if (data.length > 0)
        yield { event, data: data.join("\n") };
      boundary = buffer.indexOf("\n\n");
    }
    if (done)
      return;
  }
}

async function nextEvent(events, expected) {
  while (true) {
    const { value: event, done } = await events.next();
    if (done)
      break;
    if (event.event === expected)
      return JSON.parse(event.data);
  }
  throw new Error(`SSE 在结束前没有发送 ${expected}`);
}

async function main() {
  const root = fileURLToPath(new URL("../", import.meta.url));
  const repo = path.dirname(root.replace(/\/$/, ""));
  const evidence = path.join(repo, "target/checks/frontend-rust-smoke");
  await mkdir(evidence, { recursive: true });
  const state = await mkdtemp(path.join(evidence, "state-"));
  async function port() {
    const socket = createServer();
    socket.listen(0, "127.0.0.1");
    await once(socket, "listening");
    const value = socket.address().port;
    await new Promise(resolve => socket.close(resolve));
    return value;
  }
  const apiPort = await port();
  const webPort = await port();
  const children = [];
  const logs = [];
  let browser;
  let result;
  const exits = [];
  async function child(command, args, options = {}) {
    const proc = spawn(command, args, {
      cwd: state,
      detached: true,
      ...options,
    });
    children.push(proc);
    const log = createWriteStream(
      path.join(evidence, `${children.length}.log`),
    );
    logs.push(log);
    proc.stdout.pipe(log, { end: false });
    proc.stderr.pipe(log, { end: false });
    await once(proc, "spawn");
    return proc;
  }
  try {
    await child(path.join(repo, "target/debug/bt-sniffer"), [
      "--state-dir",
      state,
      "--ipv4-only",
      "--listen-v4",
      "127.0.0.1:0",
      "--no-bootstrap",
      "--allow-local",
      "--monitor-listen",
      `127.0.0.1:${apiPort}`,
    ]);
    await child(
      "pnpm",
      ["exec", "vite", "preview", "--port", String(webPort), "--strictPort"],
      {
        cwd: root,
        env: {
          ...process.env,
          MONITOR_PROXY_TARGET: `http://127.0.0.1:${apiPort}`,
        },
      },
    );
    const url = `http://127.0.0.1:${webPort}`;
    let snapshot;
    for (let i = 0; i < 100; i++) {
      try {
        const response = await fetch(`${url}/api/v1/snapshot`, {
          signal: AbortSignal.timeout(1000),
        });
        if (response.ok) {
          snapshot = snapshotSchema.parse(await response.json());
          break;
        }
      } catch {
        /* 服务启动前连接被拒绝，继续有界等待。 */
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(snapshot?.schema_version, 1);
    const catalogResponse = await fetch(`${url}/api/v1/torrents`);
    assert.equal(catalogResponse.status, 200);
    const catalog = await catalogResponse.json();
    assert.deepEqual(catalog.items, []);
    assert.deepEqual(catalog.index, { indexed: 0, total: 0, complete: true, search_complete: true });
    assert.equal((await fetch(`${url}/api/v1/metadata`)).status, 404);
    const abort = new AbortController();
    const stream = await fetch(`${url}/api/v1/stream`, {
      signal: AbortSignal.any([abort.signal, AbortSignal.timeout(5000)]),
    });
    assert.ok(stream.body);
    const events = sseEvents(stream.body);
    windowSchema.parse(await nextEvent(events, "hello"));
    snapshotSchema.parse(await nextEvent(events, "snapshot"));
    abort.abort();
    browser = await chromium.launch();
    const page = await browser.newPage();
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(url);
    await page.getByText("实时连接", { exact: true }).waitFor();
    await page.screenshot({
      path: path.join(evidence, "overview.png"),
      fullPage: true,
    });
    await page.goto(`${url}/torrents`);
    await page.getByRole("heading", { name: "种子查询" }).waitFor();
    await page.getByText("尚未保存可展示的 metadata。", { exact: true }).waitFor();
    assert.deepEqual(errors, []);
    result = {
      status: "passed",
      run_id: snapshot.window.run_id,
      no_bootstrap: true,
      state,
      proxy: true,
      sse: true,
      contract: true,
    };
  } finally {
    await browser?.close();
    for (const proc of children) {
      if (proc.exitCode !== null)
        continue;
      process.kill(-proc.pid, "SIGINT");
      const timer = setTimeout(
        () => process.kill(-proc.pid, "SIGKILL"),
        30_000,
      );
      try {
        await once(proc, "exit");
      } finally {
        clearTimeout(timer);
      }
      exits.push({ code: proc.exitCode, signal: proc.signalCode });
      if (proc === children[0])
        assert.equal(proc.exitCode, 0);
      else assert.ok(proc.exitCode === 0 || proc.signalCode === "SIGINT");
    }
    for (const log of logs) log.end();
  }

  await writeFile(
    path.join(evidence, "result.json"),
    JSON.stringify({ ...result, exits }, null, 2),
  );
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
