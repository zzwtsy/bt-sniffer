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
          snapshot = await response.json();
          break;
        }
      } catch {
        /* 服务启动前连接被拒绝，继续有界等待。 */
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(snapshot?.schema_version, 1);
    const abort = new AbortController();
    const stream = await fetch(`${url}/api/v1/stream`, {
      signal: AbortSignal.any([abort.signal, AbortSignal.timeout(5000)]),
    });
    const first = await stream.body.getReader().read();
    assert.match(new TextDecoder().decode(first.value), /event: ?hello/);
    abort.abort();
    browser = await chromium.launch();
    const page = await browser.newPage();
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.goto(url);
    await page.getByText("实时连接", { exact: true }).waitFor();
    await page.goto(`${url}/dht`);
    await page.getByText("查看路由、采样与 RPC →").first().waitFor();
    await page.screenshot({
      path: path.join(evidence, "dht.png"),
      fullPage: true,
    });
    assert.deepEqual(errors, []);
    result = {
      status: "passed",
      run_id: snapshot.window.run_id,
      no_bootstrap: true,
      state,
      proxy: true,
      sse: true,
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
