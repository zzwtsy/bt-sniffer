/** 生产总览对照；参数为已构建目录和证据目录，不加入日常短回归。 */
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { chromium, firefox } from "@playwright/test";
import { preview } from "vite";

async function main() {
  const [build, output, browserName = "chromium"] = process.argv.slice(2);
  if (!["chromium", "firefox"].includes(browserName))
    throw new Error("browser must be chromium or firefox");
  if (!build || !output)
    throw new Error("usage: node tests/overview-load.mjs BUILD OUTPUT [chromium|firefox]");
  await mkdir(output, { recursive: true });
  const fixture = spawn(process.execPath, ["tests/server.mjs"], { stdio: "inherit" });
  let browser;
  let site;
  const results = [];
  try {
    for (let i = 0; ; i++) {
      try {
        const response = await fetch("http://127.0.0.1:4311/api/v1/health");
        if (response.ok)
          break;
      } catch { /* 等待本机夹具启动 */ }
      if (i === 50)
        throw new Error("fixture did not start");
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    process.env.MONITOR_PROXY_TARGET = "http://127.0.0.1:4311";
    site = await preview({ build: { outDir: path.resolve(build) }, preview: { host: "127.0.0.1", port: 4183, strictPort: true } });
    browser = browserName === "firefox"
      ? await firefox.launch({ env: {
          ...process.env,
          MOZ_PROFILER_STARTUP: "1",
          MOZ_PROFILER_STARTUP_FILTERS: "GeckoMain",
          MOZ_PROFILER_STARTUP_FEATURES: "js,stackwalk,cpu",
          MOZ_PROFILER_STARTUP_INTERVAL: "2",
          MOZ_PROFILER_STARTUP_ENTRIES: "8000000",
          MOZ_PROFILER_SHUTDOWN: path.resolve(output, "firefox-profile.json"),
        } })
      : await chromium.launch();
    for (const count of [20, 100]) {
      for (let repeat = 0; repeat < 3; repeat++) {
        await fetch("http://127.0.0.1:4311/control/reset");
        const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, colorScheme: "light" });
        const page = await context.newPage();
        const errors = [];
        page.on("pageerror", error => errors.push(error.message));
        await page.addInitScript(() => {
          const stats = { longTasks: [], mutations: 0, commits: 0 };
          window.workload = stats;
          window.__REACT_DEVTOOLS_GLOBAL_HOOK__ = {
            supportsFiber: true,
            inject: () => 1,
            onCommitFiberRoot: () => stats.commits++,
            onCommitFiberUnmount: () => {},
          };
          if (PerformanceObserver.supportedEntryTypes.includes("longtask"))
            new PerformanceObserver(list => stats.longTasks.push(...list.getEntries().map(e => e.duration))).observe({ type: "longtask" });
          addEventListener("DOMContentLoaded", () => {
            new MutationObserver(() => stats.mutations++).observe(document.getElementById("root"), { childList: true, subtree: true, characterData: true });
          });
        });
        await page.goto("http://127.0.0.1:4183/");
        await page.getByText("实时连接", { exact: true }).waitFor();
        const cdp = browserName === "chromium" ? await context.newCDPSession(page) : undefined;
        await cdp?.send("Performance.enable");
        await fetch(`http://127.0.0.1:4311/control/replay?count=${count}`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        await page.evaluate(() => {
          window.workload.longTasks = [];
          window.workload.mutations = window.workload.commits = 0;
        });
        const start = await cdp?.send("Performance.getMetrics");
        const began = Date.now();
        await new Promise(resolve => setTimeout(resolve, 30_000));
        const end = await cdp?.send("Performance.getMetrics");
        const delivery = await (await fetch("http://127.0.0.1:4311/control/replay-stop")).json();
        const stats = await page.evaluate(() => window.workload);
        const metric = (data, name) => data?.metrics.find(m => m.name === name)?.value ?? 0;
        const result = {
          count,
          repeat,
          started_at_ms: began,
          ended_at_ms: Date.now(),
          elapsed_ms: Date.now() - began,
          delivery,
          errors,
          main_task_seconds: cdp ? metric(end, "TaskDuration") - metric(start, "TaskDuration") : null,
          long_tasks: cdp ? stats.longTasks.length : null,
          long_tasks_over_200ms: cdp ? stats.longTasks.filter(value => value > 200).length : null,
          long_task_ms: cdp ? stats.longTasks.reduce((sum, value) => sum + value, 0) : null,
          mutations: stats.mutations,
          react_commits: stats.commits,
          particles: await page.locator("[data-slot='pipeline-particle']").count(),
        };
        results.push(result);
        await writeFile(path.join(output, "results.json"), JSON.stringify({ browser: browser.version(), build: path.resolve(build), results }, null, 2));
        process.stdout.write(`${JSON.stringify(result)}\n`);
        await context.close();
        if (errors.length)
          throw new Error(errors.join("\n"));
      }
    }
  } finally {
    await browser?.close();
    if (site)
      await new Promise(resolve => site.httpServer.close(resolve));
    if (fixture.exitCode === null) {
      const ended = once(fixture, "exit");
      fixture.kill("SIGTERM");
      await ended;
    }
  }
}
void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
