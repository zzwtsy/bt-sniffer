import type { Page } from "@playwright/test";
import { mkdir, writeFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";

const hash = "a".repeat(40);
const themeNames = { light: "浅色", dark: "深色" } as const;
async function selectTheme(
  page: Page,
  theme: keyof typeof themeNames,
  options: { advanceClock?: boolean } = {},
) {
  await page.getByLabel("主题", { exact: true }).click();
  if (options.advanceClock)
    await page.clock.runFor(100);
  await page
    .getByRole("menuitem", { name: themeNames[theme], exact: true })
    .click();
}

test("深浅主题的重要状态文字保持可读对比度", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await request.get("http://127.0.0.1:4311/control/update");
  // 首页截图保留为深浅主题视觉检查产物
  for (const theme of ["light", "dark"] as const) {
    await selectTheme(page, theme);
    await page.screenshot({
      path: `../target/checks/frontend-${theme}.png`,
      fullPage: true,
    });
  }
  await expect(page.locator(".status").first()).toBeVisible();
  for (const theme of ["light", "dark"] as const) {
    await selectTheme(page, theme);
    const ratios = await page.locator(".status").evaluateAll(elements =>
      elements.map((element) => {
        const canvas = document.createElement("canvas");
        canvas.width = canvas.height = 1;
        const context = canvas.getContext("2d")!;
        context.fillStyle = "white";
        context.fillRect(0, 0, 1, 1);
        const ancestors: Element[] = [];
        for (
          let current: Element | null = element;
          current;
          current = current.parentElement
        )
          ancestors.push(current);
        for (const ancestor of ancestors.reverse()) {
          context.fillStyle = getComputedStyle(ancestor).backgroundColor;
          context.fillRect(0, 0, 1, 1);
        }
        const luminance = (rgb: Uint8ClampedArray) => {
          const channels = [...rgb]
            .slice(0, 3)
            .map(value => value / 255)
            .map(value =>
              value <= 0.04045
                ? value / 12.92
                : ((value + 0.055) / 1.055) ** 2.4,
            );
          return (
            channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722
          );
        };
        const background = luminance(context.getImageData(0, 0, 1, 1).data);
        context.fillStyle = getComputedStyle(element).color;
        context.fillRect(0, 0, 1, 1);
        const foreground = luminance(context.getImageData(0, 0, 1, 1).data);
        return (
          (Math.max(background, foreground) + 0.05)
          / (Math.min(background, foreground) + 0.05)
        );
      }),
    );
    expect(ratios.length).toBeGreaterThan(0);
    for (const ratio of ratios) expect(ratio).toBeGreaterThanOrEqual(4.5);
  }
});

test("首页可用，旧前端 URL 与旧 API 均不再匹配", async ({ page, request }) => {
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "采集流水线" }),
  ).toBeVisible();
  for (const path of [
    "/dht",
    "/dht/0",
    "/discoveries",
    "/discoveries/example",
    "/jobs",
    "/hashes",
    `/hashes/${hash}`,
    `/hashes/${hash}/attempts`,
    "/metadata",
    "/events",
  ]) {
    await page.goto(path);
    await expect(page.getByRole("heading", { name: "页面不存在" })).toBeVisible();
    await expect(page.getByRole("link", { name: "返回流程总览" })).toBeVisible();
    await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  }
  for (const path of [
    "/api/v1/dht/nodes",
    "/api/v1/discoveries",
    "/api/v1/hashes",
    "/api/v1/jobs",
    "/api/v1/metadata",
    "/api/v1/events",
  ]) {
    expect((await request.get(`http://127.0.0.1:4311${path}`)).status()).toBe(404);
  }
  expect(errors).toEqual([]);
});

test("冻结、断线续传、运行 reset 和容量 reset", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  const metric = page
    .locator(".metric")
    .filter({ hasText: "提交吞吐" })
    .locator(".metric-value");
  const before = await metric.innerText();
  await page.getByRole("button", { name: "冻结显示", exact: true }).click();
  await request.get("http://127.0.0.1:4311/control/update");
  await expect(page.getByText(/画面已冻结于/)).toBeVisible();
  await expect(metric).toHaveText(before);
  await page.getByRole("button", { name: "恢复实时显示", exact: true }).click();
  await expect(metric).not.toHaveText(before);
  await request.get("http://127.0.0.1:4311/control/disconnect");
  await expect(page.getByText("正在重连", { exact: true })).toBeVisible();
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await request.get("http://127.0.0.1:4311/control/reset");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await request.get("http://127.0.0.1:4311/control/capacity");
  await expect(page.getByText("同步不可用", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "重新同步" }).click();
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await expect
    .poll(
      async () =>
        (
          (await (
            await request.get("http://127.0.0.1:4311/control/status")
          ).json()) as { streams: number }
        ).streams,
    )
    .toBe(1);
});

test("首页七泳道渲染与结果分布过滤", async ({
  page,
  request,
}) => {
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  const lanes = page
    .getByRole("group", { name: "流水线泳道" })
    .getByRole("button");
  await expect(lanes).toHaveCount(7);
  for (const title of ["发现", "接纳", "领取", "查找", "下载", "校验", "提交"])
    await expect(lanes.filter({ hasText: title })).toBeVisible();
  const success = page.locator(".metric").filter({ hasText: "成功率" }).locator(".metric-value");
  await expect(success).toHaveText("—");
  await request.get("http://127.0.0.1:4311/control/update?failed=1");
  await expect(success).toHaveText("50.0%");
  await expect(lanes.filter({ hasText: "提交" })).toContainText("1");
  const stream = page.locator("[data-slot='card']", {
    has: page.getByText(/当前窗口保留最近/),
  });
  await expect(stream.getByText("主要失败原因")).toBeVisible();
  await expect(stream.locator("li", { hasText: "失败" })).toBeVisible();
  await lanes.filter({ hasText: "提交" }).click();
  await expect(stream.getByText(/已过滤：提交/)).toBeVisible();
  await expect(stream.locator("li", { hasText: "失败" })).toBeVisible();
  await lanes.filter({ hasText: "发现" }).click();
  await expect(stream.getByText(/已过滤：发现/)).toBeVisible();
  await expect(stream.getByText("窗口内无失败事件。")).toBeVisible();
  await stream.getByText(/已过滤：发现/).click();
  await expect(stream.locator("li", { hasText: "失败" })).toBeVisible();
  expect(errors).toEqual([]);
});

test("窄屏、深浅主题、键盘和放大视图", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await selectTheme(page, "dark");
  await expect(page.locator("html")).toHaveClass(/dark/);
  await selectTheme(page, "light");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "展开导航" }).click();
  await page.getByRole("navigation").getByText("流程总览").click();
  await expect(page.locator("h1")).toBeVisible();
  await page.keyboard.press("Tab");
  expect(await page.evaluate(() => document.activeElement?.tagName)).not.toBe(
    "BODY",
  );
  await page.evaluate(() => {
    document.documentElement.style.zoom = "2";
  });
  await expect(page.locator("h1")).toBeVisible();
  await page.screenshot({
    path: "../target/checks/frontend-narrow.png",
    fullPage: true,
  });
});

test("51,200 条事件的真实 SSE 有界负载", async ({ page, request }) => {
  await page.addInitScript(() => {
    const observations = {
      longTasks: [] as number[],
      commits: 0,
      reactCommits: 0,
    };
    Object.assign(window, {
      workload: observations,
      __REACT_DEVTOOLS_GLOBAL_HOOK__: {
        supportsFiber: true,
        inject: () => 1,
        onCommitFiberRoot: () => {
          observations.reactCommits++;
        },
        onCommitFiberUnmount: () => {},
      },
    });
    new PerformanceObserver(list =>
      observations.longTasks.push(...list.getEntries().map(e => e.duration)),
    ).observe({ type: "longtask" });
    addEventListener("DOMContentLoaded", () =>
      new MutationObserver(() => observations.commits++).observe(
        document.getElementById("root")!,
        { childList: true, subtree: true, characterData: true },
      ));
  });
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  const started = Date.now();
  await request.get("http://127.0.0.1:4311/control/load");
  await expect(page.locator("[data-slot='results-chart']")).toBeVisible({
    timeout: 15000,
  });
  expect(await page.locator("[data-slot='pipeline-particle']").count()).toBeLessThanOrEqual(500);
  const evidence = await page.evaluate(
    () => (window as unknown as { workload: unknown }).workload,
  );
  await mkdir("../target/checks/frontend-load", { recursive: true });
  await writeFile(
    "../target/checks/frontend-load/browser.json",
    JSON.stringify(
      {
        workload: 51200,
        elapsed_ms: Date.now() - started,
        evidence,
        note: "commits 是 DOM 变更批次数；本机生产构建，reactCommits 来自 React DevTools hook；不代表公网性能",
      },
      null,
      2,
    ),
  );
});

test("冻结保留粒子画面、主题色随变量自动生效，恢复延续寿命并处理运行切换", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await page.clock.install();
  await page.clock.pauseAt(new Date());
  await request.get("http://127.0.0.1:4311/control/animation");
  await page.clock.runFor(400);
  const particles = page.locator("[data-slot='pipeline-particle']");
  await expect(particles).not.toHaveCount(0);
  const falling = page.locator(`[data-slot='pipeline-particle'][data-hash='${"b".repeat(40)}']`);
  const fallingColor = async () =>
    falling.locator("span").evaluate(el => getComputedStyle(el).backgroundColor);
  const lightColor = await fallingColor();
  await page.getByRole("button", { name: "冻结显示", exact: true }).click({ force: true });
  await expect.poll(async () => page.locator("[data-slot='pipeline-stage']").evaluate(stage =>
    stage.getAnimations({ subtree: true }).every(animation => animation.playState === "paused"),
  )).toBe(true);
  const frozen = await falling.boundingBox();
  await page.clock.runFor(2_000);
  expect(await falling.boundingBox()).toEqual(frozen);
  await selectTheme(page, "dark", { advanceClock: true });
  expect(await fallingColor()).not.toBe(lightColor);
  await page.setViewportSize({ width: 1600, height: 1000 });
  const afterResize = await falling.boundingBox();
  expect(afterResize!.x).toBeGreaterThan(frozen!.x);
  await request.get("http://127.0.0.1:4311/control/update");
  await page.clock.runFor(5000);
  expect(await falling.boundingBox()).toEqual(afterResize);
  await page.getByRole("button", { name: "恢复实时显示", exact: true }).click({ force: true });
  await page.clock.runFor(300);
  expect(await falling.boundingBox()).not.toEqual(afterResize);
  await expect(page.locator(".metric").filter({ hasText: "成功率" }).locator(".metric-value")).toHaveText("100.0%");
  await page.getByRole("button", { name: "冻结显示", exact: true }).click({ force: true });
  const beforeReset = await particles.evaluateAll(list => list.map(element => element.getAttribute("data-hash")));
  await request.get("http://127.0.0.1:4311/control/reset");
  await page.clock.runFor(400);
  expect(await particles.evaluateAll(list => list.map(element => element.getAttribute("data-hash")))).toEqual(beforeReset);
  await page.getByRole("button", { name: "恢复实时显示", exact: true }).click({ force: true });
  await page.clock.runFor(400);
  const cleared = await particles.evaluateAll(list => list.map(element => element.getAttribute("data-hash")));
  expect(cleared).not.toEqual(beforeReset);
  await request.get("http://127.0.0.1:4311/control/animation");
  await page.clock.runFor(400);
  expect(await particles.evaluateAll(list => list.map(element => element.getAttribute("data-hash")))).not.toEqual(cleared);
});

test("200 粒子移动只合成 transform/opacity，并满足 Chromium 性能预算", async ({ page, request }) => {
  await page.addInitScript(() => {
    const evidence = { longTasks: [] as number[], reactCommits: 0, animationProperties: [] as string[][] };
    Object.assign(window, {
      animationEvidence: evidence,
      __REACT_DEVTOOLS_GLOBAL_HOOK__: {
        supportsFiber: true,
        inject: () => 1,
        onCommitFiberRoot: () => evidence.reactCommits++,
        onCommitFiberUnmount: () => {},
      },
    });
    new PerformanceObserver(list =>
      evidence.longTasks.push(...list.getEntries().map(entry => entry.duration)),
    ).observe({ type: "longtask", buffered: true });
    const animate = Object.getOwnPropertyDescriptor(Element.prototype, "animate")!.value as typeof Element.prototype.animate;
    // 保留原生调用的 this；箭头函数无法代理 Element.prototype 方法。
    Element.prototype.animate = function (keyframes, options) {
      const frames = Array.isArray(keyframes) ? keyframes : [];
      evidence.animationProperties.push(frames.flatMap(frame => Object.keys(frame)));
      return animate.call(this, keyframes, options);
    };
  });
  await request.get("http://127.0.0.1:4311/control/reset");
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  const particles = page.locator("[data-slot='pipeline-particle']");
  for (const offset of [0, 100]) {
    await request.get(`http://127.0.0.1:4311/control/animation-load?offset=${offset}&stage=discovery`);
    await expect(particles).toHaveCount(offset + 100);
  }

  const session = await page.context().newCDPSession(page);
  await session.send("Performance.enable");
  const metric = async (name: string) => {
    const result = await session.send("Performance.getMetrics");
    return result.metrics.find(item => item.name === name)?.value ?? 0;
  };
  const layoutsBefore = await metric("LayoutCount");
  const evidenceBefore = await page.evaluate(() => ({
    longTasks: (window as unknown as { animationEvidence: { longTasks: number[] } }).animationEvidence.longTasks.length,
  }));
  for (const offset of [0, 100]) {
    await request.get(`http://127.0.0.1:4311/control/animation-load?offset=${offset}&stage=lookup`);
    await page.waitForTimeout(250);
  }
  await page.waitForTimeout(250);
  const layouts = await metric("LayoutCount") - layoutsBefore;
  const animationProperties = await page.evaluate(() =>
    (window as unknown as { animationEvidence: { animationProperties: string[][] } }).animationEvidence.animationProperties,
  );
  expect(new Set(animationProperties.flat())).toEqual(new Set(["transform", "opacity"]));

  await page.evaluate(() => {
    (window as unknown as { animationEvidence: { reactCommits: number } }).animationEvidence.reactCommits = 0;
  });
  await page.waitForTimeout(2_000);
  const evidence = await page.evaluate(() =>
    (window as unknown as { animationEvidence: { longTasks: number[]; reactCommits: number } }).animationEvidence,
  );
  const windowLongTasks = evidence.longTasks.slice(evidenceBefore.longTasks);
  const report = {
    particle_count: await particles.count(),
    layout_count: layouts,
    long_tasks_ms: windowLongTasks,
    react_root_commits: evidence.reactCommits,
  };
  await mkdir("../target/checks/frontend-animation", { recursive: true });
  await writeFile(
    "../target/checks/frontend-animation/browser.json",
    JSON.stringify(report, null, 2),
  );
  expect(report.particle_count).toBe(200);
  expect(report.layout_count).toBeLessThanOrEqual(20);
  expect(report.long_tasks_ms.filter(duration => duration > 200)).toHaveLength(0);
  expect(report.react_root_commits).toBeLessThanOrEqual(8);
});

test("任务环图区分数据库不可用、完整零值和陈旧统计", async ({ page }) => {
  let database: Record<string, unknown> = { available: false, stale: true };
  await page.route("**/api/v1/snapshot", async (route) => {
    const response = await route.fetch();
    const snapshot = await response.json() as { cached: { database: unknown } };
    snapshot.cached.database = database;
    await route.fulfill({ json: snapshot });
  });
  await page.route("**/api/v1/stream?**", async route => route.abort());
  await page.goto("/");
  await expect(page.getByText("数据库统计尚不可用。", { exact: true })).toBeVisible();
  const jobs = { pending: 0, running: 0, retry_wait: 0, dormant: 0, succeeded: 0 };
  database = { available: true, value: { jobs } };
  await page.reload();
  await expect(page.getByText("数据库中尚无采集任务。", { exact: true })).toBeVisible();
  database = { available: true, stale: true, observed_at_ms: Date.now() - 60_000, value: { jobs: { ...jobs, pending: 3 } } };
  await page.reload();
  const chart = page.locator("[data-slot='card']").filter({ has: page.getByText("任务状态", { exact: true }) });
  await expect(chart).toContainText("3");
  await expect(chart).toContainText("陈旧");
  await expect(page.getByText("数据库中尚无采集任务。", { exact: true })).toHaveCount(0);
});

test("总览洪峰下结果分布持续渲染，冻结不阻塞接收，恢复展示最新", async ({ page, request }) => {
  await request.get("http://127.0.0.1:4311/control/reset");
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await request.get("http://127.0.0.1:4311/control/replay?count=100");
  try {
    // 结果汇总经 useDisplayed 冻结；description 的缓冲计数不经冻结，断言只覆盖图表区
    const chart = page
      .locator("[data-slot='card']", { has: page.getByText(/当前窗口保留最近/) })
      .locator("[data-slot='results-chart']");
    await expect(chart).toContainText("正常完成");
    await page.getByRole("button", { name: "冻结显示", exact: true }).click();
    const frozen = await chart.innerText();
    const before = await (await request.get("http://127.0.0.1:4311/control/status")).json() as { latest: number };
    await expect.poll(async () => {
      const state = await (await request.get("http://127.0.0.1:4311/control/status")).json() as { latest: number };
      return state.latest;
    }).toBeGreaterThan(before.latest + 500);
    expect(await chart.innerText()).toBe(frozen);
    await page.getByRole("button", { name: "恢复实时显示", exact: true }).click();
    await expect.poll(async () => chart.innerText()).not.toBe(frozen);
  } finally {
    await request.get("http://127.0.0.1:4311/control/replay-stop");
  }
});
