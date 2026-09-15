import { mkdir, writeFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";

const hash = "a".repeat(40);
test("深浅主题的重要状态文字保持可读对比度", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  for (const theme of ["light", "dark"]) {
    await page.getByLabel("主题", { exact: true }).selectOption(theme);
    const ratios = await page.locator("main .status").evaluateAll(elements =>
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
    await page.screenshot({
      path: `../target/checks/frontend-${theme}.png`,
      fullPage: true,
    });
  }
});

test("总览、详情、证据与所有功能页面连接真实同源接口", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "看见每一次发现" }),
  ).toBeVisible();
  for (const path of [
    "/dht",
    "/discoveries",
    "/jobs",
    "/hashes",
    "/metadata",
    "/events",
  ]) {
    await page.goto(path);
    await expect(page.locator("h1")).toBeVisible();
    await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  }
  await page.goto(`/hashes/${hash}`);
  await expect(
    page.getByRole("option", { name: /generation\s*2/ }).first(),
  ).toBeAttached();
  const trigger = page.getByRole("button", { name: /查看 #/ }).first();
  await expect(trigger).toBeVisible();
  await trigger.click();
  await expect(page.getByRole("heading", { name: "事件证据" })).toBeVisible();
  await page.getByRole("button", { name: "关闭", exact: true }).click();
  await expect(trigger).toBeFocused();
  await page.goto("/dht/0");
  await expect(page.getByText("127.0.0.1:6881").first()).toBeVisible();
  expect(errors).toEqual([]);
});

test("冻结、断线续传、运行 reset 和容量 reset", async ({ page, request }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  const metric = page
    .locator(".metric")
    .filter({ hasText: "已保存 metadata" })
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

test("窄屏、深浅主题、键盘和放大视图", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  await page.getByLabel("主题", { exact: true }).selectOption("dark");
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.getByLabel("主题", { exact: true }).selectOption("light");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "展开导航" }).click();
  await page.getByRole("navigation").getByText("采集任务").click();
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
  await page.goto("/events?mode=live");
  await expect(page.getByText("实时连接", { exact: true })).toBeVisible();
  const started = Date.now();
  await request.get("http://127.0.0.1:4311/control/load");
  await expect(page.getByText(/浏览器保留 5000 条/)).toBeVisible({
    timeout: 15000,
  });
  expect(await page.locator("tbody tr").count()).toBeLessThanOrEqual(100);
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

test("分页游标往返、generation 选择和键盘证据关闭", async ({ page }) => {
  await page.goto("/hashes");
  await expect(page.locator("tbody")).toContainText("aaaaaaaa");
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page).toHaveURL(/after=/);
  await expect(page.locator("tbody")).toContainText("bbbbbbbb");
  await page.getByRole("button", { name: "上一页" }).click();
  await expect(page.locator("tbody")).toContainText("aaaaaaaa");
  await page.locator("tbody a").first().click();
  await page
    .getByRole("combobox", { name: "领取", exact: true })
    .selectOption("1");
  await expect(page).toHaveURL(/generation=1/);
  await expect(
    page.getByRole("combobox", { name: "peer", exact: true }),
  ).toHaveValue("peer-1");
  await page
    .getByRole("combobox", { name: "领取", exact: true })
    .selectOption("2");
  await expect(
    page.getByRole("combobox", { name: "peer", exact: true }),
  ).toHaveValue("peer-2");
  await page
    .getByRole("button", { name: /查看 #/ })
    .first()
    .click();
  await page.keyboard.press("Escape");
  await expect(
    page.getByRole("heading", { name: "事件证据" }),
  ).not.toBeVisible();
});
