import process from "node:process";
import { expect, it, vi } from "vitest";

it.each(["UTC", "Asia/Shanghai", "America/Los_Angeles"])("复用格式化器保留 %s 的完整时间语义", async (zone) => {
  const previous = process.env.TZ;
  try {
    process.env.TZ = zone;
    vi.resetModules();
    const { time, timeTick } = await import("./format");
    for (const at of ["2026-09-17T00:00:00Z", "2026-09-17T23:59:59Z", "2026-03-08T10:00:00Z"]) {
      const date = new Date(at);
      expect(time(date.getTime())).toBe(date.toLocaleString("zh-CN", { hour12: false, timeZoneName: "short" }));
      expect(timeTick(date.getTime())).toBe(date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" }));
    }
    expect(time(1e20)).toBe(new Date(1e20).toLocaleString("zh-CN"));
    expect(timeTick("invalid")).toBe("Invalid Date");
    expect(time(undefined)).toBe("未知");
    expect(time(null)).toBe("未知");
    expect(time("123")).toBe("未知");
  } finally {
    if (previous === undefined)
      delete process.env.TZ;
    else process.env.TZ = previous;
    vi.resetModules();
  }
});
