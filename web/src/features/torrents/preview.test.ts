import { describe, expect, it } from "vitest";
import { parsePreview, previewApiUrl } from "./preview";

describe("预览图查询", () => {
  it("构造 whatslink 接口地址并对磁力链接编码", () => {
    const hash = "a".repeat(40);
    expect(previewApiUrl(hash)).toBe(
      `https://whatslink.info/api/v1/link?url=${encodeURIComponent(`magnet:?xt=urn:btih:${hash}`)}`,
    );
    expect(previewApiUrl(hash)).not.toContain("magnet:");
  });

  it("解析截图列表并归一化为地址数组", () => {
    const parsed = parsePreview({
      error: "",
      screenshots: [
        { screenshot: "https://img.example/1.jpg" },
        { screenshot: "https://img.example/2.jpg" },
      ],
    });
    expect(parsed).toEqual({
      screenshots: ["https://img.example/1.jpg", "https://img.example/2.jpg"],
    });
  });

  it("未收录时 screenshots 为 null，归一为空数组", () => {
    expect(parsePreview({ error: "", screenshots: null })).toEqual({ screenshots: [] });
  });

  it("error 非空时抛出该错误原文", () => {
    expect(() => parsePreview({ error: "rate limited", screenshots: null })).toThrow("rate limited");
  });

  it("拒绝缺失字段与非法截图地址", () => {
    expect(() => parsePreview({ screenshots: [] })).toThrow();
    expect(() => parsePreview({ error: "", screenshots: [{ screenshot: "not-a-url" }] })).toThrow();
  });
});
