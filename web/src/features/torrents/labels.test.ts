import { describe, expect, it } from "vitest";
import {
  fileKindLabel,
  formatLabel,
  semanticDescription,
  semanticLabel,
  semanticTone,
} from "./labels";

describe("种子枚举标签", () => {
  it("格式中文映射，未知值回退为原字符串", () => {
    expect(formatLabel("unknown")).toBe("未知");
    expect(formatLabel("v1")).toBe("v1");
    expect(formatLabel("v2")).toBe("v2");
    expect(formatLabel("hybrid")).toBe("混合");
    expect(formatLabel("future")).toBe("future");
  });

  it("语义状态中文映射与色阶，未知值回退原字符串与 neutral", () => {
    expect(semanticLabel("pending")).toBe("待校验");
    expect(semanticLabel("valid")).toBe("有效");
    expect(semanticLabel("invalid")).toBe("无效");
    expect(semanticLabel("unsupported")).toBe("不支持");
    expect(semanticLabel("other")).toBe("other");
    expect(semanticTone("pending")).toBe("neutral");
    expect(semanticTone("valid")).toBe("success");
    expect(semanticTone("invalid")).toBe("danger");
    expect(semanticTone("unsupported")).toBe("warning");
    expect(semanticTone("other")).toBe("neutral");
  });

  it("语义状态异常的兜底说明，未知状态给出通用文案", () => {
    expect(semanticDescription("pending")).toMatch(/尚未完成语义校验/);
    expect(semanticDescription("invalid")).toMatch(/未通过语义校验/);
    expect(semanticDescription("unsupported")).toMatch(/暂不支持完整解析/);
    expect(semanticDescription("future")).toMatch(/未知/);
  });

  it("文件类型标签：普通文件不产生徽标", () => {
    expect(fileKindLabel("file")).toBeNull();
    expect(fileKindLabel("padding")).toBe("填充");
    expect(fileKindLabel("symlink")).toBe("符号链接");
    expect(fileKindLabel("future")).toBe("future");
  });
});
