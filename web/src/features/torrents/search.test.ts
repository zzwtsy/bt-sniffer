import { describe, expect, it } from "vitest";
import { bytes, magnetLink, relativeTime } from "./format";
import { classifyTorrentInput, splitByQuery } from "./search";
import { buildFileTree } from "./tree";

describe("种子查询输入", () => {
  it("区分空查询、字段错误、字面搜索和完整 hash", () => {
    expect(classifyTorrentInput("  ")).toEqual({ kind: "empty" });
    expect(classifyTorrentInput("ab").kind).toBe("error");
    expect(classifyTorrentInput("中文搜索")).toEqual({ kind: "query", value: "中文搜索" });
    expect(classifyTorrentInput("A".repeat(64))).toEqual({ kind: "hash", value: "a".repeat(64) });
    expect(classifyTorrentInput("A".repeat(40))).toEqual({ kind: "hash", value: "a".repeat(40) });
    expect(classifyTorrentInput("😀".repeat(200)).kind).toBe("query");
    expect(classifyTorrentInput("😀".repeat(201)).kind).toBe("error");
  });

  it("使用 BigInt 格式化超出 JavaScript 安全整数的十进制大小", () => {
    expect(bytes("42")).toBe("42 B");
    expect(bytes("9007199254740993")).toMatch(/ PiB$/);
    expect(bytes(null)).toBe("—");
  });

  it("磁力链接补全 xt 前缀并按需编码名称", () => {
    expect(magnetLink("b".repeat(64))).toBe(`magnet:?xt=urn:btmh:1220${"b".repeat(64)}`);
    const hash = "a".repeat(40);
    expect(magnetLink(hash)).toBe(`magnet:?xt=urn:btih:${hash}`);
    expect(magnetLink(hash, null)).toBe(`magnet:?xt=urn:btih:${hash}`);
    expect(magnetLink(hash, "Fixture Torrent")).toBe(`magnet:?xt=urn:btih:${hash}&dn=Fixture%20Torrent`);
    expect(magnetLink(hash, "a&b")).toBe(`magnet:?xt=urn:btih:${hash}&dn=a%26b`);
  });
});

describe("命中高亮切分", () => {
  it("ascii 不区分大小写并支持多次命中", () => {
    expect(splitByQuery("Movie.mkv 与 movie 备份", "movie")).toEqual([
      { text: "Movie", hit: true },
      { text: ".mkv 与 ", hit: false },
      { text: "movie", hit: true },
      { text: " 备份", hit: false },
    ]);
  });

  it("cjk 精确子串与无命中、空查询", () => {
    expect(splitByQuery(" Fixture 种子 合集", "种子")).toEqual([
      { text: " Fixture ", hit: false },
      { text: "种子", hit: true },
      { text: " 合集", hit: false },
    ]);
    expect(splitByQuery("Fixture", "xyz")).toEqual([{ text: "Fixture", hit: false }]);
    expect(splitByQuery("Fixture", "")).toEqual([{ text: "Fixture", hit: false }]);
  });

  it("转义正则元字符，按字面匹配", () => {
    expect(splitByQuery("a.b aXb", "a.b")).toEqual([
      { text: "a.b", hit: true },
      { text: " aXb", hit: false },
    ]);
  });
});

describe("目录树聚合", () => {
  const file = (index: number, path: string) => ({
    index,
    path,
    kind: "file" as const,
    hidden: false,
    executable: false,
    symlink_path: null,
    sha1: null,
    path_truncated: false,
    encoding_lossy: false,
    length: "1",
  });

  it("嵌套目录聚合子项计数并保持顺序", () => {
    const roots = buildFileTree([
      file(0, "a/b/c.txt"),
      file(1, "a/d.txt"),
      file(2, "e.txt"),
    ]);
    expect(roots.map(node => node.name)).toEqual(["a", "e.txt"]);
    const dir = roots[0];
    expect(dir.count).toBe(2);
    expect(dir.depth).toBe(0);
    expect(dir.key).toBe("a");
    expect(dir.children.map(node => node.name)).toEqual(["b", "d.txt"]);
    expect(dir.children[0].count).toBe(1);
    expect(dir.children[0].depth).toBe(1);
    expect(dir.children[1].file?.path).toBe("a/d.txt");
    expect(dir.children[1].key).toBe("file:1");
    expect(roots[1].file?.index).toBe(2);
  });

  it("根级文件与空输入", () => {
    const roots = buildFileTree([file(0, "readme.txt")]);
    expect(roots).toHaveLength(1);
    expect(roots[0].file?.path).toBe("readme.txt");
    expect(buildFileTree([])).toEqual([]);
  });

  it("padding 文件（path 为 null）置于根部，name 为空并以合成键标识", () => {
    const padding = { ...file(0, "ignored"), path: null, kind: "padding" as const };
    const roots = buildFileTree([padding, file(1, "b.txt")]);
    expect(roots.map(node => node.name)).toEqual(["", "b.txt"]);
    expect(roots[0].key).toBe("file:0");
    expect(roots[0].depth).toBe(0);
    expect(roots[1].key).toBe("file:1");
  });
});

describe("相对时间", () => {
  const now = new Date("2026-09-23T12:00:00Z").getTime();

  it("覆盖秒、分、时、天与绝对日期边界", () => {
    expect(relativeTime(now - 5_000, now)).toBe("刚刚");
    expect(relativeTime(now - 30_000, now)).toBe("30秒钟前");
    expect(relativeTime(now - 5 * 60_000, now)).toBe("5分钟前");
    expect(relativeTime(now - 3 * 3_600_000, now)).toBe("3小时前");
    expect(relativeTime(now - 2 * 86_400_000, now)).toBe("前天");
    expect(relativeTime(now - 40 * 86_400_000, now)).toMatch(/2026/);
  });
});
