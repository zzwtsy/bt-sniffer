import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { describe, expect, it } from "vitest";
import { event } from "../../../tests/fixtures";
import { EventBuffer } from "./buffer";

describe("事件缓存", () => {
  it("按序淘汰乱序历史，同步删除索引，重复批次不增加内存", () => {
    const buffer = new EventBuffer({ count: 2, bytes: 10000, age: 100 });
    buffer.add([event(3), event(1), event(2)], 0);
    expect(buffer.select().map(e => e.sequence)).toEqual(["2", "3"]);
    const bytes = buffer.bytes;
    buffer.add([event(2)], 1);
    expect(buffer.bytes).toBe(bytes);
    expect(buffer.indexEntries).toBe(4);
    buffer.prune(101);
    expect(buffer.size).toBe(0);
    expect(buffer.bytes).toBe(0);
    expect(buffer.indexEntries).toBe(0);
  });
  it("字节预算与后端最早序号均可独立触发淘汰", () => {
    const buffer = new EventBuffer({ count: 100, bytes: 300, age: 1000 });
    buffer.add([event(1), event(2)], 0);
    expect(buffer.size).toBeLessThan(2);
    expect(buffer.bytes).toBeLessThanOrEqual(300);
    buffer.prune(1, "3");
    expect(buffer.size).toBe(0);
  });
  it("处理 51,200 条事件时记录和关联索引始终受限", () => {
    const buffer = new EventBuffer();
    const start = performance.now();
    for (let offset = 0; offset < 51_200; offset += 100) {
      buffer.add(
        Array.from({ length: Math.min(100, 51_200 - offset) }, (_, i) =>
          event(offset + i + 1)),
        0,
      );
      expect(buffer.size).toBeLessThanOrEqual(5000);
      expect(buffer.bytes).toBeLessThanOrEqual(8 * 1024 * 1024);
      expect(buffer.indexEntries).toBeLessThanOrEqual(10_000);
    }
    expect(buffer.evicted).toBe(46_200);
    expect(buffer.get(["1"])).toEqual([]);
    const evidence = {
      workload: 51_200,
      elapsed_ms: performance.now() - start,
      retained: buffer.size,
      encoded_bytes: buffer.bytes,
      index_entries: buffer.indexEntries,
    };
    const directory = path.resolve(process.cwd(), "../target/checks/frontend-load");
    mkdirSync(directory, { recursive: true });
    writeFileSync(path.join(directory, "unit.json"), JSON.stringify(evidence, null, 2));
  });
  it("字符串序号超过安全整数时仍按精确顺序处理", () => {
    const buffer = new EventBuffer();
    buffer.add([
      { ...event(1), sequence: "9007199254740993" },
      { ...event(1), sequence: "9007199254740992" },
    ]);
    expect(buffer.select().map(e => e.sequence)).toEqual([
      "9007199254740992",
      "9007199254740993",
    ]);
  });
});
