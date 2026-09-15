import { expect, it } from "vitest";
import { searchSchema } from "./search";

it("严格 hash、有限游标栈及页大小，保留大事件游标为字符串", () => {
  expect(searchSchema.parse({ hash: "A".repeat(40), after: "9007199254740993", limit: "100" })).toMatchObject({ hash: "a".repeat(40), after: "9007199254740993", limit: 100 });
  expect(searchSchema.parse({ hash: "oops", limit: 200, generation: -1, trail: Array.from({ length: 51 }).fill("0") })).toEqual({ hash: undefined, limit: undefined, generation: undefined, trail: undefined });
});
