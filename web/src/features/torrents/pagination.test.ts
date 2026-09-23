import { expect, it } from "vitest";
import { pageWindow } from "./pagination";

it("不足 8 页时全部展示，不含省略号", () => {
  expect(pageWindow(1, 1)).toEqual([1]);
  expect(pageWindow(4, 7)).toEqual([1, 2, 3, 4, 5, 6, 7]);
});

it("超过 7 页时保留首页末页与当前页前后各一页", () => {
  expect(pageWindow(1, 10)).toEqual([1, 2, null, 10]);
  expect(pageWindow(2, 10)).toEqual([1, 2, 3, null, 10]);
  expect(pageWindow(5, 10)).toEqual([1, null, 4, 5, 6, null, 10]);
  expect(pageWindow(9, 10)).toEqual([1, null, 8, 9, 10]);
  expect(pageWindow(10, 10)).toEqual([1, null, 9, 10]);
});

it("当前页越界时先收敛再展开窗口", () => {
  expect(pageWindow(0, 10)).toEqual([1, 2, null, 10]);
  expect(pageWindow(99, 10)).toEqual([1, null, 9, 10]);
  expect(pageWindow(1, 0)).toEqual([]);
});
