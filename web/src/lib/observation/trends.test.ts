import { expect, it } from "vitest";
import { snapshot } from "../../../tests/fixtures";
import { Trends } from "./trends";

it("只对同运行的连续更新求差，断线、回退和重复时间不伪造速率", () => {
  const trends = new Trends();
  trends.add(snapshot());
  trends.add(snapshot(2000, 3));
  expect(trends.points.at(-1)?.commits).toBe(2);
  trends.add(snapshot(2000, 9));
  expect(trends.points).toHaveLength(2);
  trends.add(snapshot(3000, 1));
  expect(trends.points.at(-1)?.commits).toBeNull();
  trends.gap();
  trends.add(snapshot(4000, 2));
  expect(trends.points.at(-1)?.commits).toBeNull();
  trends.add(snapshot(5000, 4, "new"));
  expect(trends.points.at(-1)?.commits).toBeNull();
  trends.add(snapshot(12000, 10, "new"));
  expect(trends.points.at(-1)?.commits).toBeNull();
});
it("曲线最多 900 个时间点", () => {
  const trends = new Trends();
  for (let i = 0; i < 2000; i++) trends.add(snapshot(i * 1000, i));
  expect(trends.points.length).toBe(900);
});

it("图表冻结已发布数组不影响后续采样", () => {
  const trends = new Trends();
  trends.add(snapshot());
  const published = trends.points;
  Object.freeze(published);
  trends.add(snapshot(2000, 2));
  expect(published).toHaveLength(1);
  expect(trends.points).toHaveLength(2);
});

it("断线没有新快照时趋势也遵守 15 分钟窗口", () => {
  const trends = new Trends();
  trends.add(snapshot(1000, 1));
  expect(trends.prune(901001)).toBe(true);
  expect(trends.points).toEqual([]);
});
