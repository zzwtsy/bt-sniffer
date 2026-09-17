import { render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import { Freshness, Metric } from "./common";

it("hint 传入时渲染口径说明入口，未传入时不渲染", () => {
  const view = render(
    <Metric title="活跃任务" value="35" detail="当前在途的查找与下载任务" hint="来自 runtime.active 快照" icon={<svg />} />,
  );
  expect(screen.getByRole("button", { name: "口径说明" })).toBeVisible();
  view.rerender(
    <Metric title="活跃任务" value="35" detail="当前在途的查找与下载任务" icon={<svg />} />,
  );
  expect(screen.queryByRole("button", { name: "口径说明" })).not.toBeInTheDocument();
});

it("source 前缀替代默认观察时间文案，陈旧时追加标注", () => {
  const view = render(<Freshness at={undefined} source="采集快照" />);
  expect(screen.getByText("采集快照 · 未知")).toBeVisible();
  view.rerender(<Freshness at={undefined} source="数据库统计" stale />);
  expect(screen.getByText("数据库统计 · 陈旧 · 未知")).toBeVisible();
  view.rerender(<Freshness at={undefined} />);
  expect(screen.getByText("观察时间 · 未知")).toBeVisible();
});
