import type { ReactNode } from "react";
import type { DurationEntry, Funnel, JobStates, ResultsSummary } from "./model";
import { render } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { DurationsChart, FunnelChart, JobStatesChart, ResultsChart } from "./charts";

const { container } = vi.hoisted(() => ({ container: vi.fn() }));
vi.mock("@tanstack/react-router", () => ({ useNavigate: () => vi.fn() }));
vi.mock("@/components/ui/chart", () => ({
  ChartContainer: ({ children }: { children: ReactNode }) => {
    container();
    return <div>{children}</div>;
  },
  ChartTooltip: () => null,
  ChartTooltipContent: () => null,
}));
vi.mock("recharts", () => {
  const Node = ({ children }: { children?: ReactNode }) => <div>{children}</div>;
  return Object.fromEntries(["Bar", "BarChart", "CartesianGrid", "Cell", "Line", "LineChart", "Pie", "PieChart", "XAxis", "YAxis"].map(name => [name, Node]));
});
it("无关快照及陈旧元数据不重绘图形，计数、桶边界及可用性变化仍更新", () => {
  container.mockClear();
  let jobs: JobStates | undefined = { states: [{ state: "running", count: 1 }], observedAt: 1, stale: false };
  let entries: DurationEntry[] = [{ timing: "lookup", count: 1, p50: { upperBoundMs: 10, exceedsMs: null }, p95: { upperBoundMs: 20, exceedsMs: null }, p99: { upperBoundMs: null, exceedsMs: 30 } }];
  let funnel: Funnel = { levels: [{ id: "commit", title: "提交", count: 1 }] };
  const ui = () => (
    <>
      <JobStatesChart data={jobs} />
      <DurationsChart entries={entries} />
      <FunnelChart data={funnel} />
    </>
  );
  const view = render(ui());
  expect(container).toHaveBeenCalledTimes(3);
  jobs = { ...structuredClone(jobs), stale: true, observedAt: 2 };
  entries = structuredClone(entries);
  funnel = structuredClone(funnel);
  view.rerender(ui());
  expect(container).toHaveBeenCalledTimes(3);
  jobs = { ...jobs, states: [{ state: "running", count: 2 }] };
  entries = [{ ...entries[0], p99: { upperBoundMs: null, exceedsMs: 40 } }];
  funnel = { levels: [{ id: "commit", title: "提交", count: 2 }] };
  view.rerender(ui());
  expect(container).toHaveBeenCalledTimes(6);
  jobs = undefined;
  view.rerender(ui());
  expect(view.getByText("数据库统计尚不可用。")).toBeInTheDocument();
});

it("结果汇总渲染三档与失败原因，无失败与空数据有明确文案", () => {
  let summary: ResultsSummary | undefined = {
    total: 10,
    failed: 2,
    succeeded: 5,
    other: 3,
    failures: [{ result: "timeout", count: 2 }],
  };
  const view = render(<ResultsChart data={summary} />);
  expect(view.getByText("失败")).toBeInTheDocument();
  expect(view.getByText("正常完成")).toBeInTheDocument();
  expect(view.getByText("进行中与其他")).toBeInTheDocument();
  expect(view.getByText("超时")).toBeInTheDocument();
  summary = { total: 4, failed: 0, succeeded: 4, other: 0, failures: [] };
  view.rerender(<ResultsChart data={summary} />);
  expect(view.getByText("窗口内无失败事件。")).toBeInTheDocument();
  view.rerender(<ResultsChart data={undefined} />);
  expect(view.getByText(/窗口内尚无匹配事件/)).toBeInTheDocument();
});
