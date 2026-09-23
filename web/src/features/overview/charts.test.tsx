import type { ReactNode } from "react";
import type { DurationEntry, Funnel, JobStates, ResultsSummary } from "./model";
import { render } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { DurationsChart, FunnelChart, JobStatesChart, ResultsChart } from "./charts";
import { quantileLabel } from "./model";

const { container } = vi.hoisted(() => ({ container: vi.fn() }));
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
vi.mock("./model", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./model")>();
  return { ...actual, quantileLabel: vi.fn(actual.quantileLabel) };
});
it("无关快照及陈旧元数据不重绘图形，计数、桶边界及可用性变化仍更新", () => {
  container.mockClear();
  const quantileCalls = vi.mocked(quantileLabel);
  quantileCalls.mockClear();
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
  expect(container).toHaveBeenCalledTimes(1);
  expect(quantileCalls).toHaveBeenCalledTimes(3);
  jobs = { ...structuredClone(jobs), stale: true, observedAt: 2 };
  entries = structuredClone(entries);
  funnel = structuredClone(funnel);
  view.rerender(ui());
  expect(container).toHaveBeenCalledTimes(1);
  expect(quantileCalls).toHaveBeenCalledTimes(3);
  jobs = { ...jobs, states: [{ state: "running", count: 2 }] };
  entries = [{ ...entries[0], p99: { upperBoundMs: null, exceedsMs: 40 } }];
  funnel = { levels: [{ id: "commit", title: "提交", count: 2 }] };
  view.rerender(ui());
  expect(container).toHaveBeenCalledTimes(2);
  expect(quantileCalls).toHaveBeenCalledTimes(6);
  jobs = undefined;
  view.rerender(ui());
  expect(view.getByText("数据库统计尚不可用。")).toBeInTheDocument();
});

it("阶段耗时按固定桶槽位绘制分位点，溢出、未知桶值与空样本分别标记", () => {
  const entries: DurationEntry[] = [
    {
      timing: "lookup",
      count: 12,
      p50: { upperBoundMs: 100, exceedsMs: null },
      p95: { upperBoundMs: 500, exceedsMs: null },
      p99: { upperBoundMs: null, exceedsMs: 180000 },
    },
    {
      timing: "tcp_wait",
      count: 3,
      p50: { upperBoundMs: null, exceedsMs: null },
      p95: { upperBoundMs: 250, exceedsMs: null },
      p99: { upperBoundMs: 2000, exceedsMs: null },
    },
  ];
  const view = render(<DurationsChart entries={entries} />);
  expect(view.getByText("peer 查找")).toBeInTheDocument();
  expect(view.getByText("样本 12")).toBeInTheDocument();
  expect(view.getByText("溢出")).toBeInTheDocument();
  const p50 = view.getByTitle("peer 查找 p50：≤100 ms，样本 12");
  expect(p50.dataset.state).toBe("bucket");
  expect(p50.style.left).toBe("32.14%");
  const p99 = view.getByTitle("peer 查找 p99：>180.00 s，样本 12");
  expect(p99.dataset.state).toBe("overflow");
  expect(p99.style.left).toBe("96.43%");
  const unknown = view.getByTitle("TCP 许可等待 p95：≤250 ms，样本 3");
  expect(unknown.dataset.state).toBe("unknown");
  expect(unknown.style.left).toBe("39.29%");
  expect(view.queryByTitle(/TCP 许可等待 p50/)).not.toBeInTheDocument();
  view.rerender(<DurationsChart entries={[]} />);
  expect(view.getByText("采集未启用或指标尚不可用。")).toBeInTheDocument();
});

it("漏斗渲染各级计数与级间转化率，上级为零时标注样本不足，空缓冲有明确文案", () => {
  const view = render(<FunnelChart data={undefined} />);
  expect(view.getByText(/窗口内尚无记录/)).toBeInTheDocument();
  view.rerender(
    <FunnelChart
      data={{
        levels: [
          { id: "discovery", title: "发现", count: 1000 },
          { id: "admission", title: "接纳", count: 500 },
          { id: "claim", title: "领取", count: 0 },
          { id: "commit", title: "提交", count: 0 },
        ],
      }}
    />,
  );
  expect(view.getByText("发现")).toBeInTheDocument();
  expect(view.getByText("1,000")).toBeInTheDocument();
  expect(view.getByLabelText("发现到接纳的转化率 50.0%")).toBeInTheDocument();
  expect(view.getByLabelText("接纳到领取的转化率 0.0%")).toBeInTheDocument();
  expect(view.getByLabelText("领取到提交的转化率 样本不足")).toBeInTheDocument();
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
