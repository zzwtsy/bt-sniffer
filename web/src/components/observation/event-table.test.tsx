import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { eventSchema } from "@/lib/observation/contracts";
import { time } from "@/lib/observation/format";
import { EventTable } from "./event-table";

vi.mock("@/lib/observation/format", async importOriginal => ({
  ...await importOriginal<typeof import("@/lib/observation/format")>(),
  time: vi.fn((value: unknown) => `time:${String(value)}`),
}));
vi.mock("./common", () => ({
  Empty: () => <span>空</span>,
  HashLink: ({ hash }: { hash: string }) => <a href={`/hashes/${hash}`}>{hash}</a>,
  Status: ({ value }: { value: string }) => <span>{value}</span>,
}));
function event(sequence: number) {
  return eventSchema.parse({
    schema_version: 1,
    run_id: "run",
    sequence: String(sequence),
    at_ms: sequence,
    kind: "discovery",
    step: "hash_saved",
    result: "new",
    context: {},
    data: {},
    truncated: false,
  });
}
it("新增行只格式化新事件，抽屉及筛选不重绘已有行，淘汰后保留选中证据", async () => {
  vi.mocked(time).mockClear();
  const first = event(1);
  const second = event(2);
  const view = render(<EventTable events={[first]} />);
  expect(time).toHaveBeenCalledTimes(1);
  view.rerender(<EventTable events={[first, second]} />);
  expect(time).toHaveBeenCalledTimes(2);
  fireEvent.click(screen.getByRole("button", { name: "查看 #1" }));
  await screen.findByRole("dialog");
  expect(time).toHaveBeenCalledTimes(3);
  view.rerender(<EventTable events={[second]} />);
  expect(time).toHaveBeenCalledTimes(3);
  expect(screen.getByRole("dialog")).toHaveTextContent("\"sequence\": \"1\"");
  fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(time).toHaveBeenCalledTimes(3);
});
