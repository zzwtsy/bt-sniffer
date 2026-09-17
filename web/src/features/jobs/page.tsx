import { HashLink, Status } from "@/components/observation/common";
import { Records } from "@/components/observation/records";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { usePageSearch } from "@/lib/api/search";
import { useDisplayed, useMonitor } from "@/lib/observation/context";
import { string } from "@/lib/observation/contracts";
import { count, label, time } from "@/lib/observation/format";

const stateOptions = [
  { value: "all", label: "全部状态" },
  ...["pending", "running", "retry_wait", "dormant", "succeeded"].map(
    state => ({ value: state, label: label(state) }),
  ),
];

export function JobsPage() {
  const page = usePageSearch();
  const monitor = useMonitor();
  const snapshot = useDisplayed("jobs:active", monitor.snapshot);
  return (
    <Records
      title="采集任务"
      eyebrow="COLLECTION"
      description="本地等待、远端失败和重试分别记录。到期时间不保证立即获得调度。"
      endpoint="/jobs"
      state={page.search.state}
      controls={(
        <div className="mb-3 flex flex-wrap items-center gap-3 py-3.5">
          <div className="flex items-center gap-2 text-xs">
            <span>任务状态</span>
            <Select
              items={stateOptions}
              value={page.search.state ?? "all"}
              onValueChange={value =>
                page.change({
                  state:
                    value === "all"
                      ? undefined
                      : (value as typeof page.search.state),
                  after: undefined,
                  trail: undefined,
                })}
            >
              <SelectTrigger aria-label="任务状态">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {stateOptions.map(option => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>
          <span className="text-xs text-muted-foreground">
            单状态按到期时间排列，全部状态按 hash 排列
          </span>
        </div>
      )}
      columns={[
        { name: "hash", cell: r => <HashLink hash={string(r.hash)} /> },
        { name: "状态", cell: r => <Status value={r.state} /> },
        {
          name: "当前阶段",
          cell: r =>
            label(
              snapshot?.runtime.active.find(
                a =>
                  (a.context as { hash?: string } | undefined)?.hash === r.hash,
              )?.step,
            ),
        },
        { name: "generation", cell: r => count(r.generation) },
        { name: "远端失败", cell: r => count(r.remote_failures) },
        { name: "到期时间", cell: r => time(r.due_at_ms) },
        { name: "最近错误", cell: r => label(r.error) },
      ]}
    />
  );
}
