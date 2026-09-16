import { HashLink, Status } from "@/components/observation/common";
import { Records } from "@/components/observation/records";
import {
  NativeSelect,
  NativeSelectOption,
} from "@/components/ui/native-select";
import { usePageSearch } from "@/lib/api/search";
import { useDisplayed, useMonitor } from "@/lib/observation/context";
import { string } from "@/lib/observation/contracts";
import { count, label, time } from "@/lib/observation/format";

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
        <div className="filter-bar">
          <label>
            任务状态
            <NativeSelect
              value={page.search.state ?? ""}
              onChange={e =>
                page.change({
                  state:
                    (e.target.value as typeof page.search.state) || undefined,
                  after: undefined,
                  trail: undefined,
                })}
            >
              <NativeSelectOption value="">全部状态</NativeSelectOption>
              {["pending", "running", "retry_wait", "dormant", "succeeded"].map(
                s => (
                  <NativeSelectOption key={s} value={s}>
                    {label(s)}
                  </NativeSelectOption>
                ),
              )}
            </NativeSelect>
          </label>
          <span className="muted">
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
