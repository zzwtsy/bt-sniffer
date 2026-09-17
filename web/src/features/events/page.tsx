import { useEffect, useRef, useState } from "react";
import { PageTitle, Panel } from "@/components/observation/common";
import { EventTable } from "@/components/observation/event-table";
import { HistoryPanel } from "@/components/observation/history";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { usePageSearch } from "@/lib/api/search";
import { useDisplayed, useEngine, useMonitor } from "@/lib/observation/context";
import { hashPattern } from "@/lib/observation/contracts";
import { label } from "@/lib/observation/format";

const kinds = [
  "lifecycle",
  "bootstrap",
  "routing",
  "rpc",
  "sampling",
  "discovery",
  "admission",
  "job",
  "lookup",
  "peer",
  "piece",
  "validation",
  "commit",
  "retry",
  "backpressure",
];
const kindOptions = [
  { value: "all", label: "全部类别" },
  ...kinds.map(kind => ({ value: kind, label: label(kind) })),
];
export function EventsPage() {
  const { search, change } = usePageSearch();
  const engine = useEngine();
  const monitor = useMonitor();
  const events = useDisplayed(
    "events:visible",
    engine.buffer.select(search, 100),
  );
  const [follow, setFollow] = useState(true);
  const tailRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (follow && search.mode === "live")
      tailRef.current?.scrollIntoView({ block: "nearest" });
  }, [monitor.revision, follow, search.mode]);
  const [error, setError] = useState("");
  return (
    <>
      <PageTitle
        title="事件浏览"
        eyebrow="EVENT EXPLORER"
        description="结构化事件是判断依据；实时窗口与后端历史分页分开查看。"
      />
      <form
        className="mb-3 flex flex-wrap items-center gap-3 py-3.5"
        key={`${search.hash}-${search.object}-${search.kind}`}
        onSubmit={(e) => {
          e.preventDefault();
          const data = new FormData(e.currentTarget);
          const hash = String(data.get("hash") ?? "").trim();
          if (hash && !hashPattern.test(hash)) {
            setError("hash 必须为完整 40 位十六进制");
            return;
          }
          setError("");
          const kind = String(data.get("kind") ?? "all");
          change({
            hash: hash.toLowerCase() || undefined,
            object: String(data.get("object") ?? "") || undefined,
            kind: kind === "all" ? undefined : kind,
            after: undefined,
            trail: undefined,
          });
        }}
      >
        <Input
          name="hash"
          aria-label="按 hash 筛选"
          placeholder="完整 hash"
          className="min-w-45 max-w-110 flex-1"
          defaultValue={search.hash}
        />
        <Input
          name="object"
          aria-label="关联对象"
          placeholder="关联对象 ID"
          className="min-w-45 max-w-110 flex-1"
          maxLength={128}
          defaultValue={search.object}
        />
        <Select
          items={kindOptions}
          name="kind"
          defaultValue={search.kind ?? "all"}
        >
          <SelectTrigger aria-label="事件类别">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              {kindOptions.map(option => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
        <Button>应用筛选</Button>
        {error && <span role="alert">{error}</span>}
      </form>
      <Tabs
        className="mb-5"
        value={search.mode === "live" ? "live" : "history"}
        onValueChange={value =>
          change({ mode: value === "live" ? "live" : "history" })}
      >
        <TabsList>
          <TabsTrigger value="history">历史分页</TabsTrigger>
          <TabsTrigger value="live">实时跟随</TabsTrigger>
        </TabsList>
      </Tabs>
      {search.mode === "live"
        ? (
            <Panel
              title="实时窗口"
              description={`浏览器保留 ${monitor.events} 条 · 已淘汰 ${monitor.evicted} 条`}
              action={
                !follow && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setFollow(true)}
                  >
                    有新事件 / 回到最新
                  </Button>
                )
              }
            >
              <div
                className="max-h-[65vh] overflow-auto"
                onWheel={(e) => {
                  if (e.deltaY < 0)
                    setFollow(false);
                }}
                onKeyDown={(e) => {
                  if (["PageUp", "Home", "ArrowUp"].includes(e.key))
                    setFollow(false);
                }}
                tabIndex={0}
                aria-label="实时事件列表"
              >
                <EventTable events={events} />
                <div ref={tailRef} />
              </div>
            </Panel>
          )
        : (
            <HistoryPanel
              hash={search.hash}
              object={search.object}
              kind={search.kind}
            />
          )}
    </>
  );
}
