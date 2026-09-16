import { useEffect, useRef, useState } from "react";
import { PageTitle, Panel } from "@/components/observation/common";
import { EventTable } from "@/components/observation/event-table";
import { HistoryPanel } from "@/components/observation/history";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  NativeSelect,
  NativeSelectOption,
} from "@/components/ui/native-select";
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
        className="filter-bar"
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
          change({
            hash: hash.toLowerCase() || undefined,
            object: String(data.get("object") ?? "") || undefined,
            kind: String(data.get("kind") ?? "") || undefined,
            after: undefined,
            trail: undefined,
          });
        }}
      >
        <Input
          name="hash"
          aria-label="按 hash 筛选"
          placeholder="完整 hash"
          defaultValue={search.hash}
        />
        <Input
          name="object"
          aria-label="关联对象"
          placeholder="关联对象 ID"
          maxLength={128}
          defaultValue={search.object}
        />
        <NativeSelect
          name="kind"
          aria-label="事件类别"
          defaultValue={search.kind ?? ""}
        >
          <NativeSelectOption value="">全部类别</NativeSelectOption>
          {kinds.map(k => (
            <NativeSelectOption key={k} value={k}>
              {label(k)}
            </NativeSelectOption>
          ))}
        </NativeSelect>
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
                className="live-events"
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
