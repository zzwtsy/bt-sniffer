import { queryString } from "@/lib/api/client";
import { useHistory } from "@/lib/api/queries";
import { usePageSearch } from "@/lib/api/search";
import { Pager, Panel, QueryState } from "./common";
import { EventTable } from "./event-table";

export function HistoryPanel({
  endpoint = "/events",
  hash,
  object,
  kind,
  title = "事件历史",
}: {
  endpoint?: string;
  hash?: string;
  object?: string;
  kind?: string;
  title?: string;
}) {
  const page = usePageSearch();
  const query = useHistory(
    `${endpoint}${queryString({ after: page.search.after, limit: page.limit, hash, object, kind })}`,
  );
  const more
    = (query.page.next != null && query.page.next !== "")
      && query.page.window
      && BigInt(query.page.next) < BigInt(query.page.window.latest)
      ? query.page.next
      : null;
  return (
    <Panel
      title={title}
      description="按需加载；未载入的前后事件不参与阶段结论。"
    >
      <QueryState
        loading={query.isPending}
        error={query.error}
        hasData={!!query.data}
        retry={() => {
          void query.refetch();
        }}
      />
      {query.page.completeness === "partial" && (
        <div className="notice">后端历史部分保留，较早过程可能已被淘汰。</div>
      )}
      {query.missing && (
        <div className="notice">
          本页部分事件已从浏览器缓存淘汰，请重新查询。
        </div>
      )}
      <EventTable events={query.page.events} />
      <Pager
        next={more}
        onNext={page.next}
        onPrevious={page.previous}
        onFirst={page.first}
        canPrevious={(page.search.trail?.length ?? 0) > 0}
        hasCursor={page.search.after !== undefined}
        limit={page.limit}
        onLimit={limit =>
          page.change({ limit, after: undefined, trail: undefined })}
      />
    </Panel>
  );
}
