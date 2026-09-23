import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { AlertCircle, Database, Search, TriangleAlert } from "lucide-react";
import { PageTitle, Panel } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { Pagination, PaginationContent, PaginationItem, PaginationNext } from "@/components/ui/pagination";
import { Skeleton } from "@/components/ui/skeleton";
import { catalogOptions } from "@/lib/api/torrents";
import { ResultsTable } from "./results-table";
import { classifyTorrentInput } from "./search";
import { SearchBar } from "./search-bar";

interface SearchState { q?: string; after?: string }

export function TorrentsPage({ search }: { search: SearchState }) {
  const normalized = search.q?.trim() ?? "";
  const normalizedInput = classifyTorrentInput(normalized);
  const valid = normalizedInput.kind === "empty" || normalizedInput.kind === "query";
  const query = useQuery({
    ...catalogOptions(normalized || undefined, search.after),
    enabled: valid,
    refetchInterval: current => current.state.data?.index.complete === false ? 5000 : false,
  });
  const index = query.data?.index;

  return (
    <>
      <PageTitle
        eyebrow="LOCAL TORRENT CATALOG"
        title="种子查询"
        description="查询本地已保存的 v1 metadata；不会联网下载，也不提供 .torrent 导出。"
      />
      <SearchBar q={search.q} />
      <Panel
        title={normalized === "" ? "最近采集" : `“${normalized}” 的结果`}
        description="结果按采集时间倒序排列。"
        contentClassName="px-0"
      >
        {index != null && (!index.complete || !index.search_complete) && (
          <p className="mx-4 mb-3 flex items-center gap-1.5 rounded-lg bg-status-warning-bg px-3 py-2 text-xs text-status-warning">
            <TriangleAlert size={14} aria-hidden="true" className="shrink-0" />
            {!index.complete
              ? `已索引 ${index.indexed}/${index.total} · 结果暂不完整`
              : "部分目录的文件路径未完整纳入子串索引，按路径搜索结果可能不完整。"}
          </p>
        )}
        {!valid && (
          <Alert variant="destructive" className="mx-4 w-auto">
            <AlertCircle />
            <AlertTitle>搜索条件无效</AlertTitle>
            <AlertDescription>
              {normalizedInput.kind === "error" ? normalizedInput.message : "完整 hash 对应单条记录，在搜索框输入完整 hash 会直接打开详情。"}
            </AlertDescription>
          </Alert>
        )}
        {valid && query.isPending && (
          <div className="flex flex-col gap-3 px-4">
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
            <Skeleton className="h-10 w-full" />
          </div>
        )}
        {valid && query.error != null && (
          <Alert variant="destructive" className="mx-4 w-auto">
            <AlertCircle />
            <AlertTitle>目录暂时无法读取</AlertTitle>
            <AlertDescription>{query.error.message}</AlertDescription>
            <AlertAction>
              <Button variant="outline" size="sm" onClick={() => void query.refetch()}>重试</Button>
            </AlertAction>
          </Alert>
        )}
        {valid && query.data?.items.length === 0 && (
          <Empty className="m-4 border">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                {normalized === "" ? <Database /> : <Search />}
              </EmptyMedia>
              <EmptyTitle>{normalized === "" ? "尚未保存可展示的 metadata。" : "没有匹配的本地种子"}</EmptyTitle>
              <EmptyDescription>
                {normalized === ""
                  ? "采集到新的 metadata 后会自动出现在这里。"
                  : index?.search_complete === false
                    ? "部分目录的路径索引不完整，此空结果可能不完整。"
                    : "换个关键词试试；匹配按名称与完整文件路径的字面子串进行，ASCII 不区分大小写。"}
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        )}
        {valid && query.data != null && query.data.items.length > 0 && (
          <ResultsTable items={query.data.items} q={normalized} from={search.after} />
        )}
        {valid && query.data?.next != null && (
          <Pagination className="mt-4 border-t pt-4">
            <PaginationContent>
              <PaginationItem>
                <PaginationNext
                  text="下一页"
                  render={(
                    <Link
                      to="/torrents"
                      search={{ q: normalized || undefined, after: query.data.next }}
                      aria-label="下一页"
                    />
                  )}
                />
              </PaginationItem>
            </PaginationContent>
          </Pagination>
        )}
      </Panel>
    </>
  );
}
