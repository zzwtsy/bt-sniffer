import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { AlertCircle, Database, Search, TriangleAlert } from "lucide-react";
import { useEffect } from "react";
import { PageTitle, Panel } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";
import { Skeleton } from "@/components/ui/skeleton";
import { CATALOG_PAGE_SIZE, catalogOptions } from "@/lib/api/torrents";
import { pageWindow } from "./pagination";
import { ResultsTable } from "./results-table";
import { classifyTorrentInput } from "./search";
import { SearchBar } from "./search-bar";

interface SearchState { q?: string; page?: number }

export function TorrentsPage({ search }: { search: SearchState }) {
  const navigate = useNavigate();
  const normalized = search.q?.trim() ?? "";
  const normalizedInput = classifyTorrentInput(normalized);
  const valid = normalizedInput.kind === "empty" || normalizedInput.kind === "query";
  const page = search.page ?? 1;
  const query = useQuery({
    ...catalogOptions(normalized || undefined, page),
    enabled: valid,
    refetchInterval: current => current.state.data?.index.complete === false ? 5000 : false,
  });
  const index = query.data?.index;
  const total = query.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / CATALOG_PAGE_SIZE));

  // 页越界（旧书签、采集后总页数收缩）时回到末页；空查询结果（total=0）不算越界。
  useEffect(() => {
    const data = query.data;
    if (data == null || data.items.length > 0 || data.total === 0)
      return;
    if (page > totalPages)
      void navigate({ to: "/torrents", search: { q: normalized || undefined, page: totalPages }, replace: true });
  }, [query.data, page, totalPages, normalized, navigate]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageTitle
        eyebrow="LOCAL TORRENT CATALOG"
        title="种子查询"
        description="查询本地已保存的 v1 metadata；不会联网下载，也不提供 .torrent 导出。"
      />
      <SearchBar q={search.q} />
      <Panel
        title={normalized === "" ? "最近采集" : `“${normalized}” 的结果`}
        description="结果按采集时间倒序排列。"
        className="mb-0 flex min-h-80 flex-1 flex-col"
        contentClassName="flex min-h-0 flex-1 flex-col px-0"
      >
        {index != null && (!index.complete || !index.search_complete) && (
          <p className="mx-4 mb-3 flex shrink-0 items-center gap-1.5 rounded-lg bg-status-warning-bg px-3 py-2 text-xs text-status-warning">
            <TriangleAlert size={14} aria-hidden="true" className="shrink-0" />
            {!index.complete
              ? `已索引 ${index.indexed}/${index.total} · 结果暂不完整`
              : "部分目录的文件路径未完整纳入子串索引，按路径搜索结果可能不完整。"}
          </p>
        )}
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
          <div className="max-h-full shrink-0 overflow-y-auto">
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
          </div>
          {valid && query.data != null && query.data.items.length > 0 && (
            <ResultsTable items={query.data.items} q={normalized} from={page} />
          )}
        </div>
        {valid && query.data != null && (
          <CatalogPagination q={normalized} page={page} total={total} totalPages={totalPages} />
        )}
      </Panel>
    </div>
  );
}

/** 底部分页条：页码都是可分享链接；边界页渲染为无 href 的禁用按钮，条高保持稳定。 */
function CatalogPagination({ q, page, total, totalPages }: {
  q: string;
  page: number;
  total: number;
  totalPages: number;
}) {
  const target = (targetPage: number) => ({ q: q === "" ? undefined : q, page: targetPage });
  return (
    <div className="flex shrink-0 items-center justify-between gap-3 border-t px-4 py-3">
      <p className="text-xs text-muted-foreground tabular-nums">{`共 ${total} 条`}</p>
      <Pagination className="mx-0 w-auto justify-end">
        <PaginationContent>
          <PaginationItem>
            {page > 1
              ? <PaginationPrevious text="上一页" render={<Link to="/torrents" search={target(page - 1)} aria-label="上一页" />} />
              : <PaginationPrevious text="上一页" render={<button type="button" disabled aria-label="上一页" />} />}
          </PaginationItem>
          <PaginationItem className="hidden max-[560px]:block">
            <span className="px-2 text-xs text-muted-foreground tabular-nums">{`第 ${page}/${totalPages} 页`}</span>
          </PaginationItem>
          {pageWindow(page, totalPages).map((entry, index, entries) => (
            // 省略号以其后一页的页码作 key：窗口中每个间隙后都紧跟唯一页码。
            <PaginationItem key={entry ?? `gap-${entries[index + 1]}`} className="max-[560px]:hidden">
              {entry == null
                ? <PaginationEllipsis />
                : (
                    <PaginationLink
                      isActive={entry === page}
                      render={<Link to="/torrents" search={target(entry)} aria-label={`第 ${entry} 页`} />}
                    >
                      {entry}
                    </PaginationLink>
                  )}
            </PaginationItem>
          ))}
          <PaginationItem>
            {page < totalPages
              ? <PaginationNext text="下一页" render={<Link to="/torrents" search={target(page + 1)} aria-label="下一页" />} />
              : <PaginationNext text="下一页" render={<button type="button" disabled aria-label="下一页" />} />}
          </PaginationItem>
        </PaginationContent>
      </Pagination>
    </div>
  );
}
