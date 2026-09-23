import type { TreeNode } from "./tree";
import { useInfiniteQuery } from "@tanstack/react-query";
import { AlertCircle, ChevronDown, ChevronRight, File, Folder } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Empty } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { filesAllOptions } from "@/lib/api/torrents";
import { bytes } from "./format";
import { buildFileTree } from "./tree";

/** 全量拉取上限：50 页 × 100 条 = 5000 个文件，防止超大 metadata 拖垮页面。 */
const MAX_PAGES = 50;
const PAGE_SIZE = 100;

export function FileTree({ hash }: { hash: string }) {
  const files = useInfiniteQuery(filesAllOptions(hash));
  const pages = files.data?.pages ?? [];
  const loaded = pages.reduce((sum, page) => sum + page.items.length, 0);
  const capped = files.hasNextPage && pages.length >= MAX_PAGES;
  const loadingMore = files.hasNextPage && pages.length < MAX_PAGES;
  const [toggled, setToggled] = useState<Record<string, boolean>>({});

  // 顺序拉取后续页；受请求调度限速，5000 条约需十几秒。
  useEffect(() => {
    if (files.hasNextPage && !files.isFetchingNextPage && !files.isFetchNextPageError && pages.length < MAX_PAGES)
      void files.fetchNextPage();
  });

  const roots = useMemo(() => buildFileTree((files.data?.pages ?? []).flatMap(page => page.items)), [files.data]);

  function toggle(node: TreeNode) {
    const expanded = toggled[node.path] ?? node.depth < 2;
    setToggled({ ...toggled, [node.path]: !expanded });
  }

  return (
    <div data-slot="file-tree" className="flex min-h-0 flex-1 flex-col">
      {files.isPending && (
        <div className="flex flex-col gap-3 px-2">
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
        </div>
      )}
      {files.error != null && (
        <Alert variant="destructive" className="mx-2 w-auto">
          <AlertCircle />
          <AlertTitle>文件清单无法读取</AlertTitle>
          <AlertDescription>{files.error.message}</AlertDescription>
          <AlertAction>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void (files.isFetchNextPageError ? files.fetchNextPage() : files.refetch())}
            >
              重试
            </Button>
          </AlertAction>
        </Alert>
      )}
      {!files.isPending && files.error == null && loaded === 0 && !files.hasNextPage && (
        <Empty>该 metadata 没有可展示的文件记录。</Empty>
      )}
      {roots.length > 0 && (
        <div className="min-h-0 flex-1 overflow-y-auto">
          <TreeRows nodes={roots} toggled={toggled} onToggle={toggle} />
        </div>
      )}
      {loadingMore && (
        <p className="shrink-0 px-2 pt-2 text-xs text-muted-foreground">{`已加载 ${loaded} 个文件…`}</p>
      )}
      {capped && (
        <p className="shrink-0 px-2 pt-2 text-xs text-status-warning">{`仅展示前 ${MAX_PAGES * PAGE_SIZE} 个文件`}</p>
      )}
    </div>
  );
}

function TreeRows({ nodes, toggled, onToggle }: {
  nodes: TreeNode[];
  toggled: Record<string, boolean>;
  onToggle: (node: TreeNode) => void;
}) {
  return nodes.map(node => node.file == null
    ? <DirectoryRow key={`d:${node.path}`} node={node} toggled={toggled} onToggle={onToggle} />
    : <FileRow key={`f:${node.file.index}`} node={node} />);
}

function DirectoryRow({ node, toggled, onToggle }: {
  node: TreeNode;
  toggled: Record<string, boolean>;
  onToggle: (node: TreeNode) => void;
}) {
  const expanded = toggled[node.path] ?? node.depth < 2;
  return (
    <div>
      <button
        type="button"
        aria-expanded={expanded}
        onClick={() => onToggle(node)}
        className="flex w-full cursor-pointer items-center gap-1.5 rounded-md py-1 pr-2 text-left text-sm hover:bg-muted"
        style={{ paddingInlineStart: `${node.depth * 1.25 + 0.5}rem` }}
      >
        {expanded
          ? <ChevronDown size={14} aria-hidden="true" className="shrink-0 text-muted-foreground" />
          : <ChevronRight size={14} aria-hidden="true" className="shrink-0 text-muted-foreground" />}
        <Folder size={14} aria-hidden="true" className="shrink-0 text-muted-foreground" />
        <span className="truncate">{node.name}</span>
        <span className="ml-auto shrink-0 text-xs text-muted-foreground tabular-nums">{`${node.count} 项`}</span>
      </button>
      {expanded && node.children.length > 0 && (
        <TreeRows nodes={node.children} toggled={toggled} onToggle={onToggle} />
      )}
    </div>
  );
}

function FileRow({ node }: { node: TreeNode }) {
  const file = node.file!;
  return (
    <div
      className="flex items-center gap-1.5 py-1 pr-2 text-sm"
      style={{ paddingInlineStart: `${node.depth * 1.25 + 1.75}rem` }}
    >
      <File size={14} aria-hidden="true" className="shrink-0 text-muted-foreground" />
      <span className="truncate">{node.name}</span>
      {(file.encoding_lossy || file.path_truncated) && (
        <Badge variant="outline" className="shrink-0">{file.encoding_lossy ? "有损" : "已截断"}</Badge>
      )}
      <span className="ml-auto shrink-0 text-xs text-muted-foreground tabular-nums">{bytes(file.length)}</span>
    </div>
  );
}
