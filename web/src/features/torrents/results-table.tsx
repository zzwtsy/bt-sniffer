import type { CatalogPage } from "@/lib/api/torrents";
import { Link } from "@tanstack/react-router";
import { StatusBadge } from "@/components/observation/common";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { CopyMagnet } from "./copy-magnet";
import { bytes, fetchedAt, relativeTime, shortHash } from "./format";
import { highlight } from "./highlight";
import { semanticLabel, semanticTone } from "./labels";

type CatalogItem = CatalogPage["items"][number];

export function ResultsTable({ items, q, from }: { items: CatalogItem[]; q: string; from: number }) {
  return (
    // 通过 Table 已有的 data-slot 约束滚动包装层，不修改公共 UI 原语。
    <div className="flex min-h-0 flex-1 flex-col *:data-[slot=table-container]:min-h-0 *:data-[slot=table-container]:flex-1 *:data-[slot=table-container]:overflow-auto">
      <Table className="min-w-215">
        <TableHeader className="sticky top-0 z-10 bg-card">
          <TableRow>
            <TableHead>名称</TableHead>
            <TableHead>大小</TableHead>
            <TableHead>文件</TableHead>
            <TableHead>采集时间</TableHead>
            <TableHead>Hash</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item) => {
            const name = item.parse_status === "parsed" && item.name != null ? item.name : "无法解析的 metadata";
            return (
              <TableRow key={item.hash}>
                <TableCell className="max-w-105 whitespace-normal">
                  {/* 无空格长 token 也需要换行；两行截断保持行高，完整名称经 title 悬浮可见 */}
                  <Link
                    to="/torrents/$hash"
                    params={{ hash: item.hash }}
                    search={{ q: q === "" ? undefined : q, from }}
                    title={name}
                    className="font-medium wrap-anywhere line-clamp-2"
                  >
                    {highlight(name, q)}
                  </Link>
                  {(item.semantic_status !== "valid" || item.parse_status === "unavailable") && (
                    <div className="mt-1 flex flex-wrap items-center gap-1">
                      <StatusBadge tone={semanticTone(item.semantic_status)}>{semanticLabel(item.semantic_status)}</StatusBadge>
                      {item.parse_status === "unavailable" && <Badge variant="outline">不可解析</Badge>}
                    </div>
                  )}
                  {(item.encoding_lossy || item.name_truncated) && (
                    <p className="mt-1 text-xs text-muted-foreground">{item.encoding_lossy ? "包含非 UTF-8 文本" : "名称已截断"}</p>
                  )}
                  {item.match_excerpt != null && (
                    <p className="mt-1 break-all text-xs text-muted-foreground">
                      路径命中：
                      {highlight(item.match_excerpt, q)}
                    </p>
                  )}
                </TableCell>
                <TableCell>{bytes(item.total_length)}</TableCell>
                <TableCell>{item.file_count ?? "—"}</TableCell>
                <TableCell>
                  <span title={fetchedAt(item.fetched_at_ms)}>{relativeTime(item.fetched_at_ms)}</span>
                </TableCell>
                <TableCell>
                  <span className="inline-flex items-center gap-1">
                    <code title={item.hash}>{shortHash(item.hash)}</code>
                    <CopyMagnet hash={item.hash} name={item.parse_status === "parsed" ? item.name : null} />
                  </span>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
