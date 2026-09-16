import type { ReactNode } from "react";
import type { Fields } from "@/lib/observation/contracts";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { queryString } from "@/lib/api/client";
import { useRead } from "@/lib/api/queries";
import { usePageSearch } from "@/lib/api/search";
import { listSchema } from "@/lib/observation/contracts";
import {
  Empty,
  Freshness,
  Pager,
  PageTitle,
  Panel,
  QueryState,
} from "./common";

export interface Column {
  name: string;
  cell: (row: Fields) => ReactNode;
}
export function Records({
  title,
  eyebrow,
  description,
  endpoint,
  columns,
  database = true,
  controls,
  state,
}: {
  title: string;
  eyebrow: string;
  description: string;
  endpoint: string;
  columns: Column[];
  database?: boolean;
  controls?: ReactNode;
  state?: string;
}) {
  const page = usePageSearch();
  const group = `${endpoint}:${state ?? "all"}`;
  const query = useRead(
    `${endpoint}${queryString({ after: page.search.after, limit: page.limit, state })}`,
    listSchema,
    database,
    true,
    group,
  );
  return (
    <>
      <PageTitle title={title} eyebrow={eyebrow} description={description} />
      {controls}
      <Panel
        title="记录"
        action={(
          <Freshness
            queried
            at={(Boolean(query.dataUpdatedAt)) || undefined}
            stale={!!query.error}
          />
        )}
      >
        <QueryState
          loading={query.isPending}
          error={query.error}
          hasData={!!query.data}
          retry={() => {
            void query.refetch();
          }}
        />
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map(c => (
                <TableHead key={c.name}>{c.name}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {query.data?.items.map((row, index) => (
              <TableRow key={String(row.hash ?? row.id ?? index)}>
                {columns.map(c => (
                  <TableCell key={c.name}>{c.cell(row)}</TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {query.data?.items.length === 0 && <Empty />}
        {query.data?.window && (
          <p className="muted">
            {query.data.window.evicted > 0 ? "历史部分保留" : "当前窗口内记录"}
            {" "}
            · 过程仅属于当前运行
          </p>
        )}
        <Pager
          next={query.data?.next}
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
    </>
  );
}
