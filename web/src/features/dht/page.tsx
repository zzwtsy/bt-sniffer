import { Link, useParams } from "@tanstack/react-router";
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  CopyText,
  Empty,
  Freshness,
  Pager,
  PageTitle,
  Panel,
  QueryState,
  Status,
} from "@/components/observation/common";
import { EventTable } from "@/components/observation/event-table";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
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
import { useDisplayed, useEngine, useMonitor } from "@/lib/observation/context";
import {
  fieldsSchema,
  record,
  rows,
  string,
} from "@/lib/observation/contracts";
import { count, duration, label } from "@/lib/observation/format";

export function DhtPage() {
  const monitor = useMonitor();
  const snapshot = useDisplayed("dht:snapshot", monitor.snapshot);
  const nodes = rows(snapshot?.cached.nodes);
  return (
    <>
      <PageTitle
        title="DHT 网络"
        eyebrow="NETWORK"
        description="双栈节点独立观察，共享流量预算。无路由与本地限流不等同于远端失败。"
      />
      <div className="node-grid">
        {nodes.map(node => (
          <Panel
            key={string(node.id)}
            title={label(node.family)}
            description={string(node.address)}
            action={
              <Status value={((node.available === true)) ? "ready" : "数据源不可用"} />
            }
          >
            <p className="node-id">
              <code>{string(node.node_id)}</code>
            </p>
            <div className="node-stats">
              <span>
                在途
                <strong>{count(node.pending)}</strong>
              </span>
              <span>
                排队
                <strong>{count(node.queued)}</strong>
              </span>
              <span>
                联系人
                <strong>{count(record(node.routing).contact_count)}</strong>
              </span>
            </div>
            <Freshness at={node.observed_at_ms} />
            <p>
              <Link to="/dht/$id" params={{ id: string(node.id) }}>
                查看路由、采样与 RPC →
              </Link>
            </p>
          </Panel>
        ))}
      </div>
      {nodes.length === 0 && <Empty>等待节点快照，或当前没有启用节点。</Empty>}
    </>
  );
}
export function DhtDetailPage() {
  const { id = "" } = useParams({ strict: false });
  const monitor = useMonitor();
  const engine = useEngine();
  const page = usePageSearch();
  const snapshot = useDisplayed("node:snapshot", monitor.snapshot);
  const node = rows(snapshot?.cached.nodes).find(n => n.id === id);
  const query = useRead(
    `/dht/nodes/${encodeURIComponent(id)}/routing${queryString({ after: page.search.after, limit: page.limit })}`,
    fieldsSchema,
  );
  const sampling = record(node?.sampling);
  const detail = record(sampling.detail);
  const rpc = record(node?.rpc);
  const bootstrap = useDisplayed(
    "node:bootstrap",
    engine.buffer
      .select({}, 100)
      .filter(
        e =>
          ["bootstrap", "lifecycle"].includes(e.kind)
          && e.context.node_id === node?.node_id,
      ),
  );
  const buckets = rows(query.data?.buckets).map((b, i) => ({
    ...b,
    name: `${i + 1}`,
    count: typeof b.count === "number" ? b.count : null,
  }));
  return (
    <>
      <Link to="/dht">← DHT 网络</Link>
      <PageTitle
        title={`${label(node?.family)} 节点`}
        eyebrow="NODE DETAIL"
        description="节点编号属于当前运行；进程重启后重新核对节点身份。"
      />
      <CopyText value={string(node?.node_id)} />
      <Freshness at={node?.observed_at_ms} />
      <Panel
        title="路由桶容量"
        description="柱高为当前联系人数量；容量以表格给出的桶上限为准。"
      >
        <QueryState
          loading={query.isPending}
          error={query.error}
          hasData={!!query.data}
          retry={() => {
            void query.refetch();
          }}
        />
        <ChartContainer
          config={{ count: { label: "联系人", color: "var(--chart-2)" } }}
          className="h-[220px] w-full"
        >
          <BarChart data={buckets} accessibilityLayer>
            <CartesianGrid vertical={false} />
            <XAxis dataKey="name" />
            <YAxis allowDecimals={false} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <Bar
              dataKey="count"
              fill="var(--color-count)"
              radius={3}
              isAnimationActive={false}
            />
          </BarChart>
        </ChartContainer>
        <details>
          <summary>桶容量数据</summary>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>桶</TableHead>
                <TableHead>联系人</TableHead>
                <TableHead>容量</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows(query.data?.buckets).map(b => (
                <TableRow key={string(b.id)}>
                  <TableCell>
                    <code>{string(b.id)}</code>
                  </TableCell>
                  <TableCell>{count(b.count)}</TableCell>
                  <TableCell>{count(b.capacity)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </details>
        <Freshness at={query.data?.observed_at_ms} />
      </Panel>
      <Panel title="联系人">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>节点 ID</TableHead>
              <TableHead>地址</TableHead>
              <TableHead>状态</TableHead>
              <TableHead>距最近响应</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows(query.data?.items).map(c => (
              <TableRow key={string(c.node_id)}>
                <TableCell>
                  <code>{string(c.node_id)}</code>
                </TableCell>
                <TableCell>
                  <code>{string(c.address)}</code>
                </TableCell>
                <TableCell>
                  <Status value={c.status} />
                </TableCell>
                <TableCell>{duration(c.last_response_age_ms)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <Pager
          next={typeof query.data?.next === "string" ? query.data.next : null}
          onNext={page.next}
          onFirst={page.first}
          onPrevious={page.previous}
          canPrevious={(page.search.trail?.length ?? 0) > 0}
          hasCursor={page.search.after !== undefined}
          limit={page.limit}
          onLimit={limit =>
            page.change({ limit, after: undefined, trail: undefined })}
        />
      </Panel>
      <div className="overview-grid">
        <Panel
          title="主动采样"
          description="target、候选、回退与冷却来自当前节点状态。"
        >
          <p>
            target
            <code className="break-all">{string(detail.target)}</code>
          </p>
          <div className="node-stats">
            <span>
              在途
              {count(sampling.in_flight)}
            </span>
            <span>
              成功
              {count(sampling.successful)}
            </span>
            <span>
              失败
              {count(sampling.failed)}
            </span>
            <span>
              不支持
              {count(sampling.unsupported)}
            </span>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>候选</TableHead>
                <TableHead>冷却</TableHead>
                <TableHead>回退</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows(detail.candidates)
                .slice(0, 100)
                .map(c => (
                  <TableRow key={string(c.node_id)}>
                    <TableCell>
                      <code>{string(c.address)}</code>
                    </TableCell>
                    <TableCell>{duration(c.cooldown_ms)}</TableCell>
                    <TableCell>{c.fallback === true ? "find_node" : "否"}</TableCell>
                  </TableRow>
                ))}
            </TableBody>
          </Table>
          {!((sampling.running === true)) && (
            <p className="muted">当前没有运行中的主动采样轮次。</p>
          )}
        </Panel>
        <Panel
          title="RPC 当前队列"
          description="最多展示 100 个对象；点击 RPC 查看保留的等待原因及响应事件。"
        >
          {[
            ...rows(rpc.queued).map(r => ({ ...r, state: "waiting" })),
            ...rows(rpc.pending).map(r => ({ ...r, state: "sent" })),
          ]
            .slice(0, 100)
            .map((r) => {
              const c = record(record(r).context);
              return (
                <div className="rpc-row" key={string(c.rpc_id)}>
                  <Status value={r.state} />
                  <code>{string(record(r).peer)}</code>
                  <Link
                    to="/events"
                    search={{
                      object: string(c.rpc_id),
                      kind: "rpc",
                      mode: "history",
                    }}
                  >
                    RPC
                    {string(c.rpc_id)}
                    {" "}
                    →
                  </Link>
                </div>
              );
            })}
        </Panel>
      </div>
      <Panel
        title="启动与引导事件"
        description="仅展示浏览器已接收窗口中匹配本节点的事件，不代表完整启动历史。"
      >
        <EventTable events={bootstrap} />
      </Panel>
    </>
  );
}
