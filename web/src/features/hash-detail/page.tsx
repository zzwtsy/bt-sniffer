import { Link, useParams } from "@tanstack/react-router";
import { useState } from "react";
import {
  CopyText,
  Empty,
  Freshness,
  PageTitle,
  Panel,
  QueryState,
  Status,
} from "@/components/observation/common";
import { EventTable } from "@/components/observation/event-table";
import { HistoryPanel } from "@/components/observation/history";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { useRead } from "@/lib/api/queries";
import { usePageSearch } from "@/lib/api/search";
import { useDisplayed, useEngine, useMonitor } from "@/lib/observation/context";
import {
  fieldsSchema,
  hashPattern,
  listSchema,
  number,
  record,
  rows,
  string,
} from "@/lib/observation/contracts";
import { bytes, count, label, time } from "@/lib/observation/format";
import { generations, peerEvents, pieces, spans } from "./model";
import { PieceBar } from "./piece-bar";
import { Stepper } from "./stepper";
import { Waterfall } from "./waterfall";

export function HashDetailPage() {
  const { hash = "" } = useParams({ strict: false });
  if (!hashPattern.test(hash))
    return <Empty>无效 hash：应为 40 位十六进制字符。</Empty>;
  return <HashDetail key={hash.toLowerCase()} hash={hash.toLowerCase()} />;
}
function HashDetail({ hash }: { hash: string }) {
  const page = usePageSearch();
  const engine = useEngine();
  const monitor = useMonitor();
  const fact = useRead(`/hashes/${hash}`, fieldsSchema, true);
  const [attemptCursor, setAttemptCursor] = useState<string>();
  const attempts = useRead(
    `/hashes/${hash}/attempts?limit=100${(attemptCursor != null && attemptCursor !== "") ? `&after=${attemptCursor}` : ""}`,
    listSchema,
    false,
    true,
    `attempts:${hash}`,
  );
  const snapshot = useDisplayed("hash:snapshot", monitor.snapshot);
  const events = useDisplayed(
    "hash:events",
    engine.buffer.select({ hash }, 100),
  );
  const job = record(fact.data?.job);
  const metadata = record(fact.data?.metadata);
  const available = [
    ...new Set([
      ...generations(events, number(job.generation)),
      ...(attempts.data?.items.flatMap(r =>
        number(r.generation) === undefined ? [] : [number(r.generation)!],
      ) ?? []),
    ]),
  ].sort((a, b) => b - a);
  const generation = page.search.generation ?? available[0];
  const selected = events.filter(e => e.context.generation === generation);
  const summary = attempts.data?.items.find(r => r.generation === generation);
  const peers = [
    ...new Set([
      ...selected.flatMap(e =>
        (e.context.peer_attempt_id != null) ? [e.context.peer_attempt_id] : [],
      ),
      ...rows(summary?.peers).flatMap(r =>
        typeof r.peer_attempt_id === "string" ? [r.peer_attempt_id] : [],
      ),
    ]),
  ];
  const peer = page.search.peer ?? peers.at(-1);
  const current
    = generation !== undefined && (peer != null && peer !== "")
      ? peerEvents(events, generation, peer)
      : [];
  const active = snapshot?.runtime.active.find((a) => {
    const c = record(a.context);
    return (
      c.hash === hash
      && c.generation === generation
      && c.peer_attempt_id === peer
      && record(a.data).complete !== undefined
    );
  });
  const track = spans(selected);
  const pieceModel = pieces(current, active);
  const lastResultByGeneration = new Map(
    (attempts.data?.items ?? []).flatMap((r) => {
      const g = number(r.generation);
      return g === undefined ? [] : [[g, r.last_result] as const];
    }),
  );
  const origin = events.find(
    e =>
      e.kind === "discovery"
      && (e.context.batch_id !== undefined || e.context.observation_id !== undefined),
  );
  const originId = origin?.context.batch_id ?? origin?.context.observation_id;
  return (
    <>
      <Link to="/hashes">← 已发现的 hash</Link>
      <PageTitle
        title="hash 采集详情"
        eyebrow="TRACE DETAIL"
        description="数据库事实、当前运行和有限历史分别展示。未知步骤不会被补造为成功。"
      />
      <div className="mb-5 flex items-center justify-between gap-3">
        <CopyText value={hash} />
        <Status value={job.state} />
      </div>
      <QueryState
        loading={fact.isPending}
        error={fact.error}
        hasData={!!fact.data}
        retry={() => {
          void fact.refetch();
        }}
      />
      <Alert className="mb-4">
        <AlertDescription>
          过程只展示已载入及当前保留的事件。
          {(((monitor.snapshot?.window.evicted ?? 0) > 0)) ? "后端有历史淘汰。" : ""}
          {monitor.evicted > 0 ? "浏览器也已淘汰旧记录。" : ""}
        </AlertDescription>
      </Alert>
      <Panel
        title="采集链路"
        description="点击 generation 或 peer 切换视角；每轮领取和每个 peer 的结果独立关联。"
      >
        <Stepper
          origin={
            (originId != null && originId !== "")
              ? { id: originId, batch: (origin?.context.batch_id) != null }
              : undefined
          }
          generations={available.map(g => ({
            value: g,
            lastResult: g === generation
              ? selected.at(-1)?.result ?? summary?.last_result
              : lastResultByGeneration.get(g),
          }))}
          generation={generation}
          peers={peers}
          peer={peer}
          transfer={
            (peer != null && peer !== "")
              ? (
                  <span>
                    有效
                    {count(pieceModel.received)}
                    {" "}
                    /
                    {count(pieceModel.total)}
                  </span>
                )
              : (
                  <span className="text-xs text-muted-foreground">未选择 peer</span>
                )
          }
          commit={(
            <span className="flex flex-wrap items-center gap-2.5">
              <Status value={job.state} />
              <span className="text-xs text-muted-foreground">
                {fact.data?.metadata === undefined
                  ? "保存状态未知"
                  : fact.data.metadata === null
                    ? "无保存记录"
                    : `已保存 ${bytes(metadata.bytes)}`}
              </span>
            </span>
          )}
          onSelect={(next) => {
            page.change(
              next.generation !== undefined
                ? { generation: next.generation, peer: undefined }
                : { peer: next.peer },
            );
          }}
        />
      </Panel>
      <div className="grid grid-cols-[minmax(0,2.2fr)_minmax(250px,1fr)] gap-5 max-[1200px]:grid-cols-1">
        <div>
          <Panel
            title="时间轴与分片"
            description="当前选中 generation 的阶段起止和选中 peer 的分片进度。"
          >
            <QueryState
              loading={attempts.isPending}
              error={attempts.error}
              hasData={!!attempts.data}
            />
            <p className="text-xs text-muted-foreground">
              {label(record(summary?.claim).attempt_kind)}
              {" "}
              · 领取前远端失败
              {" "}
              {count(record(summary?.claim).failed_attempts_before)}
              {" "}
              · 类别
              {" "}
              {label(record(summary?.claim).class)}
            </p>
            {(Boolean((attempts.data?.next))) && (
              <Button
                variant="outline"
                size="sm"
                onClick={() =>
                  setAttemptCursor(attempts.data?.next ?? undefined)}
              >
                加载后续领取摘要
              </Button>
            )}
            <Waterfall track={track} />
            {(peer != null && peer !== "") && (
              <PieceBar key={`${generation ?? "-"}:${peer}`} model={pieceModel} />
            )}
            <EventTable events={current.slice(-50)} />
          </Panel>
          <HistoryPanel hash={hash} title="加载链路事件" />
        </div>
        <div>
          <Panel
            title="数据库事实"
            action={(
              <Freshness
                queried
                at={(Boolean(fact.dataUpdatedAt)) || undefined}
                stale={!!fact.error}
              />
            )}
          >
            <dl className="grid grid-cols-[100px_minmax(0,1fr)] gap-3.5 text-xs">
              <dt className="text-muted-foreground">任务状态</dt>
              <dd className="m-0 wrap-anywhere">
                <Status value={job.state} />
              </dd>
              <dt className="text-muted-foreground">generation</dt>
              <dd className="m-0 wrap-anywhere">{count(job.generation)}</dd>
              <dt className="text-muted-foreground">远端失败次数</dt>
              <dd className="m-0 wrap-anywhere">{count(job.remote_failures)}</dd>
              <dt className="text-muted-foreground">首次发现</dt>
              <dd className="m-0 wrap-anywhere">{time(fact.data?.first_seen_ms)}</dd>
              <dt className="text-muted-foreground">最近观察</dt>
              <dd className="m-0 wrap-anywhere">{time(fact.data?.last_seen_ms)}</dd>
              <dt className="text-muted-foreground">重试到期</dt>
              <dd className="m-0 wrap-anywhere">{time(job.due_at_ms)}</dd>
              <dt className="text-muted-foreground">metadata</dt>
              <dd className="m-0 wrap-anywhere">
                {fact.data?.metadata === null
                  ? "无保存记录"
                  : bytes(metadata.bytes)}
              </dd>
              <dt className="text-muted-foreground">校验摘要</dt>
              <dd className="m-0 wrap-anywhere">
                {metadata.verification === "validated_before_commit"
                  ? "提交前已校验，查询未重校验"
                  : "未知"}
              </dd>
            </dl>
          </Panel>
          <Panel title="来源与提示">
            <p>
              {(originId != null && originId !== "")
                ? (
                    <Link to="/discoveries/$id" params={{ id: originId }}>
                      查看
                      {((origin?.context.batch_id) != null) ? "采样批次" : "announce 观察"}
                      {" "}
                      {originId}
                    </Link>
                  )
                : (
                    "最初来源未知或尚未载入"
                  )}
            </p>
            <p className="text-xs text-muted-foreground">当前有效提示不能证明最初发现来源。</p>
            {rows(fact.data?.peer_hints).map(h => (
              <div className="border-t py-2.5" key={string(h.peer)}>
                <code>{string(h.peer)}</code>
                <small className="block text-muted-foreground">{time(h.observed_at_ms)}</small>
              </div>
            ))}
          </Panel>
          <Panel title="结果的含义">
            <p>单个 peer 失败后仍可切换其他候选。</p>
            <p>
              下载、校验、提交是不同阶段。只有 Applied 表示本次更新已应用；Stale
              表示旧领取未应用。
            </p>
          </Panel>
        </div>
      </div>
    </>
  );
}
