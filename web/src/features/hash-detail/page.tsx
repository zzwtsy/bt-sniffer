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
import {
  NativeSelect,
  NativeSelectOption,
} from "@/components/ui/native-select";
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
import { bytes, count, duration, label, time } from "@/lib/observation/format";
import { generations, peerEvents, pieces, spans } from "./model";

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
  const start = track[0]?.at ?? 0;
  const end = Math.max(start + 1, ...track.map(t => t.at + (t.elapsed ?? 0)));
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
      <div className="hash-heading">
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
      <div className="detail-grid">
        <div>
          <Panel
            title="领取与 peer 尝试"
            description="每轮 generation 和每个 peer 的结果独立关联。"
          >
            <QueryState
              loading={attempts.isPending}
              error={attempts.error}
              hasData={!!attempts.data}
            />
            <div className="filter-bar">
              <label>
                领取
                <NativeSelect
                  value={generation ?? ""}
                  onChange={e =>
                    page.change({
                      generation: Number(e.target.value),
                      peer: undefined,
                    })}
                >
                  {available.length === 0 && (
                    <NativeSelectOption value="">未知</NativeSelectOption>
                  )}
                  {available.map(g => (
                    <NativeSelectOption key={g} value={g}>
                      generation
                      {g}
                    </NativeSelectOption>
                  ))}
                </NativeSelect>
              </label>
              <label>
                peer
                <NativeSelect
                  value={peer ?? ""}
                  onChange={e => page.change({ peer: e.target.value })}
                >
                  {peers.length === 0 && (
                    <NativeSelectOption value="">无保留尝试</NativeSelectOption>
                  )}
                  {peers.map(p => (
                    <NativeSelectOption key={p} value={p}>
                      {p}
                    </NativeSelectOption>
                  ))}
                </NativeSelect>
              </label>
            </div>
            <p className="muted">
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
            {generation !== undefined && (
              <p>
                选中 generation
                {generation}
                ，最近保留结果：
                <Status
                  value={selected.at(-1)?.result ?? summary?.last_result}
                />
              </p>
            )}
            {(track.length > 0)
              ? (
                  <div className="tracks" aria-label="阶段并列时间轨道">
                    {track.map(t => (
                      <div className="track" key={t.id}>
                        <div>
                          <strong>
                            {label(t.kind)}
                            {" "}
                            /
                            {label(t.step)}
                          </strong>
                          <small>
                            {duration(t.elapsed)}
                            {" "}
                            ·
                            {label(t.result)}
                          </small>
                        </div>
                        <div className="track-space">
                          <span
                            className={`track-bar ${t.elapsed === undefined ? "open" : ""}`}
                            style={{
                              left: `${((t.at - start) * 100) / (end - start)}%`,
                              width: `${Math.max(1, ((t.elapsed ?? 0) * 100) / (end - start))}%`,
                            }}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                )
              : (
                  <Empty>尚未载入可配对的阶段起止事件；不能据此断言未执行。</Empty>
                )}
            {(peer != null && peer !== "") && (
              <PieceView
                key={`${generation}:${peer}`}
                model={pieces(current, active)}
              />
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
            <dl className="facts">
              <dt>任务状态</dt>
              <dd>
                <Status value={job.state} />
              </dd>
              <dt>generation</dt>
              <dd>{count(job.generation)}</dd>
              <dt>远端失败次数</dt>
              <dd>{count(job.remote_failures)}</dd>
              <dt>首次发现</dt>
              <dd>{time(fact.data?.first_seen_ms)}</dd>
              <dt>最近观察</dt>
              <dd>{time(fact.data?.last_seen_ms)}</dd>
              <dt>重试到期</dt>
              <dd>{time(job.due_at_ms)}</dd>
              <dt>metadata</dt>
              <dd>
                {fact.data?.metadata === null
                  ? "无保存记录"
                  : bytes(metadata.bytes)}
              </dd>
              <dt>校验摘要</dt>
              <dd>
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
            <p className="muted">当前有效提示不能证明最初发现来源。</p>
            {rows(fact.data?.peer_hints).map(h => (
              <div className="hint" key={string(h.peer)}>
                <code>{string(h.peer)}</code>
                <small>{time(h.observed_at_ms)}</small>
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
function PieceView({ model }: { model: ReturnType<typeof pieces> }) {
  const [offset, setOffset] = useState(0);
  return (
    <div className="piece-section">
      <div className="mb-4 flex items-start justify-between gap-3">
        <h3>本 peer 的分片</h3>
        <span>
          有效
          {count(model.received)}
          {" "}
          /
          {count(model.total)}
        </span>
      </div>
      <p className="muted">
        当前保留的重复接收
        {model.duplicates}
        {" "}
        次；空白表示状态未知，不能算作尚未请求。
      </p>
      {model.total !== undefined
        ? (
            <>
              <div className="pieces" role="list" aria-label="分片状态">
                {Array.from(
                  { length: Math.min(128, Math.max(0, model.total - offset)) },
                  (_, i) => {
                    const index = offset + i;
                    const state = model.states.get(index) ?? "unknown";
                    const text = {
                      received: "有效",
                      requested: "已请求",
                      pending: "未收到",
                      invalid: "错误",
                      unknown: "未知",
                    }[state];
                    return (
                      <span
                        role="listitem"
                        key={index}
                        className={`piece ${state}`}
                        title={`分片 ${index}：${text}`}
                        aria-label={`分片 ${index}：${text}`}
                      >
                        {state === "received"
                          ? "✓"
                          : state === "invalid"
                            ? "!"
                            : "·"}
                        <small>{index}</small>
                      </span>
                    );
                  },
                )}
              </div>
              <div className="flex gap-1.5">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - 128))}
                >
                  前 128 片
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={offset + 128 >= model.total}
                  onClick={() => setOffset(offset + 128)}
                >
                  后 128 片
                </Button>
              </div>
            </>
          )
        : (
            <Empty>分片总数未知，等待保留事件或当前状态。</Empty>
          )}
    </div>
  );
}
