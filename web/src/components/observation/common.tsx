import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { ArrowUpRight, Copy, RefreshCw } from "lucide-react";
import { useState } from "react";
import { label, shortHash, time } from "@/lib/observation/format";

export function Panel({
  title,
  description,
  children,
  action,
  className = "",
}: {
  title: string;
  description?: string;
  children: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-heading">
        <div>
          <h2>{title}</h2>
          {(Boolean(description)) && <p className="muted">{description}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}
export function PageTitle({
  eyebrow,
  title,
  description,
  children,
}: {
  eyebrow: string;
  title: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <div className="page-title">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        <p className="muted">{description}</p>
      </div>
      {children}
    </div>
  );
}
export function Status({ value }: { value: unknown }) {
  const raw = typeof value === "string" ? value : "unknown";
  const tone = /failed|error|invalid|mismatch|timeout/.test(raw)
    ? "danger"
    : /wait|pause|capacity|stale|dormant/.test(raw)
      ? "warning"
      : /applied|succeeded|validated|ready|good/.test(raw)
        ? "success"
        : /running|started|sent/.test(raw)
          ? "active"
          : "neutral";
  return (
    <span className={`status ${tone}`}>
      <span aria-hidden="true" />
      {label(value)}
    </span>
  );
}
export function HashLink({
  hash,
  full = false,
}: {
  hash: string;
  full?: boolean;
}) {
  return (
    <Link
      to="/hashes/$hash"
      params={{ hash }}
      className="hash-link"
      title={hash}
    >
      <code>{full ? hash : shortHash(hash)}</code>
      <ArrowUpRight size={12} aria-hidden="true" />
    </Link>
  );
}
export function CopyText({ value }: { value: string }) {
  const [message, setMessage] = useState("");
  return (
    <span className="copy-wrap">
      <code>{value}</code>
      <button
        className="icon-button"
        aria-label="复制完整值"
        onClick={() => {
          void navigator.clipboard.writeText(value).then(
            () => setMessage("已复制"),
            () => setMessage("复制失败，请选择文本复制"),
          );
        }}
      >
        <Copy size={14} />
      </button>
      <span className="muted" role="status">
        {message}
      </span>
    </span>
  );
}
export function Empty({
  children = "当前没有记录。数据缺失不代表相关过程未发生。",
}: {
  children?: ReactNode;
}) {
  return (
    <div className="empty">
      <span className="empty-dot" aria-hidden="true" />
      <p>{children}</p>
    </div>
  );
}
export function QueryState({
  loading,
  error,
  hasData = false,
  retry,
}: {
  loading: boolean;
  error: Error | null;
  hasData?: boolean;
  retry?: () => void;
}) {
  if (error) {
    return (
      <div className="notice danger" role="status">
        {hasData ? "刷新失败，保留上次结果。" : "暂时无法读取。"}
        {error.message}
        {retry && (
          <button className="quiet-button" onClick={retry}>
            <RefreshCw size={14} />
            重试查询
          </button>
        )}
      </div>
    );
  }
  if (loading && !hasData) {
    return (
      <div className="loading" role="status">
        <span className="skeleton" />
        <span className="skeleton" />
        <span className="skeleton" />
        <span className="sr-only">正在读取数据</span>
      </div>
    );
  }
  return null;
}
export function Freshness({ at, stale, queried = false }: { at?: unknown; stale?: boolean; queried?: boolean }) {
  return (
    <span className={`freshness ${stale ? "warning-text" : ""}`}>
      {stale ? "陈旧数据 · " : queried ? "查询时间 · " : "观察时间 · "}
      {time(at)}
    </span>
  );
}
export function Pager({
  next,
  onNext,
  onPrevious,
  onFirst,
  canPrevious,
  hasCursor,
  limit,
  onLimit,
}: {
  next?: string | null;
  onNext: (next: string) => void;
  onPrevious: () => void;
  onFirst: () => void;
  canPrevious: boolean;
  hasCursor: boolean;
  limit: number;
  onLimit: (limit: 50 | 100) => void;
}) {
  return (
    <div className="pager">
      <label>
        每页
        <select
          value={limit}
          onChange={e => onLimit(e.target.value === "100" ? 100 : 50)}
        >
          <option value={50}>50</option>
          <option value={100}>100</option>
        </select>
        {" "}
        条
      </label>
      <span className="muted">固定顺序 · 分页期间数据可能变化</span>
      <div className="button-group">
        <button disabled={!hasCursor} onClick={onFirst}>
          首批
        </button>
        <button disabled={!canPrevious} onClick={onPrevious}>
          上一页
        </button>
        <button disabled={next == null} onClick={() => (next != null && next !== "") && onNext(next)}>
          下一页
        </button>
      </div>
    </div>
  );
}
export function Metric({
  title,
  value,
  detail,
  icon,
}: {
  title: string;
  value: string;
  detail: string;
  icon: ReactNode;
}) {
  return (
    <div className="metric">
      <div className="metric-label">
        {title}
        {icon}
      </div>
      <div className="metric-value">{value}</div>
      <div className="muted">{detail}</div>
    </div>
  );
}
