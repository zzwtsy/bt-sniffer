import type { ReactNode } from "react";
import type { FlowStats } from "./flow-stats";
import { Link } from "@tanstack/react-router";
import { Fragment } from "react";
import { Status } from "@/components/observation/common";
import { count } from "@/lib/observation/format";

function Node({
  step,
  title,
  primary,
  secondary,
  badges,
}: {
  step: string;
  title: string;
  primary: ReactNode;
  secondary: ReactNode;
  badges?: ReactNode;
}) {
  return (
    <>
      <span className="flow-step">{step}</span>
      <h3>{title}</h3>
      <div className="flow-primary">{primary}</div>
      <p className="muted">{secondary}</p>
      {badges !== undefined && <div className="flow-badges">{badges}</div>}
    </>
  );
}
function Connector() {
  return (
    <div className="flow-connector" aria-hidden="true">
      <svg viewBox="0 0 40 16">
        <line x1="2" y1="8" x2="30" y2="8" stroke="currentColor" strokeWidth="1.5" />
        <path d="M30 3 L39 8 L30 13 Z" fill="currentColor" />
      </svg>
    </div>
  );
}
/** 五阶段流向图：节点只陈述对应来源的事实，窗口计数空缺时不补零。 */
export function FlowDiagram({
  stats,
  perSecond,
}: {
  stats: FlowStats;
  perSecond?: number | null;
}) {
  const links = [
    <Link key="discovery" to="/discoveries" className="flow-node" role="listitem">
      <Node
        step="01"
        title="发现"
        primary={
          stats.discovery.count === undefined
            ? "窗口内尚无记录"
            : `${count(stats.discovery.count)} 次发现`
        }
        secondary={
          stats.discovery.sample === false
            ? "主动采样未启用"
            : stats.discovery.sample === true
              ? "主动采样开启"
              : "采样状态未知"
        }
      />
    </Link>,
    <Link key="schedule" to="/jobs" className="flow-node" role="listitem">
      <Node
        step="02"
        title="接纳与调度"
        primary={`${count(stats.schedule.runningWorkers)} 个 worker`}
        secondary={`等待重试 ${count(stats.schedule.retryWait)}（数据库统计）`}
        badges={stats.schedule.paused.map(p => <Status key={p} value={p} />)}
      />
    </Link>,
    <Link
      key="lookup"
      to="/events"
      search={{ kind: "lookup", mode: "live" }}
      className="flow-node"
      role="listitem"
    >
      <Node
        step="03"
        title="peer 查找"
        primary={`${count(stats.lookup.active)} 进行中`}
        secondary={
          stats.lookup.completed === undefined
            ? "窗口内尚无记录"
            : `窗口内完成 ${count(stats.lookup.completed)} 次`
        }
        badges={
          (stats.lookup.noRoute ?? 0) > 0
            ? <Status value="no_route" />
            : undefined
        }
      />
    </Link>,
    <Link
      key="transfer"
      to="/events"
      search={{ kind: "peer", mode: "live" }}
      className="flow-node"
      role="listitem"
    >
      <Node
        step="04"
        title="连接与传输"
        primary={`${count(stats.transfer.active)} 进行中`}
        secondary={
          stats.transfer.accepted === undefined
            ? "窗口内尚无记录"
            : `窗口内有效接收 ${count(stats.transfer.accepted)} 片`
        }
      />
    </Link>,
    <Link key="commit" to="/metadata" className="flow-node" role="listitem">
      <Node
        step="05"
        title="校验与提交"
        primary={
          perSecond === undefined || perSecond === null
            ? "—"
            : `${perSecond.toFixed(2)} 次 / 秒`
        }
        secondary={`数据库累计 ${count(stats.commit.total)} 条`}
        badges={
          (stats.commit.notApplied ?? 0) > 0
            ? <Status value="stale" />
            : undefined
        }
      />
    </Link>,
  ];
  return (
    <div className="flow" role="list" aria-label="发现到提交的采集流程">
      {links.map((link, index) => (
        <Fragment key={link.key}>
          {index > 0 && <Connector />}
          {link}
        </Fragment>
      ))}
    </div>
  );
}
