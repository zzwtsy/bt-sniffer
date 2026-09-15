import { Link, useParams } from "@tanstack/react-router";
import { PageTitle, Status } from "@/components/observation/common";
import { HistoryPanel } from "@/components/observation/history";
import { Records } from "@/components/observation/records";
import { string } from "@/lib/observation/contracts";
import { label, time } from "@/lib/observation/format";

export function DiscoveriesPage() {
  return (
    <Records
      title="发现记录"
      eyebrow="DISCOVERY"
      description="采样批次与 announce 观察。持久化成功事件在数据库事务提交后产生。"
      endpoint="/discoveries"
      database={false}
      columns={[
        {
          name: "发现对象",
          cell: r => (
            <Link to="/discoveries/$id" params={{ id: string(r.id) }}>
              <code>{string(r.id)}</code>
            </Link>
          ),
        },
        { name: "来源", cell: r => label(r.source) },
        { name: "最早保留时间", cell: r => time(r.first_retained_at_ms) },
        { name: "最近步骤", cell: r => label(r.last_step) },
        { name: "结果", cell: r => <Status value={r.last_result} /> },
        {
          name: "历史",
          cell: r =>
            r.completeness === "complete" ? "当前窗口内完整" : "部分保留",
        },
      ]}
    />
  );
}
export function DiscoveryDetailPage() {
  const { id = "" } = useParams({ strict: false });
  return (
    <>
      <Link to="/discoveries">← 返回发现列表</Link>
      <PageTitle
        title={`发现对象 ${id}`}
        eyebrow="DISCOVERY DETAIL"
        description="跟踪来源校验、批次交付、分段保存与关联 hash。来源缺失时保持未知。"
      />
      <HistoryPanel
        endpoint={`/discoveries/${encodeURIComponent(id)}`}
        title="来源与保存链路"
      />
    </>
  );
}
