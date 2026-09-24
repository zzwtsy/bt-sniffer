import type { TorrentDetail } from "@/lib/api/torrents";
import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { AlertCircle, ArrowLeft, FileQuestion } from "lucide-react";
import { PageTitle, Panel } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { detailOptions } from "@/lib/api/torrents";
import { CopyMagnet } from "./copy-magnet";
import { FileTree } from "./file-tree";
import { bytes, fetchedAt } from "./format";
import { formatLabel, semanticDescription, semanticLabel } from "./labels";
import { PreviewImages } from "./preview-images";

export function TorrentDetailPage({ hash, q, from }: { hash: string; q?: string; from?: number }) {
  const detail = useQuery(detailOptions(hash));
  const data = detail.data;
  const v1 = data?.identities.find(identity => identity.kind === "v1");
  const magnetName = data?.parse_status === "parsed" ? data.name : null;
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageTitle
        eyebrow="TORRENT DETAIL"
        title={data?.name ?? "种子详情"}
        description={data == null ? "摘要和文件路径来自本地保存的原始 info 字典。" : <SummaryMeta detail={data} />}
      >
        <Button variant="outline" nativeButton={false} render={<Link to="/torrents" search={{ q, page: from }} />}>
          <ArrowLeft data-icon="inline-start" />
          返回目录
        </Button>
      </PageTitle>
      {detail.isPending && <Skeleton className="mb-5 h-40 w-full" />}
      {detail.error != null && (
        <Alert variant="destructive" className="mb-5">
          <AlertCircle />
          <AlertTitle>详情无法读取</AlertTitle>
          <AlertDescription>{detail.error.message}</AlertDescription>
          <AlertAction>
            <Button variant="outline" size="sm" onClick={() => void detail.refetch()}>重试</Button>
          </AlertAction>
        </Alert>
      )}
      {data != null && (
        <div className="mb-5 flex flex-col gap-1.5">
          {data.identities.length > 0
            ? data.identities.map(identity => (
                <div key={identity.kind} className="flex items-center gap-1.5">
                  <Badge variant="outline" className="shrink-0">{identity.kind}</Badge>
                  <p className="break-all font-mono text-xs">{identity.hash}</p>
                  <CopyMagnet hash={identity.hash} name={magnetName} />
                </div>
              ))
            : (
                <div className="flex items-center gap-1.5">
                  <p className="break-all font-mono text-xs">{data.hash}</p>
                  <CopyMagnet hash={data.hash} name={magnetName} />
                </div>
              )}
        </div>
      )}
      {data != null && data.semantic_status !== "valid" && (
        <Alert variant={data.semantic_status === "invalid" ? "destructive" : "default"} className="mb-5">
          <AlertCircle />
          <AlertTitle>{semanticLabel(data.semantic_status)}</AlertTitle>
          <AlertDescription>{data.semantic_reason ?? semanticDescription(data.semantic_status)}</AlertDescription>
        </Alert>
      )}
      {data != null && data.parse_status === "unavailable" && data.semantic_status === "valid" && (
        <Alert className="mb-5">
          <FileQuestion />
          <AlertTitle>数据源不可用</AlertTitle>
          <AlertDescription>原始 metadata 无法安全解析，未展示不可信的语义字段。</AlertDescription>
        </Alert>
      )}
      {data != null && v1 != null && (
        <Panel
          title="预览图"
          description="截图来自 whatslink.info 第三方公开索引，点击加载后才发起查询。"
          className="shrink-0"
        >
          <PreviewImages hash={v1.hash} />
        </Panel>
      )}
      {data != null && data.parse_status === "parsed" && (
        <Panel
          title="文件清单"
          description="路径仅作为安全文本展示，不解释为本地文件系统路径。"
          className="mb-0 flex min-h-[240px] flex-1 flex-col"
          contentClassName="flex min-h-0 flex-1 flex-col"
        >
          <FileTree hash={hash} />
        </Panel>
      )}
    </div>
  );
}

/** 页头 meta 行：格式、采集时间与规模统计一眼扫过，未知项省略。 */
function SummaryMeta({ detail }: { detail: TorrentDetail }) {
  return (
    <span className="inline-flex flex-wrap items-center gap-x-2.5 gap-y-1">
      {detail.identities.length === 0 && <Badge variant="outline">{formatLabel(detail.format)}</Badge>}
      <span>
        采集于
        {" "}
        {fetchedAt(detail.fetched_at_ms)}
      </span>
      {detail.total_length != null && <span>{bytes(detail.total_length)}</span>}
      {detail.file_count != null && (
        <span>
          {detail.file_count}
          {" "}
          个文件
        </span>
      )}
      {detail.private === true && <Badge variant="outline">私有</Badge>}
      {(detail.encoding_lossy || detail.name_truncated) && (
        <Badge variant="outline">{detail.encoding_lossy ? "文本含有损显示" : "名称已截断"}</Badge>
      )}
    </span>
  );
}
