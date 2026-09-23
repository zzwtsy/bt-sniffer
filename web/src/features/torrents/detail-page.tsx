import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { AlertCircle, ArrowLeft, FileQuestion } from "lucide-react";
import { Metric, PageTitle, Panel } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { detailOptions } from "@/lib/api/torrents";
import { CopyMagnet } from "./copy-magnet";
import { FileTree } from "./file-tree";
import { bytes, fetchedAt } from "./format";
import { PreviewImages } from "./preview-images";

export function TorrentDetailPage({ hash, q, from }: { hash: string; q?: string; from?: string }) {
  const detail = useQuery(detailOptions(hash));
  return (
    <>
      <PageTitle
        eyebrow="TORRENT DETAIL"
        title={detail.data?.name ?? "种子详情"}
        description="摘要和文件路径来自本地保存的原始 v1 info 字典。"
      >
        <Button variant="outline" nativeButton={false} render={<Link to="/torrents" search={{ q, after: from }} />}>
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
      {detail.data != null && (
        <>
          <Panel title="种子摘要" description={`采集于 ${fetchedAt(detail.data.fetched_at_ms)}`}>
            <div className="mb-4 flex items-center gap-1">
              <p className="break-all font-mono text-xs">{detail.data.hash}</p>
              <CopyMagnet hash={detail.data.hash} name={detail.data.parse_status === "parsed" ? detail.data.name : null} />
            </div>
            {detail.data.parse_status === "unavailable"
              ? (
                  <Alert>
                    <FileQuestion />
                    <AlertTitle>数据源不可用</AlertTitle>
                    <AlertDescription>原始 metadata 无法安全解析，未展示不可信的语义字段。</AlertDescription>
                  </Alert>
                )
              : (
                  <div className="grid grid-cols-4 gap-3 max-[1000px]:grid-cols-2 max-[560px]:grid-cols-1">
                    <Metric title="总大小" value={bytes(detail.data.total_length)} detail="十进制原始大小" icon={<span>Σ</span>} />
                    <Metric title="文件数" value={String(detail.data.file_count ?? "—")} detail="按原始顺序" icon={<span>#</span>} />
                    <Metric title="Piece 长度" value={bytes(detail.data.piece_length)} detail={`${detail.data.piece_count ?? "—"} pieces`} icon={<span>◫</span>} />
                    <Metric title="Private" value={detail.data.private == null ? "未声明" : detail.data.private ? "是" : "否"} detail="info 字典标记" icon={<span>◉</span>} />
                  </div>
                )}
            {(detail.data.encoding_lossy || detail.data.name_truncated) && <Badge variant="outline" className="mt-4">{detail.data.encoding_lossy ? "文本含有损显示" : "名称已截断"}</Badge>}
          </Panel>
          <Panel title="预览图" description="截图来自 whatslink.info 第三方公开索引，点击加载后才发起查询。">
            <PreviewImages hash={hash} />
          </Panel>
          {detail.data.parse_status === "parsed" && (
            <Panel title="文件清单" description="路径仅作为安全文本展示，不解释为本地文件系统路径。">
              <FileTree hash={hash} />
            </Panel>
          )}
        </>
      )}
    </>
  );
}
