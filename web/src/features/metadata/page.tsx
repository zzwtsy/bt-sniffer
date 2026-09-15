import { HashLink } from "@/components/observation/common";
import { Records } from "@/components/observation/records";
import { string } from "@/lib/observation/contracts";
import { bytes, time } from "@/lib/observation/format";

export function MetadataPage() {
  return (
    <Records
      title="Metadata"
      eyebrow="VERIFIED RECORDS"
      description="仅展示大小、获取时间与采集校验摘要，不提供名称、文件列表或原始内容。"
      endpoint="/metadata"
      columns={[
        { name: "hash", cell: r => <HashLink hash={string(r.hash)} /> },
        { name: "大小", cell: r => bytes(r.bytes) },
        { name: "获取时间", cell: r => time(r.fetched_at_ms) },
        {
          name: "校验",
          cell: r =>
            r.verification === "validated_before_commit"
              ? "采集提交前已校验"
              : "未知",
        },
        {
          name: "查询时重校验",
          cell: r =>
            r.content_rechecked === false ? "未重新读取正文" : "未知",
        },
      ]}
    />
  );
}
