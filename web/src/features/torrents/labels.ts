import type { StatusTone } from "@/components/observation/common";

/** 种子切片领域枚举的中文展示映射；接口新增未知值时回退为原字符串与 neutral 色。 */

const formatLabels: Record<string, string> = {
  unknown: "未知",
  v1: "v1",
  v2: "v2",
  hybrid: "混合",
};
export function formatLabel(format: string) {
  return formatLabels[format] ?? format;
}

const semanticLabels: Record<string, string> = {
  pending: "待校验",
  valid: "有效",
  invalid: "无效",
  unsupported: "不支持",
};
export function semanticLabel(status: string) {
  return semanticLabels[status] ?? status;
}

const semanticTones: Record<string, StatusTone> = {
  pending: "neutral",
  valid: "success",
  invalid: "danger",
  unsupported: "warning",
};
export function semanticTone(status: string): StatusTone {
  return semanticTones[status] ?? "neutral";
}

/** 语义状态异常时的兜底说明；后端给出 semantic_reason 时优先使用原文。 */
export function semanticDescription(status: string): string {
  const descriptions: Record<string, string> = {
    pending: "metadata 尚未完成语义校验，展示的信息可能不完整。",
    invalid: "metadata 未通过语义校验，展示的信息不可信。",
    unsupported: "该 metadata 版本暂不支持完整解析，仅展示已核对的事实。",
  };
  return descriptions[status] ?? "metadata 语义状态未知。";
}

/** 普通文件不产生徽标，仅特殊类型返回标签。 */
export function fileKindLabel(kind: string): string | null {
  if (kind === "file")
    return null;
  const labels: Record<string, string> = {
    padding: "填充",
    symlink: "符号链接",
  };
  return labels[kind] ?? kind;
}
