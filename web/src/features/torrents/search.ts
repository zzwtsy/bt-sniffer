const HASH = /^[0-9a-f]{40}$/i;

export type TorrentInput
  = | { kind: "empty" }
    | { kind: "hash"; value: string }
    | { kind: "query"; value: string }
    | { kind: "error"; message: string };

export function classifyTorrentInput(input: string): TorrentInput {
  const value = input.trim();
  if (value === "")
    return { kind: "empty" };
  if (HASH.test(value))
    return { kind: "hash", value: value.toLowerCase() };
  const length = [...value].length;
  if (length < 3)
    return { kind: "error", message: "请输入至少 3 个字符，或输入完整的 40 位 hash。" };
  if (length > 200)
    return { kind: "error", message: "搜索内容不能超过 200 个字符。" };
  return { kind: "query", value };
}

export interface TextSegment { text: string; hit: boolean }

/** 按查询词切分文本用于命中高亮；ASCII 不区分大小写，与后端 trigram 语义一致。 */
export function splitByQuery(text: string, query: string): TextSegment[] {
  if (query === "")
    return [{ text, hit: false }];
  const escaped = query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const matcher = new RegExp(escaped, "giu");
  const segments: TextSegment[] = [];
  let cursor = 0;
  for (const match of text.matchAll(matcher)) {
    const index = match.index;
    if (index > cursor)
      segments.push({ text: text.slice(cursor, index), hit: false });
    segments.push({ text: text.slice(index, index + match[0].length), hit: true });
    cursor = index + match[0].length;
  }
  if (cursor < text.length)
    segments.push({ text: text.slice(cursor), hit: false });
  return segments;
}
