import type { ReactNode } from "react";
import { Fragment } from "react";
import { splitByQuery } from "./search";

/** 将文本按查询词渲染为片段；命中段用 mark 标签承载语义与高亮。 */
export function highlight(text: string, query: string): ReactNode {
  const segments = splitByQuery(text, query);
  if (segments.length === 1 && !segments[0].hit)
    return text;
  let offset = 0;
  return segments.map((segment) => {
    // 片段静态有序，以文本偏移作为稳定键
    const key = offset;
    offset += segment.text.length;
    return segment.hit
      ? <mark key={key} className="rounded-xs bg-primary/15 px-0.5 text-inherit">{segment.text}</mark>
      : <Fragment key={key}>{segment.text}</Fragment>;
  });
}
