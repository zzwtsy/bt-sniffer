import { z } from "zod";

/** whatslink.info 公开接口的最小校验形状；其余字段（name/size/count 等）不使用。 */
const previewResponse = z.object({
  error: z.string(),
  screenshots: z.array(z.object({ screenshot: z.url() })).nullable(),
});

export interface Preview { screenshots: string[] }

export function previewApiUrl(hash: string) {
  const magnet = encodeURIComponent(`magnet:?xt=urn:btih:${hash}`);
  return `https://whatslink.info/api/v1/link?url=${magnet}`;
}

/** error 非空视为查询失败并抛出原文；screenshots 为 null 表示未收录，归一为空数组。 */
export function parsePreview(json: unknown): Preview {
  const parsed = previewResponse.parse(json);
  if (parsed.error !== "")
    throw new Error(parsed.error);
  return { screenshots: (parsed.screenshots ?? []).map(item => item.screenshot) };
}
