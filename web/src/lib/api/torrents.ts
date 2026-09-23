import { infiniteQueryOptions, queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { read } from "@/lib/api/client";

const indexState = z.object({
  indexed: z.number().int().nonnegative(),
  total: z.number().int().nonnegative(),
  complete: z.boolean(),
  search_complete: z.boolean(),
});

const catalogItem = z.object({
  hash: z.string().regex(/^[0-9a-f]{40}$/),
  parse_status: z.enum(["parsed", "unavailable"]),
  name: z.string().nullable(),
  name_truncated: z.boolean(),
  encoding_lossy: z.boolean(),
  total_length: z.string().regex(/^\d+$/).nullable(),
  file_count: z.number().int().nonnegative().nullable(),
  piece_length: z.string().regex(/^\d+$/).nullable(),
  piece_count: z.number().int().nonnegative().nullable(),
  private: z.boolean().nullable(),
  fetched_at_ms: z.number().int().nonnegative(),
  match_excerpt: z.string().optional(),
});

const catalogPage = z.object({
  items: z.array(catalogItem),
  total: z.number().int().nonnegative(),
  page: z.number().int().min(1),
  index: indexState,
});

const torrentDetail = catalogItem.omit({ match_excerpt: true }).extend({
  file_count: z.number().int().nonnegative().nullable(),
});

const fileItem = z.object({
  index: z.number().int().nonnegative(),
  path: z.string(),
  path_truncated: z.boolean(),
  encoding_lossy: z.boolean(),
  length: z.string().regex(/^\d+$/),
});

const filePage = z.object({
  available: z.boolean(),
  items: z.array(fileItem),
  next: z.string().nullable(),
});

export type CatalogPage = z.infer<typeof catalogPage>;
export type TorrentDetail = z.infer<typeof torrentDetail>;
export type FileItem = z.infer<typeof fileItem>;
export type FilePage = z.infer<typeof filePage>;

function queryString(values: Record<string, string | number | undefined>) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined && value !== "")
      params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded === "" ? "" : `?${encoded}`;
}

/** 目录每页条数：前端按 total 换算总页数，必须与请求 limit 一致。 */
export const CATALOG_PAGE_SIZE = 50;

export function catalogOptions(q?: string, page = 1) {
  const path = `/torrents${queryString({ q, page, limit: CATALOG_PAGE_SIZE })}`;
  return queryOptions({
    queryKey: ["torrent-catalog", q ?? "", page],
    queryFn: async ({ signal }) => read(path, catalogPage, signal, true),
  });
}

export function detailOptions(hash: string) {
  return queryOptions({
    queryKey: ["torrent-detail", hash],
    queryFn: async ({ signal }) => read(`/torrents/${hash}`, torrentDetail, signal, true),
  });
}

export function filesAllOptions(hash: string) {
  return infiniteQueryOptions({
    queryKey: ["torrent-files", hash],
    queryFn: async ({ pageParam, signal }) =>
      read(`/torrents/${hash}/files${queryString({ after: pageParam, limit: 100 })}`, filePage, signal, true),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last: FilePage) => last.next ?? undefined,
  });
}
