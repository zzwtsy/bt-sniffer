import { createFileRoute } from "@tanstack/react-router";
import { TorrentsPage } from "@/features/torrents/page";

interface TorrentSearch { q?: string; page?: number }

export const Route = createFileRoute("/torrents/")({
  validateSearch: (search: Record<string, unknown>): TorrentSearch => ({
    q: typeof search.q === "string" && search.q !== "" ? search.q : undefined,
    // 非法页码按第 1 页处理，不向后报错。
    page: typeof search.page === "number" && Number.isInteger(search.page) && search.page >= 1 ? search.page : undefined,
  }),
  component: function TorrentIndexRoute() {
    const search = Route.useSearch();
    return <TorrentsPage search={search} />;
  },
});
