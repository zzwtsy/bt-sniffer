import { createFileRoute } from "@tanstack/react-router";
import { TorrentDetailPage } from "@/features/torrents/detail-page";

interface TorrentDetailSearch { q?: string; from?: number }

export const Route = createFileRoute("/torrents/$hash")({
  validateSearch: (search: Record<string, unknown>): TorrentDetailSearch => ({
    q: typeof search.q === "string" && search.q !== "" ? search.q : undefined,
    // from 是来源目录页码，用于"返回目录"落回原页。
    from: typeof search.from === "number" && Number.isInteger(search.from) && search.from >= 1 ? search.from : undefined,
  }),
  component: function TorrentDetailRoute() {
    const { hash } = Route.useParams();
    const { q, from } = Route.useSearch();
    return <TorrentDetailPage hash={hash.toLowerCase()} q={q} from={from} />;
  },
});
