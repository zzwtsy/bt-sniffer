import { createFileRoute } from "@tanstack/react-router";
import { TorrentDetailPage } from "@/features/torrents/detail-page";

interface TorrentDetailSearch { q?: string; from?: string }

export const Route = createFileRoute("/torrents/$hash")({
  validateSearch: (search: Record<string, unknown>): TorrentDetailSearch => ({
    q: typeof search.q === "string" && search.q !== "" ? search.q : undefined,
    from: typeof search.from === "string" && search.from !== "" ? search.from : undefined,
  }),
  component: function TorrentDetailRoute() {
    const { hash } = Route.useParams();
    const { q, from } = Route.useSearch();
    return <TorrentDetailPage hash={hash.toLowerCase()} q={q} from={from} />;
  },
});
