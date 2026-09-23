import { createFileRoute } from "@tanstack/react-router";
import { TorrentsPage } from "@/features/torrents/page";

interface TorrentSearch { q?: string; after?: string }

export const Route = createFileRoute("/torrents/")({
  validateSearch: (search: Record<string, unknown>): TorrentSearch => ({
    q: typeof search.q === "string" && search.q !== "" ? search.q : undefined,
    after: typeof search.after === "string" && search.after !== "" ? search.after : undefined,
  }),
  component: function TorrentIndexRoute() {
    const search = Route.useSearch();
    return <TorrentsPage search={search} />;
  },
});
