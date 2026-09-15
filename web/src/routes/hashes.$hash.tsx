import { createFileRoute } from "@tanstack/react-router";
import { HashDetailPage } from "@/features/hash-detail/page";

export const Route = createFileRoute("/hashes/$hash")({
  component: HashDetailPage,
});
