import { createFileRoute } from "@tanstack/react-router";
import { DiscoveryDetailPage } from "@/features/discoveries/page";

export const Route = createFileRoute("/discoveries/$id")({
  component: DiscoveryDetailPage,
});
