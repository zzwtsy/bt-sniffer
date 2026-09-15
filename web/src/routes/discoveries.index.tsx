import { createFileRoute } from "@tanstack/react-router";
import { DiscoveriesPage } from "@/features/discoveries/page";

export const Route = createFileRoute("/discoveries/")({
  component: DiscoveriesPage,
});
