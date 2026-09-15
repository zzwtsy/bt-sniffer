import { createFileRoute } from "@tanstack/react-router";
import { DhtDetailPage } from "@/features/dht/page";

export const Route = createFileRoute("/dht/$id")({ component: DhtDetailPage });
