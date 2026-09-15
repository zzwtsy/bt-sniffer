import { createFileRoute } from "@tanstack/react-router";
import { DhtPage } from "@/features/dht/page";

export const Route = createFileRoute("/dht/")({ component: DhtPage });
