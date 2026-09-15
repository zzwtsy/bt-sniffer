import { createFileRoute } from "@tanstack/react-router";
import { HashesPage } from "@/features/hashes/page";

export const Route = createFileRoute("/hashes/")({ component: HashesPage });
