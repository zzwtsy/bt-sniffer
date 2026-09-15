import { createFileRoute } from "@tanstack/react-router";
import { MetadataPage } from "@/features/metadata/page";

export const Route = createFileRoute("/metadata")({ component: MetadataPage });
