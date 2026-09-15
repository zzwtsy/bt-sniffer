import { createFileRoute } from "@tanstack/react-router";
import { JobsPage } from "@/features/jobs/page";

export const Route = createFileRoute("/jobs")({ component: JobsPage });
