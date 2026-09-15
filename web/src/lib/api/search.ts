import { useNavigate, useSearch } from "@tanstack/react-router";
import { z } from "zod";

export const searchSchema = z.object({
  after: z.string().max(256).optional().catch(undefined),
  trail: z.array(z.string().max(256)).max(50).optional().catch(undefined),
  limit: z.coerce
    .number()
    .refine(n => n === 50 || n === 100)
    .optional()
    .catch(undefined),
  state: z
    .enum(["pending", "running", "retry_wait", "dormant", "succeeded"])
    .optional()
    .catch(undefined),
  hash: z
    .string()
    .regex(/^[a-f\d]{40}$/i)
    .transform(v => v.toLowerCase())
    .optional()
    .catch(undefined),
  object: z.string().max(128).optional().catch(undefined),
  kind: z.string().max(32).optional().catch(undefined),
  generation: z.coerce
    .number()
    .int()
    .nonnegative()
    .safe()
    .optional()
    .catch(undefined),
  peer: z.string().max(128).optional().catch(undefined),
  mode: z.enum(["history", "live"]).optional().catch(undefined),
});
export type Search = z.infer<typeof searchSchema>;
export function usePageSearch() {
  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const change = (patch: Partial<Search>) => {
    void navigate({
      to: ".",
      search: { ...search, ...patch },
      resetScroll: false,
    });
  };
  return {
    search,
    change,
    limit: search.limit ?? 50,
    first: () => change({ after: undefined, trail: undefined }),
    next: (cursor: string) =>
      change({
        after: cursor,
        trail: [...(search.trail ?? []), search.after ?? ""].slice(-50),
      }),
    previous: () => {
      const trail = [...(search.trail ?? [])];
      const after = trail.pop();
      change({ after: after === "" ? undefined : after, trail });
    },
  };
}
