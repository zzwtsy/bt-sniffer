import { z } from "zod";

export type Fields = Record<string, unknown>;
export const fieldsSchema = z.record(z.string(), z.unknown());
const sequence = z
  .string()
  .regex(/^\d+$/)
  .refine(v => BigInt(v) <= 18446744073709551615n);
const millis = z.number().nonnegative().finite();
export const windowSchema = z
  .object({
    run_id: z.string().min(1),
    oldest: sequence,
    latest: sequence,
    retained: z.number().int().nonnegative(),
    bytes: z.number().nonnegative(),
    evicted: z.number().nonnegative(),
    truncated: z.number().nonnegative(),
  })
  .passthrough();
export const contextSchema = z
  .object({
    hash: z.string().optional(),
    generation: z.number().int().safe().optional(),
    node_id: z.string().optional(),
    observation_id: z.string().optional(),
    batch_id: z.string().optional(),
    span_id: z.string().optional(),
    parent_span_id: z.string().optional(),
    peer_attempt_id: z.string().optional(),
    rpc_id: z.string().optional(),
  })
  .passthrough();
export const eventSchema = z
  .object({
    schema_version: z.literal(1),
    run_id: z.string(),
    sequence,
    at_ms: millis,
    kind: z.string(),
    step: z.string(),
    result: z.string(),
    context: contextSchema,
    data: z.unknown(),
    truncated: z.boolean(),
  })
  .passthrough();
export const snapshotSchema = z.object({
  schema_version: z.literal(1),
  window: windowSchema,
  runtime: z.object({
    sources: z.record(
      z.string(),
      z.object({ observed_at_ms: millis, value: z.unknown() }),
    ),
    active: z.array(fieldsSchema),
  }),
  cached: fieldsSchema,
});
export const eventPageSchema = z.object({
  events: z.array(eventSchema).max(100),
  next: sequence,
  window: windowSchema,
  completeness: z.enum(["complete", "partial", "unavailable"]),
});
export type ObservationEvent = z.infer<typeof eventSchema>;
export type Snapshot = z.infer<typeof snapshotSchema>;
export type ObservationWindow = z.infer<typeof windowSchema>;
export type EventPage = z.infer<typeof eventPageSchema>;
export function record(value: unknown): Fields {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Fields)
    : {};
}
export function rows(value: unknown): Fields[] {
  return Array.isArray(value) ? value.map(record) : [];
}
export function number(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}
export function string(value: unknown): string {
  return typeof value === "string" ? value : "未知";
}
export function source(snapshot: Snapshot | undefined, name: string): Fields {
  return record(snapshot?.runtime.sources[name]?.value);
}
export function encodedBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}
