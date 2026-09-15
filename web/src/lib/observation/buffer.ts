import type { ObservationEvent } from "./contracts";
import { encodedBytes } from "./contracts";

interface Entry {
  event: ObservationEvent;
  bytes: number;
  received: number;
}
export interface EventFilter {
  hash?: string;
  object?: string;
  kind?: string;
}
/** 所有历史页与实时事件共用记录；索引不额外持有事件对象。 */
export class EventBuffer {
  private entries = new Map<string, Entry>();
  private order: string[] = [];
  private hashes = new Map<string, Set<string>>();
  private objects = new Map<string, Set<string>>();
  bytes = 0;
  evicted = 0;
  readonly limits: { count: number; bytes: number; age: number };
  constructor(limits = { count: 5000, bytes: 8 * 1024 * 1024, age: 900_000 }) {
    this.limits = limits;
  }

  get size() {
    return this.entries.size;
  }

  get indexEntries() {
    return [...this.hashes.values(), ...this.objects.values()].reduce(
      (n, ids) => n + ids.size,
      0,
    );
  }

  private objectIds(event: ObservationEvent) {
    const c = event.context;
    return [
      ...new Set(
        [
          c.observation_id,
          c.batch_id,
          c.span_id,
          c.parent_span_id,
          c.peer_attempt_id,
          c.rpc_id,
        ].filter((v): v is string => typeof v === "string"),
      ),
    ];
  }

  add(events: ObservationEvent[], now = Date.now()) {
    for (const event of events) {
      if (this.entries.has(event.sequence))
        continue;
      const bytes = encodedBytes(event);
      this.entries.set(event.sequence, { event, bytes, received: now });
      this.order.push(event.sequence);
      this.bytes += bytes;
      if (event.context.hash != null && event.context.hash !== "")
        this.index(this.hashes, event.context.hash, event.sequence);
      for (const id of this.objectIds(event))
        this.index(this.objects, id, event.sequence);
    }
    this.order.sort((a, b) =>
      BigInt(a) < BigInt(b) ? -1 : BigInt(a) > BigInt(b) ? 1 : 0,
    );
    this.prune(now);
  }

  private index(index: Map<string, Set<string>>, id: string, sequence: string) {
    let set = index.get(id);
    if (!set) {
      set = new Set();
      index.set(id, set);
    }
    set.add(sequence);
  }

  private remove(id: string) {
    const entry = this.entries.get(id);
    if (!entry)
      return;
    this.entries.delete(id);
    this.bytes -= entry.bytes;
    this.evicted++;
    const removeIndex = (index: Map<string, Set<string>>, key: string) => {
      const set = index.get(key);
      set?.delete(id);
      if (set?.size === 0)
        index.delete(key);
    };
    if (entry.event.context.hash !== undefined && entry.event.context.hash !== "")
      removeIndex(this.hashes, entry.event.context.hash);
    for (const key of this.objectIds(entry.event))
      removeIndex(this.objects, key);
  }

  prune(now = Date.now(), oldest?: string) {
    this.order = this.order.filter((id) => {
      const entry = this.entries.get(id)!;
      if (
        now - entry.received >= this.limits.age
        || (oldest !== undefined && BigInt(id) < BigInt(oldest))
      ) {
        this.remove(id);
        return false;
      }
      return true;
    });
    while (this.size > this.limits.count || this.bytes > this.limits.bytes)
      this.remove(this.order.shift()!);
  }

  select(filter: EventFilter = {}, limit = 100): ObservationEvent[] {
    const allowed = (filter.hash != null && filter.hash !== "")
      ? this.hashes.get(filter.hash)
      : (filter.object != null && filter.object !== "")
          ? this.objects.get(filter.object)
          : undefined;
    if (((filter.hash != null && filter.hash !== "") || (filter.object != null && filter.object !== "")) && !allowed)
      return [];
    const result: ObservationEvent[] = [];
    for (let i = this.order.length - 1; i >= 0 && result.length < limit; i--) {
      const id = this.order[i];
      if (allowed && !allowed.has(id))
        continue;
      const event = this.entries.get(id)!.event;
      if ((filter.object != null && filter.object !== "") && !this.objectIds(event).includes(filter.object))
        continue;
      if ((Boolean(filter.kind)) && event.kind !== filter.kind)
        continue;
      result.push(event);
    }
    return result.reverse();
  }

  get(ids: string[]) {
    return ids.flatMap((id) => {
      const value = this.entries.get(id);
      return value ? [value.event] : [];
    });
  }

  clear() {
    this.entries.clear();
    this.order = [];
    this.hashes.clear();
    this.objects.clear();
    this.bytes = 0;
    this.evicted = 0;
  }
}
