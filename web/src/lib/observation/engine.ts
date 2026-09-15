import type { QueryClient } from "@tanstack/react-query";
import type { EventPage, Snapshot } from "./contracts";
import { ApiError, read, scheduler } from "../api/client";
import { EventBuffer } from "./buffer";
import { eventPageSchema, snapshotSchema, windowSchema } from "./contracts";
import { Trends } from "./trends";

export type Phase
  = "connecting" | "live" | "reconnecting" | "unavailable" | "stopped";
export interface MonitorView {
  phase: Phase;
  message?: string;
  snapshot?: Snapshot;
  revision: number;
  epoch: number;
  events: number;
  bytes: number;
  evicted: number;
}
/** 一次挂载一个连接所有者；所有异步回调同时检查生命周期代次。 */
export class MonitorEngine {
  readonly buffer = new EventBuffer();
  readonly trends = new Trends();
  private listeners = new Set<() => void>();
  private view: MonitorView = {
    phase: "connecting",
    revision: 0,
    epoch: 0,
    events: 0,
    bytes: 0,
    evicted: 0,
  };

  private snapshot?: Snapshot;
  private connection?: EventSource;
  private abort?: AbortController;
  private timer?: ReturnType<typeof setTimeout>;
  private maintenance?: ReturnType<typeof setInterval>;
  private publishTimer?: ReturnType<typeof setTimeout>;
  private generation = 0;
  private stopped = true;
  private cursor = "0";
  private attempt = 0;
  private queries: QueryClient;
  private eventSource: (url: string) => EventSource;
  constructor(
    queries: QueryClient,
    eventSource: (url: string) => EventSource = url => new EventSource(url),
  ) {
    this.queries = queries;
    this.eventSource = eventSource;
  }

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = () => this.view;
  private publish(patch: Partial<MonitorView> = {}) {
    this.view = {
      ...this.view,
      snapshot: this.snapshot,
      revision: this.view.revision + 1,
      events: this.buffer.size,
      bytes: this.buffer.bytes,
      evicted: this.buffer.evicted,
      ...patch,
    };
    for (const listener of this.listeners) listener();
  }

  private batchPublish() {
    this.publishTimer ??= setTimeout(() => {
      this.publishTimer = undefined;
      this.publish();
    }, 200);
  }

  start() {
    if (!this.stopped)
      return;
    this.maintenance = setInterval(() => {
      const before = this.buffer.size;
      this.buffer.prune();
      const expiredTrends = this.trends.prune();
      if (before !== this.buffer.size || expiredTrends)
        this.batchPublish();
    }, 1000);
    this.stopped = false;
    void this.sync();
  }

  stop() {
    this.stopped = true;
    if (this.maintenance !== undefined)
      clearInterval(this.maintenance);
    this.maintenance = undefined;
    this.generation++;
    this.connection?.close();
    this.connection = undefined;
    this.abort?.abort();
    if (this.timer != null)
      clearTimeout(this.timer);
    if (this.publishTimer != null)
      clearTimeout(this.publishTimer);
    this.timer = undefined;
    this.publishTimer = undefined;
    scheduler.cancelAll();
    this.queries.clear();
  }

  retry = () => {
    this.stop();
    this.attempt = 0;
    this.start();
  };

  private async sync() {
    const generation = ++this.generation;
    this.connection?.close();
    this.connection = undefined;
    this.abort?.abort();
    await this.queries.cancelQueries();
    if (this.stopped || generation !== this.generation)
      return;
    this.queries.clear();
    scheduler.cancelAll();
    this.buffer.clear();
    this.trends.clear();
    this.snapshot = undefined;
    this.publish({
      phase: "connecting",
      message: undefined,
      epoch: this.view.epoch + 1,
    });
    const abort = new AbortController();
    this.abort = abort;
    try {
      const snapshot = await read(
        "/snapshot",
        snapshotSchema,
        abort.signal,
        false,
        0,
      );
      if (this.stopped || generation !== this.generation)
        return;
      this.snapshot = snapshot;
      this.cursor = snapshot.window.latest;
      this.trends.add(snapshot);
      this.publish();
      this.connect(generation);
    } catch (error) {
      if (!this.stopped && generation === this.generation)
        this.failure(error, generation, true);
    }
  }

  private failure(error: unknown, generation: number, needsSnapshot = false) {
    this.connection?.close();
    this.connection = undefined;
    this.trends.gap();
    if (error instanceof ApiError && error.code === "incompatible") {
      this.publish({ phase: "unavailable", message: error.message });
      return;
    }
    this.publish({
      phase: "reconnecting",
      message: "监控连接中断，保留最后观察结果，正在重连",
    });
    const delay = Math.min(30_000, 1000 * 2 ** Math.min(this.attempt++, 5));
    this.timer = setTimeout(
      () => {
        this.timer = undefined;
        if (!this.stopped && generation === this.generation) {
          if (needsSnapshot)
            void this.sync();
          else this.connect(generation);
        }
      },
      Math.min(30_000, delay * (0.9 + Math.random() * 0.2)),
    );
  }

  private connect(generation: number) {
    if (!this.snapshot || this.stopped)
      return;
    const run = this.snapshot.window.run_id;
    const connection = this.eventSource(
      `/api/v1/stream?after=${encodeURIComponent(`${run}:${this.cursor}`)}`,
    );
    this.connection = connection;
    const current = () =>
      !this.stopped
      && generation === this.generation
      && this.connection === connection;
    const receive = (event: MessageEvent, action: (value: unknown) => void) => {
      if (!current())
        return;
      try {
        action(JSON.parse(String(event.data)));
      } catch {
        connection.close();
        this.connection = undefined;
        this.publish({
          phase: "unavailable",
          message: "事件格式不兼容，已停止自动同步",
        });
      }
    };
    connection.addEventListener("hello", event =>
      receive(event, (value) => {
        const window = windowSchema.parse(value);
        if (window.run_id !== run) {
          void this.sync();
          return;
        }
        this.publish({ phase: "live", message: undefined });
      }));
    connection.addEventListener("snapshot", event =>
      receive(event, (value) => {
        const snapshot = snapshotSchema.parse(value);
        if (snapshot.window.run_id !== run) {
          void this.sync();
          return;
        }
        this.attempt = 0;
        this.snapshot = snapshot;
        this.buffer.prune(Date.now(), snapshot.window.oldest);
        this.trends.add(snapshot);
        this.batchPublish();
      }));
    connection.addEventListener("events", event =>
      receive(event, (value) => {
        const page = eventPageSchema.parse(value);
        if (
          page.window.run_id !== run
          || page.events.some(e => e.run_id !== run)
        ) {
          void this.sync();
          return;
        }
        if (BigInt(page.next) < BigInt(this.cursor))
          return;
        if (page.events.some(e => BigInt(e.sequence) > BigInt(page.next)))
          throw new Error("无效事件游标");
        this.attempt = 0;
        this.acceptHistory(page);
        this.cursor = page.next;
      }));
    connection.addEventListener("reset", event =>
      receive(event, (value) => {
        connection.close();
        this.connection = undefined;
        if (
          (value !== null)
          && typeof value === "object"
          && "reason" in value
          && value.reason === "response_capacity"
        ) {
          this.publish({
            phase: "unavailable",
            message: "监控响应超过容量，自动同步已停止",
          });
        } else {
          windowSchema.parse(value);
          void this.sync();
        }
      }));
    connection.onerror = () => {
      if (current())
        this.failure(undefined, generation);
    };
  }

  acceptHistory(page: EventPage) {
    if (page.window.run_id !== this.snapshot?.window.run_id)
      return;
    this.buffer.add(page.events);
    this.buffer.prune(Date.now(), page.window.oldest);
    this.batchPublish();
  }
}
