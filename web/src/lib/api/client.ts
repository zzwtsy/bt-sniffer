import type { ZodType } from "zod";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}
interface Task {
  key: string;
  database: boolean;
  priority: number;
  controller: AbortController;
  promise: Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
  work: (signal: AbortSignal) => Promise<unknown>;
  consumers: number;
  running: boolean;
}
/** 请求许可直到 fetch 实际结束才回收；取消一个订阅者不会取消其他订阅者。 */
export class RequestScheduler {
  private tasks = new Map<string, Task>();
  private queue: Task[] = [];
  private active = 0;
  private databaseActive = 0;
  private tokens = 5;
  private updated = Date.now();
  private timer?: ReturnType<typeof setTimeout>;
  get stats() {
    return {
      active: this.active,
      database: this.databaseActive,
      queued: this.queue.length,
    };
  }

  async request<T>(
    key: string,
    database: boolean,
    signal: AbortSignal,
    work: (signal: AbortSignal) => Promise<T>,
    priority = 1,
  ): Promise<T> {
    if (signal.aborted)
      return Promise.reject(new DOMException("请求已取消", "AbortError"));
    let task = this.tasks.get(key);
    if (task?.controller.signal.aborted)
      task = undefined;
    if (!task) {
      if (this.queue.length >= 20) {
        return Promise.reject(
          new ApiError(429, "client_capacity", "请求队列已满，请稍后重试"),
        );
      }
      let resolve!: Task["resolve"];
      let reject!: Task["reject"];
      const promise = new Promise<unknown>((ok, fail) => {
        resolve = ok;
        reject = fail;
      });
      task = {
        key,
        database,
        priority,
        controller: new AbortController(),
        promise,
        resolve,
        reject,
        work,
        consumers: 0,
        running: false,
      };
      this.tasks.set(key, task);
      this.queue.push(task);
    }
    const shared = task;
    shared.consumers++;
    const result = new Promise<T>((resolve, reject) => {
      let finished = false;
      const release = () => {
        if (finished)
          return;
        finished = true;
        signal.removeEventListener("abort", abort);
        shared.consumers--;
        if (shared.consumers === 0) {
          shared.controller.abort();
          this.drain();
        }
      };
      function abort() {
        release();
        reject(new DOMException("请求已取消", "AbortError"));
      }
      signal.addEventListener("abort", abort, { once: true });
      shared.promise.then(
        (v) => {
          if (!finished) {
            release();
            resolve(v as T);
          }
        },
        (e) => {
          if (!finished) {
            release();
            reject(e);
          }
        },
      );
    });
    this.drain();
    return result;
  }

  cancelAll() {
    if (this.timer != null)
      clearTimeout(this.timer);
    this.timer = undefined;
    for (const task of this.tasks.values()) task.controller.abort();
    this.drain();
  }

  private drain() {
    if (this.timer != null) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    const now = Date.now();
    this.tokens = Math.min(
      5,
      this.tokens + Math.max(0, now - this.updated) / 200,
    );
    this.updated = now;
    this.queue = this.queue
      .filter((task) => {
        if (!task.controller.signal.aborted)
          return true;
        task.reject(new DOMException("请求已取消", "AbortError"));
        if (this.tasks.get(task.key) === task)
          this.tasks.delete(task.key);
        return false;
      })
      .sort((a, b) => a.priority - b.priority);
    while (this.active < 2 && this.tokens >= 1) {
      const index = this.queue.findIndex(
        t => !t.database || this.databaseActive === 0,
      );
      if (index < 0)
        break;
      const task = this.queue.splice(index, 1)[0];
      this.tokens--;
      this.active++;
      if (task.database)
        this.databaseActive++;
      task.running = true;
      void Promise.resolve()
        .then(async () => task.work(task.controller.signal))
        .then(task.resolve, task.reject)
        .finally(() => {
          this.active--;
          if (task.database)
            this.databaseActive--;
          if (this.tasks.get(task.key) === task)
            this.tasks.delete(task.key);
          this.drain();
        });
    }
    if ((this.queue.length > 0) && this.active < 2 && this.tokens < 1) {
      this.timer = setTimeout(
        () => this.drain(),
        Math.ceil((1 - this.tokens) * 200),
      );
    }
  }
}
export const scheduler = new RequestScheduler();
export async function read<T>(
  path: string,
  schema: ZodType<T>,
  signal: AbortSignal,
  database = false,
  priority = 1,
): Promise<T> {
  return scheduler.request(
    path,
    database,
    signal,
    async (abort) => {
      const timeout = AbortSignal.timeout(5000);
      let response: Response;
      try {
        response = await fetch(`/api/v1${path}`, {
          signal: AbortSignal.any([abort, timeout]),
          headers: { Accept: "application/json" },
        });
      } catch (error) {
        if (abort.aborted)
          throw error;
        throw new ApiError(0, "network", "连接失败或请求超时");
      }
      const body: unknown = await response.json().catch(() => null);
      if (!response.ok) {
        const error
          = (body !== null) && typeof body === "object" && "error" in body
            ? (body.error as { code?: string; message?: string })
            : {};
        throw new ApiError(
          response.status,
          error.code ?? "http_error",
          error.message ?? `请求失败（${response.status}）`,
        );
      }
      const parsed = schema.safeParse(body);
      if (!parsed.success) {
        throw new ApiError(
          422,
          "incompatible",
          "响应格式不兼容，无法安全展示数据",
        );
      }
      return parsed.data;
    },
    priority,
  );
}
export function retry(failures: number, error: Error): boolean {
  return (
    failures < 2
    && error instanceof ApiError
    && [0, 429, 503, 504].includes(error.status)
  );
}
export function queryString(
  values: Record<string, string | number | undefined>,
): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined && value !== "")
      params.set(key, String(value));
  }
  return (params.size !== 0) ? `?${params}` : "";
}
