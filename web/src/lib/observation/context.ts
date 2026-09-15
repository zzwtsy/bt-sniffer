import type { MonitorEngine } from "./engine";
import { createContext, use, useEffect, useSyncExternalStore } from "react";
import { encodedBytes } from "./contracts";

export const EngineContext = createContext<MonitorEngine | null>(null);
export function useEngine() {
  const engine = use(EngineContext);
  if (!engine)
    throw new Error("缺少观测 Provider");
  return engine;
}
export function useMonitor() {
  const engine = useEngine();
  return useSyncExternalStore(engine.subscribe, engine.getSnapshot);
}

/** 只冻结已挂载视图的数据，退出页面注销；超过预算时拒绝冻结而不保留部分画面。 */
export class DisplayStore {
  private live = new Map<string, unknown>();
  private saved = new Map<string, unknown>();
  private listeners = new Set<() => void>();
  private state = { frozen: false, at: 0, error: "" };
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = () => this.state;
  register(key: string, value: unknown) {
    this.live.set(key, value);
  }

  unregister(key: string) {
    this.live.delete(key);
  }

  value<T>(key: string, live: T): T {
    return this.state.frozen && this.saved.has(key)
      ? (this.saved.get(key) as T)
      : live;
  }

  toggle = () => {
    if (this.state.frozen) {
      this.resume();
      return;
    }
    const entries = [...this.live];
    if (encodedBytes(entries) > 2 * 1024 * 1024) {
      this.state = {
        ...this.state,
        error: "当前画面超过 2 MiB 冻结预算，请缩小查看范围",
      };
    } else {
      this.saved = new Map(structuredClone(entries));
      this.state = { frozen: true, at: Date.now(), error: "" };
    }
    this.notify();
  };

  resume = () => {
    this.saved.clear();
    this.state = { frozen: false, at: 0, error: "" };
    this.notify();
  };

  private notify() {
    for (const listener of this.listeners) listener();
  }
}
export const DisplayContext = createContext<DisplayStore | null>(null);
export function useDisplay() {
  const store = use(DisplayContext);
  if (!store)
    throw new Error("缺少显示 Provider");
  const state = useSyncExternalStore(store.subscribe, store.getSnapshot);
  return { store, ...state };
}
export function useDisplayed<T>(key: string, value: T): T {
  const { store } = useDisplay();
  useEffect(() => {
    store.register(key, value);
    return () => store.unregister(key);
  }, [key, store, value]);
  return store.value(key, value);
}
