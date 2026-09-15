import type { ZodType } from "zod";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import {
  useDisplay,
  useDisplayed,
  useEngine,
  useMonitor,
} from "../observation/context";
import { eventPageSchema } from "../observation/contracts";
import { read } from "./client";

export function useRead<T>(
  path: string,
  schema: ZodType<T>,
  database = false,
  enabled = true,
  group = path.split("?")[0],
) {
  const { epoch, snapshot, phase } = useMonitor();
  const { frozen } = useDisplay();
  const client = useQueryClient();
  const query = useQuery({
    queryKey: ["api", epoch, group, path],
    queryFn: async ({ signal }) => read(path, schema, signal, database),
    enabled: enabled && !!snapshot && phase !== "unavailable" && !frozen,
    staleTime: 5000,
    gcTime: 60_000,
    refetchInterval: frozen ? false : 5000,
    refetchIntervalInBackground: false,
  });
  useEffect(() => {
    const pages = client
      .getQueryCache()
      .findAll({ queryKey: ["api", epoch, group] })
      .sort((a, b) => b.state.dataUpdatedAt - a.state.dataUpdatedAt);
    for (const page of pages.slice(3)) {
      if (page.getObserversCount() === 0)
        client.removeQueries({ queryKey: page.queryKey, exact: true });
    }
  }, [client, epoch, group, query.dataUpdatedAt]);
  const visible = useDisplayed(`query:${group}`, {
    data: query.data,
    at: query.dataUpdatedAt,
  });
  return { ...query, data: visible.data, dataUpdatedAt: visible.at };
}
/** Query 仅保存事件序号和页元信息；原始事件进入唯一有界缓存。 */
export function useHistory(path: string) {
  const engine = useEngine();
  const { epoch, snapshot, revision, phase } = useMonitor();
  const { frozen } = useDisplay();
  const query = useQuery({
    queryKey: ["history", epoch, path],
    queryFn: async ({ signal }) => {
      const page = await read(path, eventPageSchema, signal);
      if (engine.getSnapshot().epoch !== epoch)
        throw new DOMException("旧运行", "AbortError");
      engine.acceptHistory(page);
      return {
        ids: page.events.map(e => e.sequence),
        next: page.next,
        window: page.window,
        completeness: page.completeness,
      };
    },
    enabled: !!snapshot && !frozen && phase !== "unavailable",
    gcTime: 60_000,
    staleTime: 5000,
  });
  const client = useQueryClient();
  useEffect(() => {
    const pages = client
      .getQueryCache()
      .findAll({ queryKey: ["history", epoch] })
      .sort((a, b) => b.state.dataUpdatedAt - a.state.dataUpdatedAt);
    for (const page of pages.slice(3)) {
      if (page.getObserversCount() === 0)
        client.removeQueries({ queryKey: page.queryKey, exact: true });
    }
  }, [client, epoch, query.dataUpdatedAt]);
  // revision 表示共享缓存可能经历追加或淘汰。
  const visible = useDisplayed(`history:${path.split("?")[0]}`, {
    ...query.data,
    events: engine.buffer.get(query.data?.ids ?? []),
    revision,
  });
  return {
    ...query,
    page: visible,
    missing: (visible.ids?.length ?? 0) > visible.events.length,
  };
}
