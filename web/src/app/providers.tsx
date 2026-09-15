import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { ThemeProvider } from "@/components/theme-provider";
import { retry } from "@/lib/api/client";
import {
  DisplayStore,
  EngineContext,
} from "@/lib/observation/context";
import { DisplayProvider } from "@/lib/observation/display-provider";
import { MonitorEngine } from "@/lib/observation/engine";

export function Providers({ children }: { children: ReactNode }) {
  const [resources] = useState(() => {
    const queries = new QueryClient({
      defaultOptions: {
        queries: {
          retry,
          retryDelay: n => Math.min(30_000, 1000 * 2 ** n),
          refetchOnWindowFocus: false,
          refetchOnReconnect: false,
        },
      },
    });
    return {
      queries,
      engine: new MonitorEngine(queries),
      display: new DisplayStore(),
    };
  });
  useEffect(() => {
    resources.engine.start();
    return () => resources.engine.stop();
  }, [resources]);
  return (
    <ThemeProvider>
      <QueryClientProvider client={resources.queries}>
        <EngineContext value={resources.engine}>
          <DisplayProvider store={resources.display}>
            {children}
          </DisplayProvider>
        </EngineContext>
      </QueryClientProvider>
    </ThemeProvider>
  );
}
