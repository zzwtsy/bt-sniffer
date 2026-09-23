import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import { cn } from "cn";
import {
  Activity,
  Menu,
  Network,
  Pause,
  Play,
  RefreshCw,
  Search,
} from "lucide-react";
import { useEffect, useState } from "react";
import { ModeToggle } from "@/components/mode-toggle";
import { Status } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { useDisplay, useEngine, useMonitor } from "@/lib/observation/context";
import { source } from "@/lib/observation/contracts";
import { time } from "@/lib/observation/format";

const navigation = [
  { to: "/", title: "流程总览", icon: Activity },
  { to: "/torrents", title: "种子查询", icon: Search },
];
export function Layout() {
  const monitor = useMonitor();
  const engine = useEngine();
  const display = useDisplay();
  const location = useRouterState({ select: s => s.location.pathname });
  const [open, setOpen] = useState(false);
  useEffect(() => {
    display.store.resume();
  }, [location, display.store]);
  const phase = {
    connecting: "首次同步中",
    live: "实时连接",
    reconnecting: "正在重连",
    unavailable: "同步不可用",
    stopped: "连接已停止",
  }[monitor.phase];
  return (
    <>
      <a
        className="absolute left-[-9999px] focus:left-4 focus:top-4 focus:z-100 focus:bg-card focus:p-3"
        href="#main-content"
      >
        跳到主要内容
      </a>
      <div className="grid h-screen grid-cols-[224px_minmax(0,1fr)] bg-[color-mix(in_oklch,var(--background)_96%,var(--muted-foreground))] max-[1200px]:grid-cols-[192px_minmax(0,1fr)] max-[760px]:block">
        <aside
          className={cn(
            "sticky top-0 flex h-screen flex-col border-r bg-card px-4.5 pt-7 pb-5 max-[760px]:hidden",
            open
            && "max-[760px]:fixed max-[760px]:z-20 max-[760px]:flex max-[760px]:w-56 max-[760px]:shadow-[0_0_0_100vw_#0005]",
          )}
        >
          <Link
            to="/"
            className="flex items-center gap-2.5 text-[19px] font-bold [letter-spacing:-0.5px] text-foreground hover:no-underline"
          >
            <span className="grid size-9.5 place-items-center rounded-[9px] bg-primary text-primary-foreground">
              <Network size={23} />
            </span>
            <span>
              bt-sniffer
              <small className="block text-[8px] font-[550] tracking-[1.25px] text-muted-foreground">
                DISCOVERY OBSERVATORY
              </small>
            </span>
          </Link>
          <p className="mx-3 mt-9.5 mb-3 text-[11px] tracking-[1px] text-muted-foreground">
            工作空间
          </p>
          <nav aria-label="主导航" className="flex flex-col gap-1.25">
            {navigation.map(({ to, title, icon: Icon }) => (
              <Link
                key={to}
                to={to}
                activeOptions={{ exact: to === "/" }}
                className="flex items-center gap-3 rounded-md px-3 py-2.75 text-[13px] font-medium hover:bg-muted hover:no-underline"
                activeProps={{
                  "className": "bg-primary/9 font-semibold text-primary",
                  "aria-current": "page",
                }}
                inactiveProps={{ className: "text-muted-foreground" }}
                onClick={() => setOpen(false)}
              >
                <Icon size={18} />
                <span>{title}</span>
              </Link>
            ))}
          </nav>
          <div className="mt-auto border-t px-3 pt-5 text-[11px] text-muted-foreground">
            <Status value="只读观测" />
            <p>
              界面只读；数据来自进程快照与本地数据库。
            </p>
          </div>
        </aside>
        <div className="flex h-full min-h-0 min-w-0 flex-col">
          <header className="flex h-17 shrink-0 items-center justify-between gap-3 border-b bg-card px-8 max-[1200px]:px-6 max-[760px]:h-auto max-[760px]:min-h-16 max-[760px]:flex-wrap max-[760px]:px-4 max-[760px]:py-3">
            <div className="flex flex-wrap items-center gap-2.5">
              <Button
                variant="ghost"
                size="icon"
                className="hidden max-[760px]:inline-grid"
                aria-label="展开导航"
                aria-expanded={open}
                onClick={() => setOpen(!open)}
              >
                <Menu />
              </Button>
              <span
                className={cn(
                  "size-1.75 rounded-full bg-muted-foreground",
                  monitor.phase === "live" && "bg-connected ring-4 ring-connected/7",
                )}
                aria-hidden="true"
              />
              <strong className="text-xs font-[550]">{phase}</strong>
              <span
                className="border-l pl-3 font-mono text-[11px] text-muted-foreground max-[1200px]:hidden"
                title="后端本次启动的运行 ID，重启后变化"
              >
                运行
                {" "}
                {monitor.snapshot?.window.run_id.slice(0, 12) ?? "—"}
              </span>
            </div>
            <div className="flex flex-wrap items-center gap-2.5">
              {location === "/" && (
                <Button
                  variant="ghost"
                  size="sm"
                  aria-pressed={display.frozen}
                  onClick={display.store.toggle}
                >
                  {display.frozen ? <Play /> : <Pause />}
                  {display.frozen ? "恢复实时显示" : "冻结显示"}
                </Button>
              )}
              <ModeToggle />
            </div>
          </header>
          <main
            id="main-content"
            className="mx-auto min-h-0 w-full max-w-[1600px] flex-1 overflow-y-auto p-8 max-[1200px]:p-6 max-[760px]:px-4 min-[1700px]:pt-10.5"
          >
            {source(monitor.snapshot, "monitor").phase === "shutting_down" && (
              <Alert className="mb-4">
                <AlertDescription>
                  服务正在关闭，当前结果属于最后观察窗口。
                </AlertDescription>
              </Alert>
            )}
            {location === "/" && display.frozen && (
              <Alert className="mb-4">
                <AlertDescription>
                  画面已冻结于
                  {time(display.at)}
                  ，采集不受影响。恢复后显示最新状态。
                </AlertDescription>
              </Alert>
            )}
            {display.error && (
              <Alert variant="destructive" className="mb-4">
                <AlertDescription>{display.error}</AlertDescription>
              </Alert>
            )}
            {(monitor.message != null && monitor.message !== "") && (
              <Alert variant="destructive" className="mb-4">
                <AlertDescription>{monitor.message}</AlertDescription>
                <AlertAction>
                  <Button variant="outline" size="sm" onClick={engine.retry}>
                    <RefreshCw />
                    重新同步
                  </Button>
                </AlertAction>
              </Alert>
            )}
            <Outlet />
          </main>
          <footer className="flex shrink-0 justify-between gap-4 px-8 py-4 text-[9px] tracking-[1px] text-muted-foreground max-[760px]:flex-wrap max-[760px]:p-4">
            <span>BT-SNIFFER / READ-ONLY MONITOR</span>
          </footer>
        </div>
      </div>
    </>
  );
}
