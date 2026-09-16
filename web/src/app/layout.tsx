import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import {
  Activity,
  Boxes,
  Database,
  Fingerprint,
  Globe2,
  ListTree,
  Menu,
  Network,
  Pause,
  Play,
  Radio,
  RefreshCw,
  SunMoon,
} from "lucide-react";
import { useEffect, useState } from "react";
import { Status } from "@/components/observation/common";
import { useTheme } from "@/components/theme-provider";
import { Alert, AlertAction, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  NativeSelect,
  NativeSelectOption,
} from "@/components/ui/native-select";
import { useDisplay, useEngine, useMonitor } from "@/lib/observation/context";
import { source } from "@/lib/observation/contracts";
import { time } from "@/lib/observation/format";

const navigation = [
  { to: "/", title: "流程总览", icon: Activity },
  { to: "/dht", title: "DHT 网络", icon: Globe2 },
  { to: "/discoveries", title: "发现记录", icon: Radio },
  { to: "/jobs", title: "采集任务", icon: Boxes },
  { to: "/hashes", title: "Hash 索引", icon: Fingerprint },
  { to: "/metadata", title: "Metadata", icon: Database },
  { to: "/events", title: "事件浏览", icon: ListTree },
];
export function Layout() {
  const monitor = useMonitor();
  const engine = useEngine();
  const display = useDisplay();
  const theme = useTheme();
  const location = useRouterState({ select: s => s.location.href });
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
      <a className="skip-link" href="#main-content">
        跳到主要内容
      </a>
      <div className="app-shell">
        <aside className={`sidebar ${open ? "mobile-open" : ""}`}>
          <Link to="/" className="brand">
            <span className="brand-mark">
              <Network size={23} />
            </span>
            <span>
              bt-sniffer
              <small>DISCOVERY OBSERVATORY</small>
            </span>
          </Link>
          <p className="nav-caption">工作空间</p>
          <nav aria-label="主导航">
            {navigation.map(({ to, title, icon: Icon }) => (
              <Link
                key={to}
                to={to}
                activeOptions={{ exact: to === "/" }}
                activeProps={{ "className": "selected", "aria-current": "page" }}
                onClick={() => setOpen(false)}
              >
                <Icon size={18} />
                <span>{title}</span>
              </Link>
            ))}
          </nav>
          <div className="sidebar-footer">
            <Status value="只读观测" />
            <p>
              当前过程 · 有限历史
              <br />
              数据库事实 · 持久保留
            </p>
          </div>
        </aside>
        <div className="workspace">
          <header className="topbar">
            <div className="row">
              <Button
                variant="ghost"
                size="icon"
                className="mobile-toggle"
                aria-label="展开导航"
                aria-expanded={open}
                onClick={() => setOpen(!open)}
              >
                <Menu />
              </Button>
              <span
                className={`connection-dot ${monitor.phase === "live" ? "connected" : ""}`}
                aria-hidden="true"
              />
              <strong>{phase}</strong>
              <span className="run-label">
                运行
                {monitor.snapshot?.window.run_id.slice(0, 12) ?? "—"}
              </span>
            </div>
            <div className="row">
              <Button
                variant="ghost"
                size="sm"
                aria-pressed={display.frozen}
                onClick={display.store.toggle}
              >
                {display.frozen ? <Play /> : <Pause />}
                {display.frozen ? "恢复实时显示" : "冻结显示"}
              </Button>
              <label className="theme-picker">
                <SunMoon size={17} />
                <NativeSelect
                  size="sm"
                  aria-label="主题"
                  value={theme.theme}
                  onChange={e =>
                    theme.setTheme(
                      e.target.value as "light" | "dark" | "system",
                    )}
                >
                  <NativeSelectOption value="system">跟随系统</NativeSelectOption>
                  <NativeSelectOption value="light">浅色</NativeSelectOption>
                  <NativeSelectOption value="dark">深色</NativeSelectOption>
                </NativeSelect>
              </label>
            </div>
          </header>
          <main id="main-content">
            {source(monitor.snapshot, "monitor").phase === "shutting_down" && (
              <Alert className="mb-4">
                <AlertDescription>
                  服务正在关闭，当前结果属于最后观察窗口。
                </AlertDescription>
              </Alert>
            )}
            {display.frozen && (
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
          <footer className="page-footer">
            <span>BT-SNIFFER / READ-ONLY MONITOR</span>
            <span>不同数据源独立观察 · 未知值不等于零</span>
          </footer>
        </div>
      </div>
    </>
  );
}
