import type { ComponentProps, ReactNode } from "react";
import { cn } from "cn";
import { Info } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  EmptyHeader,
  Empty as EmptyRoot,
  EmptyTitle,
} from "@/components/ui/empty";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { label, time } from "@/lib/observation/format";

/** 口径说明入口：hover/focus 信息图标时以 Tooltip 展示，不占正文版面。 */
export function Hint({ text }: { text: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger
          delay={0}
          aria-label="口径说明"
          className="inline-flex cursor-help items-center text-muted-foreground/70 hover:text-muted-foreground"
        >
          <Info size={13} aria-hidden="true" />
        </TooltipTrigger>
        <TooltipContent>{text}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

export function Panel({
  title,
  description,
  hint,
  children,
  action,
  className = "",
  contentClassName = "",
}: {
  title: string;
  description?: string;
  hint?: string;
  children: ReactNode;
  action?: ReactNode;
  className?: string;
  contentClassName?: string;
}) {
  return (
    <Card className={cn("mb-5", className)}>
      <CardHeader>
        <CardTitle>
          <span className="inline-flex items-center gap-1.5">
            {title}
            {hint !== undefined && hint !== "" && <Hint text={hint} />}
          </span>
        </CardTitle>
        {description !== undefined && description !== "" && (
          <CardDescription>{description}</CardDescription>
        )}
        {action !== undefined && <CardAction>{action}</CardAction>}
      </CardHeader>
      <CardContent className={contentClassName}>{children}</CardContent>
    </Card>
  );
}
export function PageTitle({
  eyebrow,
  title,
  description,
  children,
}: {
  eyebrow: string;
  title: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <div className="mb-6.5 flex items-center justify-between gap-4">
      <div>
        <p className="m-0 mb-2 text-[10px] font-[650] tracking-[2px] text-primary">
          {eyebrow}
        </p>
        <h1 className="max-[760px]:text-[25px]">{title}</h1>
        <p className="mt-2.5 text-xs text-muted-foreground">{description}</p>
      </div>
      {children}
    </div>
  );
}
const statusTones: Record<string, string> = {
  danger: "border-transparent bg-status-danger-bg text-status-danger",
  warning: "border-transparent bg-status-warning-bg text-status-warning",
  success: "border-transparent bg-status-success-bg text-status-success",
  active: "border-transparent bg-primary/10 text-primary",
  neutral: "border-transparent bg-muted text-foreground",
};
export function Status({ value }: { value: unknown }) {
  const raw = typeof value === "string" ? value : "unknown";
  const tone = /failed|error|invalid|mismatch|timeout/.test(raw)
    ? "danger"
    : /wait|pause|capacity|stale|dormant/.test(raw)
      ? "warning"
      : /applied|succeeded|validated|ready|good/.test(raw)
        ? "success"
        : /running|started|sent/.test(raw)
          ? "active"
          : "neutral";
  return (
    <Badge variant="outline" className={cn("status", statusTones[tone])}>
      <span aria-hidden="true" className="size-1.5 rounded-full bg-current" />
      {label(value)}
    </Badge>
  );
}
/** 胶囊筛选按钮：选中态高亮边框与底色，用于泳道过滤、状态图例与链路直选。 */
export function Chip({
  selected = false,
  className,
  type = "button",
  ...props
}: { selected?: boolean } & ComponentProps<"button">) {
  return (
    <button
      type={type}
      aria-pressed={selected}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border bg-card px-2.5 py-0.75 text-[11px] text-card-foreground hover:border-primary focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring",
        selected && "border-primary bg-primary/12",
        className,
      )}
      {...props}
    />
  );
}
export function Empty({
  children = "当前没有记录。数据缺失不代表相关过程未发生。",
}: {
  children?: ReactNode;
}) {
  return (
    <EmptyRoot>
      <EmptyHeader>
        <EmptyTitle className="font-normal text-muted-foreground">
          {children}
        </EmptyTitle>
      </EmptyHeader>
    </EmptyRoot>
  );
}
export function Freshness({ at, stale, queried = false, source }: { at?: unknown; stale?: boolean; queried?: boolean; source?: string }) {
  const prefix = source ?? (stale ? "陈旧数据" : queried ? "查询时间" : "观察时间");
  return (
    <span
      className={cn(
        "text-xs text-muted-foreground",
        stale && "text-status-warning",
      )}
    >
      {prefix}
      {source !== undefined && source !== "" && stale ? " · 陈旧" : ""}
      {" · "}
      {time(at)}
    </span>
  );
}
export function Metric({
  title,
  value,
  detail,
  hint,
  icon,
}: {
  title: string;
  value: string;
  detail: string;
  hint?: string;
  icon: ReactNode;
}) {
  return (
    <Card size="sm" className="metric">
      <CardContent>
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span className="inline-flex items-center gap-1">
            {title}
            {hint !== undefined && hint !== "" && <Hint text={hint} />}
          </span>
          {icon}
        </div>
        <div className="metric-value mt-2 text-2xl font-semibold tracking-tight tabular-nums">
          {value}
        </div>
        <div className="mt-1 text-xs text-muted-foreground">{detail}</div>
      </CardContent>
    </Card>
  );
}
