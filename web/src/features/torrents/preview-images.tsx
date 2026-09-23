import { AlertCircle, ChevronLeft, ChevronRight, ImageOff, Images } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Empty } from "@/components/observation/common";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { parsePreview, previewApiUrl } from "./preview";

type PreviewState
  = | { status: "idle" }
    | { status: "loading" }
    | { status: "success"; screenshots: string[] }
    | { status: "error"; message: string };

/** 详情页预览图：手动点击后浏览器直连 whatslink.info，不经内部请求调度；卸载即中止。 */
export function PreviewImages({ hash }: { hash: string }) {
  const [state, setState] = useState<PreviewState>({ status: "idle" });
  const [broken, setBroken] = useState<ReadonlySet<number>>(() => new Set());
  const [selected, setSelected] = useState<number | null>(null);
  const [scrollEdge, setScrollEdge] = useState({ left: false, right: false });
  const abortRef = useRef<AbortController | null>(null);
  const stripRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => () => abortRef.current?.abort(), []);

  async function load() {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setState({ status: "loading" });
    setBroken(new Set());
    setSelected(null);
    try {
      const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]);
      const response = await fetch(previewApiUrl(hash), { signal });
      if (!response.ok)
        throw new Error(`whatslink.info 返回 HTTP ${response.status}`);
      const preview = parsePreview(await response.json());
      setState({ status: "success", screenshots: preview.screenshots });
    } catch (error) {
      if (controller.signal.aborted)
        return;
      const message = error instanceof Error ? error.message : String(error);
      setState({ status: "error", message });
    }
  }

  function updateScrollEdge() {
    const strip = stripRef.current;
    if (strip == null)
      return;
    setScrollEdge({
      left: strip.scrollLeft > 1,
      right: strip.scrollLeft + strip.clientWidth < strip.scrollWidth - 1,
    });
  }

  // 缩略图条挂载与尺寸变化时刷新左右按钮可用性；滚动过程由 onScroll 更新。
  useEffect(() => {
    const strip = stripRef.current;
    if (strip == null)
      return;
    const observer = new ResizeObserver(updateScrollEdge);
    observer.observe(strip);
    return () => observer.disconnect();
  }, [state]);

  function scrollStrip(direction: -1 | 1) {
    stripRef.current?.scrollBy({ left: direction * stripRef.current.clientWidth * 0.8, behavior: "smooth" });
  }

  return (
    <div data-slot="preview-images">
      {state.status === "idle" && (
        <div className="flex flex-col items-start gap-2">
          <Button variant="outline" onClick={() => void load()}>
            <Images data-icon="inline-start" />
            加载预览图
          </Button>
          <p className="text-xs text-muted-foreground">
            将从 whatslink.info 查询该 hash 的公开截图索引；第三方服务，约 5 次/分钟限速。
          </p>
        </div>
      )}
      {state.status === "loading" && (
        <div className="flex gap-2 overflow-hidden">
          <Skeleton className="aspect-video w-52 shrink-0" />
          <Skeleton className="aspect-video w-52 shrink-0" />
          <Skeleton className="aspect-video w-52 shrink-0" />
          <Skeleton className="aspect-video w-52 shrink-0" />
        </div>
      )}
      {state.status === "error" && (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>预览图查询失败</AlertTitle>
          <AlertDescription>{state.message}</AlertDescription>
          <AlertAction>
            <Button variant="outline" size="sm" onClick={() => void load()}>重试</Button>
          </AlertAction>
        </Alert>
      )}
      {state.status === "success" && state.screenshots.length === 0 && (
        <Empty>whatslink.info 未收录该种子的预览截图。</Empty>
      )}
      {state.status === "success" && state.screenshots.length > 0 && (
        <div className="relative">
          <div
            ref={stripRef}
            onScroll={updateScrollEdge}
            className="flex snap-x gap-2 overflow-x-auto pb-1"
          >
            {state.screenshots.map((url, index) => broken.has(index)
              ? (
                  <div
                    key={url}
                    aria-label="截图加载失败"
                    className="flex aspect-video w-52 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground"
                  >
                    <ImageOff size={16} aria-hidden="true" />
                  </div>
                )
              : (
                  <button
                    key={url}
                    type="button"
                    aria-label={`预览截图 ${index + 1}`}
                    onClick={() => setSelected(index)}
                    className="w-52 shrink-0 cursor-pointer snap-start overflow-hidden rounded-md outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    <img
                      src={url}
                      alt=""
                      loading="lazy"
                      referrerPolicy="no-referrer"
                      className="aspect-video w-full object-cover"
                      onError={() => setBroken(previous => new Set(previous).add(index))}
                    />
                  </button>
                ))}
          </div>
          {scrollEdge.left && (
            <Button
              variant="outline"
              size="icon-sm"
              aria-label="向左滚动"
              onClick={() => scrollStrip(-1)}
              className="absolute top-1/2 left-1 -translate-y-1/2 bg-background/90"
            >
              <ChevronLeft />
            </Button>
          )}
          {scrollEdge.right && (
            <Button
              variant="outline"
              size="icon-sm"
              aria-label="向右滚动"
              onClick={() => scrollStrip(1)}
              className="absolute top-1/2 right-1 -translate-y-1/2 bg-background/90"
            >
              <ChevronRight />
            </Button>
          )}
        </div>
      )}
      <Dialog
        open={selected != null}
        onOpenChange={(open) => {
          if (!open)
            setSelected(null);
        }}
      >
        {selected != null && state.status === "success" && (
          <DialogContent
            className="max-w-4xl"
            onKeyDown={(event) => {
              if (event.key === "ArrowLeft" && selected > 0)
                setSelected(selected - 1);
              if (event.key === "ArrowRight" && selected < state.screenshots.length - 1)
                setSelected(selected + 1);
            }}
          >
            <DialogTitle className="sr-only">{`预览截图 ${selected + 1}`}</DialogTitle>
            <div className="flex items-center justify-center">
              {broken.has(selected)
                ? (
                    <div className="flex aspect-video w-full items-center justify-center rounded-md bg-muted text-muted-foreground">
                      <ImageOff size={20} aria-hidden="true" />
                    </div>
                  )
                : (
                    <img
                      src={state.screenshots[selected]}
                      alt={`预览截图 ${selected + 1}`}
                      referrerPolicy="no-referrer"
                      className="max-h-[70vh] max-w-full rounded-md object-contain"
                      onError={() => setBroken(previous => new Set(previous).add(selected))}
                    />
                  )}
            </div>
            <div className="mt-3 flex items-center justify-between">
              <span className="text-xs text-muted-foreground tabular-nums">
                {`${selected + 1} / ${state.screenshots.length}`}
              </span>
              <div className="flex items-center gap-1">
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="上一张"
                  disabled={selected === 0}
                  onClick={() => setSelected(selected - 1)}
                >
                  <ChevronLeft />
                </Button>
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="下一张"
                  disabled={selected >= state.screenshots.length - 1}
                  onClick={() => setSelected(selected + 1)}
                >
                  <ChevronRight />
                </Button>
              </div>
            </div>
          </DialogContent>
        )}
      </Dialog>
    </div>
  );
}
