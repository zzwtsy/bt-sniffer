import { Check, Magnet } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { magnetLink } from "./format";

/** 复制完整磁力链接的图标按钮；成功后短暂显示对勾，剪贴板不可用时保持原图标。 */
export function CopyMagnet({ hash, name }: { hash: string; name?: string | null }) {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<number | undefined>(undefined);
  useEffect(() => () => window.clearTimeout(timerRef.current), []);

  async function copy() {
    try {
      await navigator.clipboard.writeText(magnetLink(hash, name));
    } catch {
      return;
    }
    setCopied(true);
    window.clearTimeout(timerRef.current);
    timerRef.current = window.setTimeout(setCopied, 1500, false);
  }

  return (
    <Button
      type="button"
      variant="ghost"
      size="icon-xs"
      aria-label="复制磁力链接"
      onClick={() => void copy()}
    >
      {copied ? <Check /> : <Magnet />}
    </Button>
  );
}
