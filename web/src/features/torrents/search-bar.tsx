import { useNavigate } from "@tanstack/react-router";
import { Search, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field";
import { InputGroup, InputGroupAddon, InputGroupButton, InputGroupInput } from "@/components/ui/input-group";
import { classifyTorrentInput } from "./search";

/** 即时搜索条：输入防抖 300ms 驱动 URL；满 40 位 hash 直跳详情，其余有效输入 replace 目录查询。 */
export function SearchBar({ q }: { q?: string }) {
  const navigate = useNavigate({ from: "/torrents/" });
  const inputRef = useRef<HTMLInputElement>(null);
  const [input, setInput] = useState(q ?? "");
  // self 记录防抖导航的目标值（null 表示无自发导航）；URL 追赶上该值时不回写输入框，避免吃掉正在输入的字符。
  const [sync, setSync] = useState<{ q?: string; self: string | null | undefined }>({ q, self: null });
  if (sync.q !== q) {
    if (sync.self !== q)
      setInput(q ?? "");
    setSync({ q, self: null });
  }

  useEffect(() => {
    const timer = window.setTimeout(() => {
      const classified = classifyTorrentInput(input);
      if (classified.kind === "hash") {
        void navigate({ to: "/torrents/$hash", params: { hash: classified.value } });
        return;
      }
      const target = classified.kind === "query" ? classified.value : classified.kind === "empty" ? undefined : null;
      if (target === null || target === q)
        return;
      setSync(prev => ({ ...prev, self: target }));
      void navigate({ to: "/torrents", search: target === undefined ? {} : { q: target }, replace: true });
    }, 300);
    return () => window.clearTimeout(timer);
  }, [input, q, navigate]);

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey)
        return;
      const target = event.target;
      if (target instanceof HTMLElement && target.closest("input, textarea, select, [contenteditable]") != null)
        return;
      event.preventDefault();
      inputRef.current?.focus();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  const length = [...input.trim()].length;
  const classified = classifyTorrentInput(input);
  const hint = classified.kind === "error" && length < 3 ? `再输入 ${3 - length} 个字符开始搜索` : undefined;
  const error = classified.kind === "error" && length > 200 ? classified.message : undefined;

  return (
    <FieldGroup className="mb-5">
      <Field data-invalid={error != null}>
        <FieldLabel htmlFor="torrent-search">名称、文件路径或完整 hash</FieldLabel>
        <InputGroup>
          <InputGroupAddon>
            <Search />
          </InputGroupAddon>
          <InputGroupInput
            id="torrent-search"
            ref={inputRef}
            value={input}
            aria-invalid={error != null}
            placeholder="例如 Ubuntu、folder/movie.mkv 或 40 位 hash"
            onChange={event => setInput(event.target.value)}
          />
          <InputGroupAddon align="inline-end">
            {input !== "" && (
              <InputGroupButton
                aria-label="清空"
                onClick={() => {
                  setInput("");
                  inputRef.current?.focus();
                }}
              >
                <X />
              </InputGroupButton>
            )}
            <kbd>/</kbd>
          </InputGroupAddon>
        </InputGroup>
        {hint != null && <FieldDescription>{hint}</FieldDescription>}
        <FieldError>{error}</FieldError>
      </Field>
    </FieldGroup>
  );
}
