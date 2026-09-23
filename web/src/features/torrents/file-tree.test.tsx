import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { retry } from "@/lib/api/client";
import { FileTree } from "./file-tree";

let client: QueryClient | undefined;

afterEach(() => {
  client?.clear();
  client = undefined;
  vi.unstubAllGlobals();
});

it.each([429, 503, 504, 0])("续页遇到 %i 类错误后暂停自动拉取，手动重试成功后继续", async (failureStatus) => {
  let failuresRemaining = 3;
  const file = (index: number, path: string) => ({
    index,
    path,
    path_truncated: false,
    encoding_lossy: false,
    length: "1",
  });
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input), "http://localhost");
    if (!url.searchParams.has("after")) {
      return new Response(JSON.stringify({
        available: true,
        items: [file(0, "root/first.txt")],
        next: "0000000000000001",
      }));
    }
    if (failuresRemaining > 0) {
      failuresRemaining--;
      if (failureStatus === 0)
        throw new TypeError("请求超时");
      return new Response(JSON.stringify({
        error: { code: "source_unavailable", message: "服务忙" },
      }), { status: failureStatus });
    }
    return new Response(JSON.stringify({
      available: true,
      items: [file(1, "root/second.txt")],
      next: null,
    }));
  });
  vi.stubGlobal("fetch", fetchMock);
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry, retryDelay: 0 } },
  });
  client = queryClient;

  render(
    <QueryClientProvider client={queryClient}>
      <FileTree hash={"0".repeat(40)} />
    </QueryClientProvider>,
  );

  expect(await screen.findByText("first.txt")).toBeInTheDocument();
  expect(await screen.findByText("文件清单无法读取")).toBeInTheDocument();
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(4));
  await new Promise(resolve => setTimeout(resolve, 50));
  expect(fetchMock).toHaveBeenCalledTimes(4);

  fireEvent.click(screen.getByRole("button", { name: "重试" }));
  expect(await screen.findByText("second.txt")).toBeInTheDocument();
  expect(fetchMock).toHaveBeenCalledTimes(5);
});
