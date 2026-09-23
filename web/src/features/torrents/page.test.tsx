import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  RouterProvider,
} from "@tanstack/react-router";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { retry } from "@/lib/api/client";
import { TorrentsPage } from "./page";

const item = {
  hash: "a".repeat(40),
  parse_status: "parsed",
  name: "Example",
  name_truncated: false,
  encoding_lossy: false,
  total_length: "42",
  file_count: 1,
  piece_length: "16384",
  piece_count: 1,
  private: false,
  fetched_at_ms: 1_700_000_000_000,
};
const index = { indexed: 2, total: 2, complete: true, search_complete: true };

let client: QueryClient | undefined;

afterEach(() => {
  client?.clear();
  client = undefined;
  vi.unstubAllGlobals();
});

function stubCatalog(total: number, perRequestItems = [item]) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input), "http://localhost");
    const page = Number(url.searchParams.get("page") ?? 1);
    const lastPage = Math.max(1, Math.ceil(total / 50));
    return new Response(JSON.stringify({
      items: page > lastPage ? [] : perRequestItems,
      total,
      page,
      index,
    }));
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function renderAt(entry: string) {
  const rootRoute = createRootRoute({ component: Outlet });
  const torrentsRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/torrents",
    validateSearch: (search: Record<string, unknown>) => ({
      q: typeof search.q === "string" && search.q !== "" ? search.q : undefined,
      page: typeof search.page === "number" && Number.isInteger(search.page) && search.page >= 1 ? search.page : undefined,
    }),
    component: function TorrentsTestRoute() {
      return <TorrentsPage search={torrentsRoute.useSearch()} />;
    },
  });
  const detailRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/torrents/$hash",
    component: () => null,
  });
  const router = createRouter({
    routeTree: rootRoute.addChildren([torrentsRoute, detailRoute]),
    history: createMemoryHistory({ initialEntries: [entry] }),
  });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry, retryDelay: 0 } } });
  client = queryClient;
  render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  return router;
}

it("首页展示总数与页码链接，上一页为禁用按钮", async () => {
  stubCatalog(100);
  renderAt("/torrents");

  expect(await screen.findByText("共 100 条")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "上一页" })).toBeDisabled();
  const second = screen.getByRole("link", { name: "第 2 页" });
  expect(second).toHaveAttribute("href", expect.stringContaining("page=2"));
  expect(screen.getByRole("link", { name: "下一页" })).toHaveAttribute("href", expect.stringContaining("page=2"));
  expect(screen.getByRole("link", { name: "第 1 页" })).toHaveAttribute("aria-current", "page");
});

it("末页保留下一页禁用态，链接保留搜索词", async () => {
  stubCatalog(100);
  renderAt("/torrents?q=fix&page=2");

  expect(await screen.findByText("共 100 条")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "下一页" })).toBeDisabled();
  const previous = screen.getByRole("link", { name: "上一页" });
  expect(previous).toHaveAttribute("href", expect.stringContaining("page=1"));
  expect(previous).toHaveAttribute("href", expect.stringContaining("q=fix"));
  const first = screen.getByRole("link", { name: "第 1 页" });
  expect(first).toHaveAttribute("href", expect.stringContaining("q=fix"));
});

it("页码越界时 replace 到末页", async () => {
  stubCatalog(100);
  const router = renderAt("/torrents?page=9");

  await waitFor(() => expect(router.state.location.search.page).toBe(2));
  expect(await screen.findByText("共 100 条")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Example" })).toBeInTheDocument();
});

it("刷新失败保留已有结果，并可从错误提示重试", async () => {
  const fetchMock = stubCatalog(100);
  renderAt("/torrents");
  expect(await screen.findByRole("link", { name: "Example" })).toBeInTheDocument();
  fetchMock.mockImplementation(async () => new Response(JSON.stringify({
    error: { code: "source_unavailable", message: "服务忙" },
  }), { status: 503 }));
  await client!.refetchQueries();
  expect(await screen.findByText("目录暂时无法读取")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Example" })).toBeInTheDocument();
  stubCatalog(100);
  fireEvent.click(screen.getByRole("button", { name: "重试" }));
  await waitFor(() => expect(screen.queryByText("目录暂时无法读取")).not.toBeInTheDocument());
  expect(screen.getByRole("link", { name: "Example" })).toBeInTheDocument();
});
