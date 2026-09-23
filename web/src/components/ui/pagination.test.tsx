import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Link,
  Outlet,
  RouterProvider,
} from "@tanstack/react-router";
import { PaginationNext } from "./pagination";

it("保留分页链接的子内容、无障碍名称和 SPA 导航", async () => {
  const rootRoute = createRootRoute({ component: Outlet });
  const pageRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/",
    component: () => (
      <PaginationNext
        text="下一页"
        render={<Link to="/torrents" aria-label="下一页" />}
      />
    ),
  });
  const nextRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/torrents",
    component: () => <p>下一页内容</p>,
  });
  const router = createRouter({
    routeTree: rootRoute.addChildren([pageRoute, nextRoute]),
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });

  render(<RouterProvider router={router} />);

  const link = await screen.findByRole("link", { name: "下一页" });
  expect(screen.getByText("下一页")).toBeInTheDocument();
  expect(link.querySelector("svg")).toBeInTheDocument();
  fireEvent.click(link);
  expect(await screen.findByText("下一页内容")).toBeInTheDocument();
});
