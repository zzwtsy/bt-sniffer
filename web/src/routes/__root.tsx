import { createRootRoute, Link } from "@tanstack/react-router";
import { Layout } from "@/app/layout";
import { searchSchema } from "@/lib/api/search";

export const Route = createRootRoute({
  component: Layout,
  validateSearch: input => searchSchema.parse(input),
  notFoundComponent: () => (
    <div className="empty">
      <h1>页面不存在</h1>
      <Link to="/">返回流程总览</Link>
    </div>
  ),
  errorComponent: ({ error, reset }) => (
    <div className="notice danger">
      <h1>页面暂时无法显示</h1>
      <p>{error instanceof Error ? error.message : "未知错误"}</p>
      <button onClick={reset}>重新加载页面</button>
    </div>
  ),
});
