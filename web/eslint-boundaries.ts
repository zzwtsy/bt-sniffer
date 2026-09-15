import type { Rule } from "eslint";
import path from "node:path";

/** 解析相对路径与别名，避免用另一种导入写法绕过切片边界。 */
export const boundaries: Rule.RuleModule = {
  meta: { type: "problem", schema: [], messages: { direction: "违反垂直切片依赖方向：{{source}} → {{target}}" } },
  create(context) {
    const root = path.join(context.cwd, "src");
    const source = path.relative(root, context.filename).split(path.sep);
    return {
      ImportDeclaration(node) {
        const imported = node.source.value;
        if (typeof imported !== "string")
          return;
        const target = imported.startsWith("@/") ? path.join(root, imported.slice(2)) : imported.startsWith(".") ? path.resolve(path.dirname(context.filename), imported) : undefined;
        if (target === undefined)
          return;
        const parts = path.relative(root, target).split(path.sep);
        const shared = ["lib", "components"].includes(source[0]);
        const crossSlice = source[0] === "features" && parts[0] === "features" && source[1] !== parts[1];
        const reversed = shared && ["app", "routes", "features"].includes(parts[0]);
        if (crossSlice || reversed || (source[0] === "features" && ["app", "routes"].includes(parts[0]))) {
          context.report({ node, messageId: "direction", data: { source: source.join("/"), target: parts.join("/") } });
        }
      },
    };
  },
};
