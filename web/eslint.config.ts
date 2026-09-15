import antfu from "@antfu/eslint-config";
import pluginRouter from "@tanstack/eslint-plugin-router";
import { boundaries } from "./eslint-boundaries";

export default antfu({
  isInEditor: false,
  formatters: true,
  react: true,
  typescript: {
    tsconfigPath: "tsconfig.json",
  },
  stylistic: {
    indent: 2,
    quotes: "double",
    semi: true,
    braceStyle: "1tbs",
  },
  ignores: [
    "**/node_modules/**",
    "**/dist/**",
    "src/components/ui/**",
    "src/routeTree.gen.ts",
  ],
}, {
  files: ["src/**/*.{ts,tsx}"],
  plugins: {
    "@tanstack/router": pluginRouter,
    "local": { rules: { boundaries } },
  },
  rules: {
    "local/boundaries": "error",
    "ts/strict-boolean-expressions": ["error", {
      allowString: true,
      allowNumber: false,
      allowNullableObject: true,
      allowNullableBoolean: true,
    }],
  },
});
