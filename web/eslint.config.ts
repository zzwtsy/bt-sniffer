import antfu from "@antfu/eslint-config";
import pluginRouter from "@tanstack/eslint-plugin-router";

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
  files: ["apps/frontend/src/**/*.{ts,tsx}", "apps/backend/src/**/*.ts"],
  plugins: {
    "@tanstack/router": pluginRouter,
  },
  rules: {
    "ts/strict-boolean-expressions": ["error", {
      allowString: true,
      allowNumber: false,
      allowNullableObject: true,
      allowNullableBoolean: true,
    }],
  },
});
