import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/browser",
  timeout: 30_000,
  workers: 1,
  fullyParallel: false,
  use: { baseURL: "http://127.0.0.1:4183", browserName: "chromium", trace: "retain-on-failure" },
  outputDir: "../target/checks/frontend-browser",
  webServer: [
    { command: "node tests/server.mjs", url: "http://127.0.0.1:4311/api/v1/health", reuseExistingServer: false },
    { command: "pnpm exec vite preview --host 127.0.0.1 --port 4183 --strictPort", url: "http://127.0.0.1:4183", env: { MONITOR_PROXY_TARGET: "http://127.0.0.1:4311" }, reuseExistingServer: false },
  ],
});
