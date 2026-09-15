import path from "node:path";
import process from "node:process";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "MONITOR_");
  const target = env.MONITOR_PROXY_TARGET || "http://127.0.0.1:3001";
  const proxy = { "/api": { target, changeOrigin: false } };
  return {
    server: { host: "127.0.0.1", proxy },
    preview: { host: "127.0.0.1", proxy },
    plugins: [
    // Please make sure that '@tanstack/router-plugin' is passed before '@vitejs/plugin-react'
      tanstackRouter({
        target: "react",
        autoCodeSplitting: true,
      }),
      react(),
      tailwindcss(),
    ],
    resolve: {
      alias: {
        "@": path.resolve(import.meta.dirname, "./src"),
      },
    },
  };
});
