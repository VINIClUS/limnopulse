import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          charts: ["recharts"],
          auth: ["aws-amplify/auth", "aws-amplify/auth/cognito"],
          react: ["react", "react-dom", "react-router"],
        },
      },
    },
  },
  server: {
    port: 3000,
    strictPort: true,
    proxy: {
      "/v1": {
        target: process.env.API_PROXY_TARGET || "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
