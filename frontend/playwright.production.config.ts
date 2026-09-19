import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  testMatch: "production.spec.ts",
  workers: 1,
  use: { baseURL: "http://127.0.0.1:3001" },
  webServer: {
    command: "npm run preview -- --port 3001",
    url: "http://127.0.0.1:3001",
    reuseExistingServer: false,
  },
  reporter: "list",
  outputDir: "../artifacts/local-tests/production",
});
