import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  testMatch: "local.spec.ts",
  workers: 1,
  timeout: 45000,
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:3000",
    locale: "pt-BR",
    timezoneId: "America/Sao_Paulo",
  },
  outputDir: "../artifacts/local-tests",
});
