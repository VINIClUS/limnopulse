import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  testMatch: "visual.spec.ts",
  timeout: 45000,
  fullyParallel: false,
  workers: 1,
  reporter: [
    ["list"],
    [
      "html",
      { outputFolder: "../artifacts/visual/playwright-report", open: "never" },
    ],
  ],
  use: {
    baseURL: "http://127.0.0.1:3000",
    locale: "pt-BR",
    timezoneId: "America/Sao_Paulo",
    colorScheme: "light",
    reducedMotion: "reduce",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "VITE_DEV_AUTH=true VITE_DEV_USER_SUB=local-user npm run dev",
    url: "http://127.0.0.1:3000",
    reuseExistingServer: !process.env.CI,
  },
  outputDir: "../artifacts/visual/test-results",
});
