import { test, expect } from "@playwright/test";
import { visualApi, visualSession } from "./fixtures";
import { mkdir } from "node:fs/promises";
const screens = [
  ["inicio", "/"],
  ["produto", "/produto"],
  ["planos", "/planos"],
  ["checkout", "/checkout"],
  ["login", "/entrar"],
  ["onboarding", "/onboarding"],
  ["dashboard", "/app"],
];
for (const [name, path] of screens)
  for (const mobile of [false, true])
    test(`${name} ${mobile ? "mobile" : "desktop"}`, async ({ page }) => {
      const wide = ["login", "onboarding", "dashboard"].includes(name);
      await page.setViewportSize(
        mobile
          ? { width: 390, height: 844 }
          : wide
            ? { width: 1448, height: 1086 }
            : { width: 1086, height: 1448 },
      );
      await visualApi(page);
      if (["dashboard", "onboarding"].includes(name)) await visualSession(page);
      const errors: string[] = [];
      page.on("pageerror", (e) => errors.push(e.message));
      await page.goto(path);
      await page.evaluate(() => document.fonts.ready);
      await page.addStyleTag({
        content:
          "*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important}.dev-note{display:none!important}",
      });
      await expect(page.locator("h1")).toBeVisible();
      if (name === "dashboard") {
        await expect(page.getByRole("table")).toBeVisible();
        await page
          .getByRole("combobox", { name: "Viveiro", exact: true })
          .selectOption("pond_3");
        await expect(
          page.getByRole("img", { name: /Gráfico de/ }),
        ).toBeVisible();
      }
      if (name === "onboarding") {
        await page.getByLabel("Nome da propriedade").fill("Fazenda Santana");
        await page.getByLabel(/Cidade/).fill("Panorama - SP");
      }
      await page.locator("h1").click();
      // Decode every CSS background before the deterministic capture.
      await page.evaluate(async () => {
        const urls = new Set<string>();
        for (const el of document.querySelectorAll("*")) {
          for (const match of getComputedStyle(el).backgroundImage.matchAll(
            /url\("?([^"\)]+)"?\)/g,
          ))
            urls.add(match[1]);
        }
        await Promise.all(
          [...urls].map(
            (url) =>
              new Promise<void>((resolve) => {
                const im = new Image();
                im.onload = () => resolve();
                im.onerror = () => resolve();
                im.src = url;
              }),
          ),
        );
      });
      await page.waitForTimeout(250);
      await mkdir("../artifacts/visual/screenshots", { recursive: true });
      const suffix = mobile ? "mobile" : "desktop";
      await page.screenshot({
        path: `../artifacts/visual/screenshots/${name}-${suffix}.png`,
        animations: "disabled",
      });
      await page.screenshot({
        path: `../artifacts/visual/screenshots/${name}-${suffix}-full.png`,
        fullPage: true,
        animations: "disabled",
      });
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      expect(errors).toEqual([]);
      if (["planos", "checkout"].includes(name))
        await expect(page.locator('meta[name="robots"]')).toHaveAttribute(
          "content",
          "noindex, nofollow",
        );
      if (name === "inicio" || name === "produto") {
        expect(await page.locator('a[href="/planos"]').count()).toBe(0);
        await expect(page.getByText(/R\$ /)).toHaveCount(0);
      }
    });
test("dashboard visual states: period, parameter and tenant are isolated", async ({
  page,
}) => {
  await visualApi(page);
  await visualSession(page);
  await page.goto("/app");
  await expect(page.getByRole("table")).toBeVisible();
  await page.getByRole("tab", { name: /pH/ }).click();
  await expect(page.getByRole("img", { name: /Gráfico de pH/ })).toBeVisible();
  await page
    .getByRole("combobox", { name: "Período", exact: true })
    .selectOption("7d");
  await expect(page.getByRole("img", { name: /7d/ })).toBeVisible();
  await page.getByLabel("Propriedade", { exact: true }).selectOption("tnt_2");
  await expect(page.getByRole("button", { name: "Lago Sul" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Viveiro 03" })).toHaveCount(0);
});
for (const status of [403, 503])
  test(`dashboard HTTP ${status} visual state`, async ({ page }) => {
    await visualApi(page);
    await visualSession(page);
    await page.route("**/v1/tenants", (route) =>
      route.fulfill({ status, json: { detail: "failure" } }),
    );
    await page.goto("/app");
    await expect(page.getByRole("alert")).toBeVisible();
    await page.screenshot({
      path: `../artifacts/visual/screenshots/dashboard-error-${status}.png`,
    });
  });
test("empty telemetry is rendered explicitly", async ({ page }) => {
  await visualApi(page);
  await visualSession(page);
  await page.route("**/metrics/summary?*", (route) =>
    route.fulfill({ json: { series: [], statistics: {} } }),
  );
  await page.goto("/app");
  await expect(page.getByText("Sem telemetria neste período.")).toBeVisible();
});
test("expired session returns to sign in without retaining private UI", async ({
  page,
}) => {
  await visualApi(page);
  await visualSession(page);
  await page.route("**/v1/tenants", (route) =>
    route.fulfill({ status: 401, json: { detail: "expired" } }),
  );
  await page.goto("/app");
  await expect(page).toHaveURL(/\/entrar$/);
  await expect(
    page.getByRole("heading", { name: "Bem-vindo de volta" }),
  ).toBeVisible();
});
test("contact failure is visible and can be retried", async ({ page }) => {
  await visualApi(page);
  await page.goto("/");
  await page
    .getByRole("button", { name: "Falar com o time", exact: true })
    .click();
  await page.getByLabel("Nome completo").fill("Maria Silva");
  await page.getByLabel("E-mail", { exact: false }).fill("maria@example.com");
  await page.getByLabel(/Autorizo/).check();
  await page.route("**/v1/leads", (route) =>
    route.fulfill({ status: 503, json: { detail: "unavailable" } }),
  );
  await page.getByRole("button", { name: "Enviar interesse" }).click();
  await expect(page.getByRole("alert")).toContainText(
    "temporariamente indisponível",
  );
  await expect(page.getByLabel("Nome completo")).toHaveValue("Maria Silva");
  await page.unroute("**/v1/leads");
  await page.getByRole("button", { name: "Enviar interesse" }).click();
  await expect(page.getByText("Interesse recebido!")).toBeVisible();
});

test("unavailable alerts never imply healthy water", async ({ page }) => {
  await visualApi(page);
  await visualSession(page);
  await page.route("**/alert-events", (r) =>
    r.fulfill({ status: 503, json: { detail: "unavailable" } }),
  );
  await page.goto("/app");
  await expect(page.getByRole("table")).toBeVisible();
  await expect(page.getByText("Sem alertas em aberto")).toHaveCount(0);
  await expect(
    page.getByText("Alertas indisponíveis", { exact: true }),
  ).toBeVisible();
});
