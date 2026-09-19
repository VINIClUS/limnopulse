import { test, expect } from "@playwright/test";
for (const route of ["/", "/produto", "/planos", "/checkout", "/entrar"])
  test(`production ${route}`, async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(route);
    await expect(page.locator("h1")).toBeVisible();
    expect(errors).toEqual([]);
    await expect(page.getByText(/autenticação de desenvolvimento/)).toHaveCount(
      0,
    );
  });
test("production ignores local development credentials", async ({ page }) => {
  await page.addInitScript(() => {
    sessionStorage.setItem("limnopulse:dev-user", "fake@example.com");
    localStorage.setItem("limnopulse:remember", "false");
  });
  await page.goto("/app");
  await expect(page).toHaveURL("/entrar");
  await expect(
    page.getByRole("heading", { name: "Bem-vindo de volta" }),
  ).toBeVisible();
  await page.goto("/onboarding");
  await expect(page).toHaveURL("/entrar");
});
