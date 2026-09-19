import { test, expect } from "@playwright/test";
// This suite deliberately uses no HTTP interception. Requires the documented local stack.
test("real API: sign in, onboarding, back navigation, dashboard and logout", async ({
  page,
  request,
}) => {
  const suffix = Date.now();
  await page.goto("/entrar");
  await page
    .getByLabel("E-mail", { exact: true })
    .fill(`local-${suffix}@example.com`);
  await page.getByLabel("Senha", { exact: true }).fill("local-only");
  await page.getByRole("button", { name: "Entrar", exact: true }).click();
  await expect(page).toHaveURL("/app");
  await page.goto("/onboarding");
  await page.getByLabel("Nome da propriedade").fill(`Fazenda E2E ${suffix}`);
  await page.getByLabel(/Cidade/).fill("Panorama - SP");
  await page.getByRole("button", { name: "Continuar" }).click();
  await expect(
    page.getByRole("heading", { name: "Cadastre seus viveiros" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Voltar", exact: true }).click();
  await page.getByRole("button", { name: "Continuar" }).click();
  await page.getByLabel("Nome do viveiro 1").fill("Viveiro E2E");
  await page.getByRole("button", { name: "Continuar" }).click();
  await expect(
    page.getByRole("heading", { name: "Adicione um dispositivo" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Voltar", exact: true }).click();
  await page.getByRole("button", { name: "Continuar" }).click();
  await page.getByLabel("Nome do dispositivo").fill("Sensor E2E");
  await page.getByRole("button", { name: "Continuar" }).click();
  await page.getByRole("button", { name: "Ir para a visão geral" }).click();
  await expect(page).toHaveURL("/app");
  await expect(page.getByRole("table")).toContainText("Viveiro E2E");
  await expect(page.getByText("Sem telemetria neste período.")).toBeVisible();
  await expect(
    page.getByText("Conexão não informada", { exact: true }),
  ).toBeVisible();
  const response = await request.get("/v1/tenants", {
    headers: { "X-Dev-User-Sub": "local-user" },
  });
  expect(response.ok()).toBe(true);
  const items = (await response.json()).items;
  const matching = items.filter(
    (t: { name: string }) => t.name === `Fazenda E2E ${suffix}`,
  );
  expect(matching).toHaveLength(1);
  expect(matching[0].city).toBe("Panorama - SP");
  const ponds = await request.get(
    `/v1/tenants/${matching[0].tenant_id}/ponds`,
    { headers: { "X-Dev-User-Sub": "local-user" } },
  );
  expect((await ponds.json()).items).toHaveLength(1);
  const devices = await request.get(
    `/v1/tenants/${matching[0].tenant_id}/devices`,
    { headers: { "X-Dev-User-Sub": "local-user" } },
  );
  expect((await devices.json()).items).toHaveLength(1);
  await page.getByRole("button", { name: "Sair", exact: true }).click();
  await expect(page).toHaveURL("/entrar");
  await page.goto("/app");
  await expect(page).toHaveURL("/entrar");
});
test("real API: lead through public modal and checkout payment disabled", async ({
  page,
}) => {
  await page.goto("/produto");
  await page.getByRole("button", { name: "Solicitar contato" }).first().click();
  await page.getByLabel("Nome completo").fill("Teste Local");
  await page.getByLabel("E-mail").fill("local-test@example.com");
  await page.getByLabel(/Autorizo/).check();
  const submitted = page.waitForResponse((r) => r.url().endsWith("/v1/leads"));
  await page.getByRole("button", { name: "Enviar interesse" }).click();
  expect((await submitted).status()).toBe(201);
  await expect(page.getByText("Interesse recebido!")).toBeVisible();
  await page.getByRole("button", { name: "Fechar contato" }).click();
  await page.goto("/checkout");
  await expect(page.getByLabel("CPF / CNPJ")).toBeDisabled();
  await expect(page.getByLabel("Número do cartão")).toBeDisabled();
});
