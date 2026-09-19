import type { Page } from "@playwright/test";
export const fixedTime = new Date("2026-09-19T20:30:00-03:00");
export const tenants = [
  {
    tenant_id: "tnt_1",
    name: "Fazenda Santana",
    city: "Panorama - SP",
    version: 1,
    status: "active",
  },
  {
    tenant_id: "tnt_2",
    name: "Fazenda Boa Esperança",
    city: "Ilha Solteira - SP",
    version: 1,
    status: "active",
  },
];
export const ponds = Array.from({ length: 4 }, (_, i) => ({
  tenant_id: "tnt_1",
  pond_id: `pond_${i + 1}`,
  name: `Viveiro 0${i + 1}`,
  status: "active",
  version: 1,
}));
export async function visualSession(page: Page) {
  await page.addInitScript(() => {
    sessionStorage.setItem("limnopulse:dev-user", "vinicius@example.com");
    localStorage.setItem("limnopulse:remember", "false");
  });
}
export async function visualApi(page: Page) {
  await page.clock.setFixedTime(fixedTime);
  await page.route("**/v1/**", async (route) => {
    const url = new URL(route.request().url()),
      path = url.pathname;
    let data: unknown = {};
    if (path === "/v1/tenants") data = { items: tenants };
    else if (path.endsWith("/ponds"))
      data = {
        items: path.includes("tnt_2")
          ? [
              {
                ...ponds[0],
                tenant_id: "tnt_2",
                pond_id: "pond_other",
                name: "Lago Sul",
              },
            ]
          : ponds,
      };
    else if (path.endsWith("/devices"))
      data = {
        items: Array.from({ length: 12 }, (_, i) => ({
          device_id: `dev_${i}`,
          pond_id: ponds[i % 4].pond_id,
          name: `Sensor ${i + 1}`,
          status: "active",
        })),
      };
    else if (path.endsWith("/alert-events"))
      data = {
        items: [
          {
            event_id: "alert_1",
            pond_id: "pond_4",
            status: "open",
            severity: "warning",
            metric: "do_mg_l",
          },
          {
            event_id: "alert_2",
            pond_id: "pond_4",
            status: "acknowledged",
            severity: "critical",
            metric: "ph",
          },
        ],
      };
    else if (path.endsWith("/metrics/latest")) {
      const i = Number(path.match(/pond_(\d)/)?.[1] || 1) - 1;
      data = {
        tenant_id: "tnt_1",
        pond_id: ponds[i]?.pond_id,
        measured_at: fixedTime.toISOString(),
        do_mg_l: [6.8, 5.9, 6.4, 4.8][i],
        ph: [7.4, 7.1, 7.3, 6.9][i],
        temp_c: [27.8, 28.3, 28.1, 29][i],
      };
    } else if (path.endsWith("/metrics/summary")) {
      const period = url.searchParams.get("period") || "24h";
      const interval = period === "24h" ? 300000 : 3600000;
      data = {
        tenant_id: "tnt_1",
        pond_id: "pond_1",
        period,
        interval: period === "24h" ? "5m" : "1h",
        series: Array.from({ length: 80 }, (_, i) => ({
          measured_at: new Date(+fixedTime - (79 - i) * interval).toISOString(),
          do_mg_l:
            3.5 +
            i * 0.035 +
            Math.sin(i * 0.22) * 0.8 +
            Math.sin(i * 1.4) * 0.17,
          ph: 7.1 + Math.sin(i * 0.2) * 0.3,
          temp_c: 27.8 + Math.sin(i * 0.2) * 1.1,
        })),
        statistics: {
          do_mg_l: { mean: 6.1, min: 4.8, max: 7.9, count: 12960 },
          ph: { mean: 7.3, min: 6.9, max: 7.7, count: 12960 },
          temp_c: { mean: 28.1, min: 26.4, max: 29.3, count: 12960 },
        },
      };
    } else if (path === "/v1/leads")
      data = { lead_id: "lead_visual", status: "received" };
    else
      return route.fulfill({
        status: 404,
        json: { detail: "fixture not configured" },
      });
    await route.fulfill({ json: data });
  });
}
