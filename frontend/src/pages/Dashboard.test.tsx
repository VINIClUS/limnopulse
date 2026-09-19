import { render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";

const api = vi.hoisted(() => vi.fn());
vi.mock("../lib/api", () => ({ api }));
vi.mock("../lib/session", () => ({
  useSession: () => ({
    user: { id: "user-1", email: "owner@example.com" },
    exit: vi.fn(),
  }),
}));
import { Dashboard } from "./Dashboard";

beforeEach(() => {
  vi.clearAllMocks();
  api.mockImplementation((path: string) => {
    if (path === "/tenants") {
      return Promise.resolve({
        items: [
          {
            tenant_id: "tnt_1",
            name: "Operação 1",
            city: null,
            version: 1,
            status: "active",
          },
        ],
      });
    }
    if (path === "/tenants/tnt_1/ponds") {
      return Promise.resolve({
        items: [
          {
            pond_id: "pond_1",
            tenant_id: "tnt_1",
            name: "Viveiro 1",
            status: "active",
            version: 1,
          },
        ],
      });
    }
    if (path === "/tenants/tnt_1/devices") {
      return Promise.resolve({ items: [] });
    }
    if (path === "/tenants/tnt_1/alert-events/active?limit=100") {
      return Promise.resolve({
        items: [
          {
            event_id: "alert_1",
            pond_id: "pond_1",
            status: "open",
            metric: "do_mg_l",
          },
        ],
        has_more: true,
      });
    }
    if (path.endsWith("/metrics/latest")) {
      return Promise.resolve({
        measured_at: new Date().toISOString(),
        tenant_id: "tnt_1",
        pond_id: "pond_1",
        do_mg_l: 4.2,
        ph: 7.1,
        temp_c: 26,
      });
    }
    if (path.includes("/metrics/summary")) {
      return Promise.resolve({
        series: [
          {
            measured_at: new Date().toISOString(),
            do_mg_l: 4.2,
            ph: 7.1,
            temp_c: 26,
          },
        ],
        statistics: {
          do_mg_l: { mean: 4.2, min: 4.2, max: 4.2, count: 1 },
          ph: { mean: 7.1, min: 7.1, max: 7.1, count: 1 },
          temp_c: { mean: 26, min: 26, max: 26, count: 1 },
        },
        period: "24h",
        interval: "1h",
      });
    }
    throw new Error(`unexpected API path: ${path}`);
  });
});

it("polls the bounded active-alert view and marks truncated counts", async () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <Dashboard onContact={() => {}} />
      </MemoryRouter>
    </QueryClientProvider>,
  );

  await waitFor(() =>
    expect(
      api.mock.calls.some(
        ([path]) => path === "/tenants/tnt_1/alert-events/active?limit=100",
      ),
    ).toBe(true),
  );
  expect(
    await screen.findByText(/Exibindo os 100 alertas mais recentes/),
  ).toBeInTheDocument();
  expect(screen.getAllByText("1+").length).toBeGreaterThan(0);
  expect(
    api.mock.calls.some(([path]) => path === "/tenants/tnt_1/alert-events"),
  ).toBe(false);
});
