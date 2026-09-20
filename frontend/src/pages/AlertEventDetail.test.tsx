import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => vi.fn());
vi.mock("../lib/api", () => ({ api }));
import { AlertEventDetail } from "./AlertEventDetail";

beforeEach(() => vi.clearAllMocks());

describe("AlertEventDetail", () => {
  it.each([
    ["forbidden", "Você não tem permissão para acessar estes dados."],
    ["missing", "Alerta não encontrado."],
  ])("renders the %s error state", async (_name, message) => {
    api.mockRejectedValueOnce(new Error(message));
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={["/tenants/tnt_1/alert-events/a1"]}>
          <Routes>
            <Route
              path="/tenants/:tenantId/alert-events/:eventId"
              element={<AlertEventDetail />}
            />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>,
    );

    expect(await screen.findByRole("alert")).toHaveTextContent(message);
  });
});
