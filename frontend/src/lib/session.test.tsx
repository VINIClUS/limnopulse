import {
  render,
  screen,
  waitFor,
  act,
  fireEvent,
} from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { vi, it, expect } from "vitest";
import { MemoryRouter, Routes, Route, useLocation } from "react-router";
const auth = vi.hoisted(() => ({ currentUser: vi.fn(), logout: vi.fn() }));
vi.mock("./auth", () => auth);
import { Protected, SessionProvider, useSession } from "./session";
function Who() {
  const { user, loading } = useSession();
  return <span>{loading ? "loading" : user?.id || "none"}</span>;
}
function RefreshButton() {
  const { refresh } = useSession();
  return <button onClick={() => void refresh()}>refresh</button>;
}
function RedirectTarget() {
  const location = useLocation();
  return <output>{location.state?.from || "missing redirect state"}</output>;
}
it("clears cached private data when another tab changes the authenticated principal", async () => {
  auth.currentUser.mockResolvedValue({ id: "A", email: "a@example.com" });
  const cache = new QueryClient();
  render(
    <QueryClientProvider client={cache}>
      <SessionProvider>
        <Who />
      </SessionProvider>
    </QueryClientProvider>,
  );
  await screen.findByText("A");
  cache.setQueryData(["tenants", "A"], { secret: "private A" });
  auth.currentUser.mockResolvedValue({ id: "B", email: "b@example.com" });
  act(() =>
    window.dispatchEvent(
      new StorageEvent("storage", {
        key: "CognitoIdentityServiceProvider.client.LastAuthUser",
        newValue: "B",
      }),
    ),
  );
  await waitFor(() => expect(screen.getByText("B")).toBeInTheDocument());
  expect(cache.getQueryData(["tenants", "A"])).toBeUndefined();
  expect(auth.logout).not.toHaveBeenCalled();
});

it("preserves the current user and cache when refresh fails transiently", async () => {
  auth.currentUser.mockResolvedValue({ id: "A", email: "a@example.com" });
  const cache = new QueryClient();
  render(
    <QueryClientProvider client={cache}>
      <SessionProvider>
        <Who />
        <RefreshButton />
      </SessionProvider>
    </QueryClientProvider>,
  );
  await screen.findByText("A");
  cache.setQueryData(["tenants", "A"], { secret: "private A" });
  auth.currentUser.mockRejectedValueOnce(
    Object.assign(new Error("offline"), { name: "NetworkError" }),
  );

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "refresh" }));
    await Promise.resolve();
  });

  expect(screen.getByText("A")).toBeInTheDocument();
  expect(cache.getQueryData(["tenants", "A"])).toEqual({ secret: "private A" });
});

it("captures the actual protected pathname, query and fragment for login", async () => {
  auth.currentUser.mockResolvedValue(null);
  const cache = new QueryClient();
  render(
    <QueryClientProvider client={cache}>
      <MemoryRouter
        initialEntries={["/tenants/tnt_1/alert-events/a1?tab=history#details"]}
      >
        <SessionProvider>
          <Routes>
            <Route
              path="/tenants/:tenantId/alert-events/:eventId"
              element={
                <Protected>
                  <h1>private alert</h1>
                </Protected>
              }
            />
            <Route path="/entrar" element={<RedirectTarget />} />
          </Routes>
        </SessionProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );

  expect(
    await screen.findByText(
      "/tenants/tnt_1/alert-events/a1?tab=history#details",
    ),
  ).toBeInTheDocument();
});
