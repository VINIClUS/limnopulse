import {
  render,
  screen,
  waitFor,
  act,
  fireEvent,
} from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { vi, it, expect } from "vitest";
const auth = vi.hoisted(() => ({ currentUser: vi.fn(), logout: vi.fn() }));
vi.mock("./auth", () => auth);
import { SessionProvider, useSession } from "./session";
function Who() {
  const { user, loading } = useSession();
  return <span>{loading ? "loading" : user?.id || "none"}</span>;
}
function RefreshButton() {
  const { refresh } = useSession();
  return <button onClick={() => void refresh()}>refresh</button>;
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
