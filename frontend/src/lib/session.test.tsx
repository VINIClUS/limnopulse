import { render, screen, waitFor, act } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { vi, it, expect } from "vitest";
const auth = vi.hoisted(() => ({ currentUser: vi.fn(), logout: vi.fn() }));
vi.mock("./auth", () => auth);
import { SessionProvider, useSession } from "./session";
function Who() {
  const { user, loading } = useSession();
  return <span>{loading ? "loading" : user?.id || "none"}</span>;
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
