import { vi, it, expect, beforeEach } from "vitest";
const auth = vi.hoisted(() => ({
  accessToken: vi.fn(),
  currentUser: vi.fn(),
  devAuthEnabled: () => false,
}));
vi.mock("./auth", () => auth);
import { api } from "./api";
beforeEach(() => vi.clearAllMocks());
it("preserves the session when token refresh has a network failure", async () => {
  auth.accessToken.mockRejectedValue(
    Object.assign(new Error("Network unavailable"), { name: "NetworkError" }),
  );
  const expired = vi.fn();
  window.addEventListener("session-expired", expired);
  await expect(api("/tenants")).rejects.toMatchObject({ status: 503 });
  expect(expired).not.toHaveBeenCalled();
  window.removeEventListener("session-expired", expired);
});
it("expires a definitively unauthenticated session", async () => {
  auth.accessToken.mockRejectedValue(
    Object.assign(new Error("missing"), {
      name: "UserUnAuthenticatedException",
    }),
  );
  const expired = vi.fn();
  window.addEventListener("session-expired", expired);
  await expect(api("/tenants")).rejects.toMatchObject({ status: 401 });
  expect(expired).toHaveBeenCalledOnce();
  window.removeEventListener("session-expired", expired);
});
it("retries a 401 once with a renewed access token", async () => {
  auth.accessToken.mockResolvedValueOnce("old").mockResolvedValueOnce("new");
  const fetcher = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(new Response("{}", { status: 401 }))
    .mockResolvedValueOnce(new Response('{"items":[]}', { status: 200 }));
  await expect(api("/tenants")).resolves.toEqual({ items: [] });
  expect(auth.accessToken).toHaveBeenLastCalledWith(true);
  expect(
    (fetcher.mock.calls[1][1]?.headers as Headers).get("Authorization"),
  ).toBe("Bearer new");
  fetcher.mockRestore();
});
