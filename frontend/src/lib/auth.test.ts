import { describe, it, expect, vi, beforeEach } from "vitest";
const sdk = vi.hoisted(() => ({
  fetchAuthSession: vi.fn(),
  getCurrentUser: vi.fn(),
  signIn: vi.fn(),
  signOut: vi.fn(),
  resetPassword: vi.fn(),
  confirmResetPassword: vi.fn(),
  confirmSignIn: vi.fn(),
}));
vi.mock("aws-amplify/auth", () => sdk);
vi.mock("aws-amplify", () => ({ Amplify: { configure: vi.fn() } }));
vi.mock("aws-amplify/auth/cognito", () => ({
  cognitoUserPoolsTokenProvider: { setKeyValueStorage: vi.fn() },
}));
import {
  accessToken,
  login,
  logout,
  recover,
  finishRecovery,
  authStorage,
} from "./auth";
beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  sessionStorage.clear();
  vi.stubEnv("VITE_DEV_AUTH", "false");
});
describe("Cognito session", () => {
  it("uses the access token and refreshes explicitly when requested", async () => {
    sdk.fetchAuthSession.mockResolvedValue({
      tokens: {
        accessToken: { toString: () => "access" },
        idToken: { toString: () => "id" },
      },
    });
    expect(await accessToken(true)).toBe("access");
    expect(sdk.fetchAuthSession).toHaveBeenCalledWith({ forceRefresh: true });
  });
  it("keeps unchecked sessions in sessionStorage and uses SRP", async () => {
    sdk.signIn.mockResolvedValue({ isSignedIn: true });
    await login("a@example.com", "secret", false);
    await authStorage.setItem("token", "temporary");
    expect(sessionStorage.getItem("token")).toBe("temporary");
    expect(localStorage.getItem("token")).toBeNull();
    expect(sdk.signIn).toHaveBeenCalledWith({
      username: "a@example.com",
      password: "secret",
      options: { authFlowType: "USER_SRP_AUTH" },
    });
  });
  it("keeps the storage backend local to the tab when another tab changes remember", async () => {
    sdk.signIn.mockResolvedValue({ isSignedIn: true });
    await login("a@example.com", "secret", false);
    localStorage.setItem("limnopulse:remember", "true");
    await authStorage.setItem("token", "temporary");
    expect(sessionStorage.getItem("token")).toBe("temporary");
    expect(localStorage.getItem("token")).toBeNull();
  });
  it("persists only when requested and clears tokens on logout", async () => {
    sdk.signIn.mockResolvedValue({ isSignedIn: true });
    await login("a@example.com", "secret", true);
    await authStorage.setItem("token", "persistent");
    expect(localStorage.getItem("token")).toBe("persistent");
    await logout();
    expect(sdk.signOut).toHaveBeenCalled();
  });
  it("delegates recovery and confirmation to Cognito", async () => {
    await recover("a@example.com");
    await finishRecovery("a@example.com", "123456", "new-password");
    expect(sdk.resetPassword).toHaveBeenCalledWith({
      username: "a@example.com",
    });
    expect(sdk.confirmResetPassword).toHaveBeenCalledWith({
      username: "a@example.com",
      confirmationCode: "123456",
      newPassword: "new-password",
    });
  });
});

it("never enables development auth in a production build", async () => {
  vi.stubEnv("VITE_DEV_AUTH", "true");
  vi.stubEnv("DEV", false);
  vi.stubEnv("MODE", "production");
  const { devAuthEnabled } = await import("./auth");
  expect(devAuthEnabled()).toBe(false);
  vi.unstubAllEnvs();
});
