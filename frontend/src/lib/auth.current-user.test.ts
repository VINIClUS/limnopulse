import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

const sdk = vi.hoisted(() => ({
  fetchAuthSession: vi.fn(),
  getCurrentUser: vi.fn(),
}));
vi.mock("aws-amplify/auth", () => sdk);
vi.mock("aws-amplify", () => ({ Amplify: { configure: vi.fn() } }));
vi.mock("aws-amplify/auth/cognito", () => ({
  cognitoUserPoolsTokenProvider: { setKeyValueStorage: vi.fn() },
}));

let currentUser: typeof import("./auth").currentUser;

beforeAll(async () => {
  vi.stubEnv("VITE_COGNITO_USER_POOL_ID", "us-east-1_test");
  vi.stubEnv("VITE_COGNITO_CLIENT_ID", "client");
  ({ currentUser } = await import("./auth"));
});

afterAll(() => vi.unstubAllEnvs());

beforeEach(() => vi.clearAllMocks());

describe("currentUser", () => {
  it("propagates transient failures while checking the current user", async () => {
    const error = Object.assign(new Error("offline"), { name: "NetworkError" });
    sdk.getCurrentUser.mockRejectedValue(error);

    await expect(currentUser()).rejects.toBe(error);
  });

  it("returns null for a definitively unauthenticated user", async () => {
    sdk.getCurrentUser.mockRejectedValue(
      Object.assign(new Error("missing"), {
        name: "UserUnAuthenticatedException",
      }),
    );

    await expect(currentUser()).resolves.toBeNull();
  });

  it("propagates transient failures while fetching the session", async () => {
    const error = Object.assign(new Error("cognito unavailable"), {
      name: "NetworkError",
    });
    sdk.getCurrentUser.mockResolvedValue({
      userId: "user-1",
      username: "user-1",
      signInDetails: { loginId: "a@example.com" },
    });
    sdk.fetchAuthSession.mockRejectedValue(error);

    await expect(currentUser()).rejects.toBe(error);
  });
});
