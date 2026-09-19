import { Amplify } from "aws-amplify";
import { cognitoUserPoolsTokenProvider } from "aws-amplify/auth/cognito";
import {
  fetchAuthSession,
  getCurrentUser,
  signIn,
  signOut,
  resetPassword,
  confirmResetPassword,
  confirmSignIn,
} from "aws-amplify/auth";
const rememberKey = "limnopulse:remember";
const devKey = "limnopulse:dev-user";
const cognitoKeyPrefix = "CognitoIdentityServiceProvider.";
export const devAuthEnabled = () =>
  (import.meta.env.DEV || import.meta.env.MODE === "test") &&
  import.meta.env.VITE_DEV_AUTH === "true" &&
  ["localhost", "127.0.0.1", "[::1]"].includes(location.hostname);
const hasPersistentTokens = () =>
  Object.keys(localStorage).some((key) => key.startsWith(cognitoKeyPrefix));
const storage = () => {
  const preference = sessionStorage.getItem(rememberKey);
  if (preference === "true") return localStorage;
  if (preference === "false") return sessionStorage;
  return hasPersistentTokens() ? localStorage : sessionStorage;
};
export const authStorage = {
  async setItem(key: string, value: string) {
    storage().setItem(key, value);
  },
  async getItem(key: string) {
    return storage().getItem(key);
  },
  async removeItem(key: string) {
    localStorage.removeItem(key);
    sessionStorage.removeItem(key);
  },
  async clear() {
    for (const s of [localStorage, sessionStorage])
      for (const k of Object.keys(s))
        if (k.startsWith(cognitoKeyPrefix)) s.removeItem(k);
  },
};
const pool = import.meta.env.VITE_COGNITO_USER_POOL_ID,
  client = import.meta.env.VITE_COGNITO_CLIENT_ID;
export const authConfigured = Boolean(pool && client);
if (authConfigured) {
  Amplify.configure({
    Auth: { Cognito: { userPoolId: pool, userPoolClientId: client } },
  });
  cognitoUserPoolsTokenProvider.setKeyValueStorage(authStorage);
}
export type SessionUser = { id: string; email: string };
const definitiveAuthErrorNames = new Set([
  "UserUnAuthenticatedException",
  "NotAuthorizedException",
]);
const isDefinitiveAuthError = (error: unknown) => {
  if (!error || typeof error !== "object" || !("name" in error)) return false;
  const name = error.name;
  return typeof name === "string" && definitiveAuthErrorNames.has(name);
};
export async function currentUser(): Promise<SessionUser | null> {
  if (devAuthEnabled()) {
    const email = storage().getItem(devKey);
    return email
      ? { id: import.meta.env.VITE_DEV_USER_SUB || "local-user", email }
      : null;
  }
  if (!authConfigured) return null;
  try {
    const user = await getCurrentUser();
    const session = await fetchAuthSession();
    if (!session.tokens?.accessToken) return null;
    return {
      id: user.userId,
      email: user.signInDetails?.loginId || user.username,
    };
  } catch (error) {
    if (isDefinitiveAuthError(error)) return null;
    throw error;
  }
}
export async function accessToken(forceRefresh = false) {
  const session = await fetchAuthSession({ forceRefresh });
  if (!session.tokens?.accessToken)
    throw Object.assign(new Error("Sua sessão expirou. Entre novamente."), {
      name: "UserUnAuthenticatedException",
    });
  return session.tokens.accessToken.toString();
}
export async function login(
  email: string,
  password: string,
  remember: boolean,
) {
  localStorage.removeItem(devKey);
  sessionStorage.removeItem(devKey);
  sessionStorage.setItem(rememberKey, String(remember));
  if (devAuthEnabled()) {
    storage().setItem(devKey, email);
    return { isSignedIn: true, nextStep: { signInStep: "DONE" } };
  }
  return signIn({
    username: email,
    password,
    options: { authFlowType: "USER_SRP_AUTH" },
  });
}
export const completeChallenge = (value: string) =>
  confirmSignIn({ challengeResponse: value });
export async function logout() {
  try {
    if (!devAuthEnabled()) await signOut();
  } finally {
    await authStorage.clear();
    localStorage.removeItem(devKey);
    sessionStorage.removeItem(devKey);
    sessionStorage.removeItem(rememberKey);
    localStorage.removeItem(rememberKey);
    for (const k of Object.keys(sessionStorage))
      if (k.startsWith("limnopulse:onboarding:")) sessionStorage.removeItem(k);
  }
}
export const recover = (email: string) => resetPassword({ username: email });
export const finishRecovery = (email: string, code: string, password: string) =>
  confirmResetPassword({
    username: email,
    confirmationCode: code,
    newPassword: password,
  });
