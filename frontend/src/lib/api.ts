import { accessToken, currentUser, devAuthEnabled } from "./auth";
export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
export async function api<T>(
  path: string,
  options: RequestInit = {},
  isPublic = false,
): Promise<T> {
  const headers = new Headers(options.headers);
  headers.set("Content-Type", "application/json");
  const authenticate = async (refresh = false) => {
    if (isPublic) return;
    if (devAuthEnabled()) {
      const user = await currentUser();
      if (!user) throw new ApiError(401, "Entre para continuar.");
      headers.set("X-Dev-User-Sub", user.id);
    } else {
      try {
        headers.set("Authorization", `Bearer ${await accessToken(refresh)}`);
      } catch (error) {
        if (
          error instanceof Error &&
          ["UserUnAuthenticatedException", "NotAuthorizedException"].includes(
            error.name,
          )
        ) {
          window.dispatchEvent(new Event("session-expired"));
          throw new ApiError(401, "Sua sessão expirou. Entre novamente.");
        }
        throw new ApiError(
          503,
          "Não foi possível renovar sua sessão agora. Tente novamente.",
        );
      }
    }
  };
  await authenticate();
  let response = await fetch(`/v1${path}`, { ...options, headers });
  if (response.status === 401 && !isPublic && !devAuthEnabled()) {
    await authenticate(true);
    response = await fetch(`/v1${path}`, { ...options, headers });
  }
  if (!response.ok) {
    if (response.status === 401 && !isPublic)
      window.dispatchEvent(new Event("session-expired"));
    const messages: Record<number, string> = {
      401: "Sua sessão expirou. Entre novamente.",
      403: "Você não tem permissão para acessar estes dados.",
      409: "Os dados foram alterados. Atualize a página e tente novamente.",
      422: "Confira os dados informados.",
      429: "Muitas solicitações. Aguarde um minuto e tente novamente.",
      503: "Serviço temporariamente indisponível. Tente novamente.",
    };
    throw new ApiError(
      response.status,
      messages[response.status] ||
        "Não foi possível concluir. Tente novamente.",
    );
  }
  return response.json() as Promise<T>;
}
export type Tenant = {
  tenant_id: string;
  name: string;
  city: string | null;
  version: number;
  status: string;
};
export type Pond = {
  pond_id: string;
  tenant_id: string;
  name: string;
  status: string;
  version: number;
};
export type Device = {
  device_id: string;
  pond_id: string;
  name: string;
  status: string;
};
export type Metric = "do_mg_l" | "ph" | "temp_c";
export type Latest = {
  measured_at: string | null;
  tenant_id: string;
  pond_id: string;
} & Record<Metric, number | null>;
export type Stats = {
  mean: number | null;
  min: number | null;
  max: number | null;
  count: number;
};
export type Summary = {
  series: ({ measured_at: string } & Record<Metric, number | null>)[];
  statistics: Record<Metric, Stats>;
  period: string;
  interval: string;
};
export type AlertEvent = {
  event_id: string;
  pond_id: string;
  status: string;
  state?: string;
  metric?: string;
  severity?: string;
  opened_at?: string;
};
export const post = <T>(path: string, body: unknown, isPublic = false) =>
  api<T>(path, { method: "POST", body: JSON.stringify(body) }, isPublic);
