import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Routes, Route } from "react-router";
import { vi, it, expect, beforeEach } from "vitest";
const auth = vi.hoisted(() => ({
  login: vi.fn(),
  recover: vi.fn(),
  finishRecovery: vi.fn(),
  completeChallenge: vi.fn(),
  devAuthEnabled: () => false,
  authConfigured: true,
}));
const refresh = vi.hoisted(() => vi.fn());
vi.mock("../lib/auth", () => auth);
vi.mock("../lib/session", () => ({ useSession: () => ({ refresh }) }));
import { Login } from "./Login";
beforeEach(() => vi.clearAllMocks());
function setup(initialEntries = ["/entrar"]) {
  render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/entrar" element={<Login onContact={() => {}} />} />
        <Route path="/app" element={<h1>Operação autenticada</h1>} />
        <Route
          path="/tenants/:tenantId/alert-events/:eventId"
          element={<h1>Detalhe autenticado</h1>}
        />
      </Routes>
    </MemoryRouter>,
  );
  return userEvent.setup();
}
it("signs in through the custom form and opens the operation", async () => {
  auth.login.mockResolvedValue({ isSignedIn: true });
  const user = setup();
  await user.type(screen.getByLabelText("E-mail"), "maria@example.com");
  await user.type(screen.getByLabelText("Senha"), "password");
  await user.click(screen.getByRole("button", { name: "Entrar" }));
  expect(await screen.findByText("Operação autenticada")).toBeInTheDocument();
  expect(refresh).toHaveBeenCalledOnce();
});
it("returns to a protected deep link with query and fragment after sign-in", async () => {
  auth.login.mockResolvedValue({ isSignedIn: true });
  const user = setup([
    {
      pathname: "/entrar",
      state: { from: "/tenants/tnt_1/alert-events/a1?tab=history#details" },
    },
  ] as never);
  await user.type(screen.getByLabelText("E-mail"), "maria@example.com");
  await user.type(screen.getByLabelText("Senha"), "password");
  await user.click(screen.getByRole("button", { name: "Entrar" }));
  expect(await screen.findByText("Detalhe autenticado")).toBeInTheDocument();
});
it("recovers the password and confirms a new password without creating an account", async () => {
  auth.recover.mockResolvedValue({
    nextStep: { resetPasswordStep: "CONFIRM_RESET_PASSWORD_WITH_CODE" },
  });
  auth.finishRecovery.mockResolvedValue(undefined);
  const user = setup();
  await user.click(
    screen.getByRole("button", { name: "Esqueci minha senha?" }),
  );
  await user.type(screen.getByLabelText("E-mail"), "maria@example.com");
  await user.click(screen.getByRole("button", { name: "Enviar código" }));
  await user.type(
    await screen.findByLabelText("Código de confirmação"),
    "123456",
  );
  await user.type(screen.getByLabelText("Nova senha"), "NewStrongPass123!");
  await user.click(screen.getByRole("button", { name: "Salvar nova senha" }));
  expect(
    await screen.findByText("Senha alterada. Entre com sua nova senha."),
  ).toBeInTheDocument();
  expect(auth.finishRecovery).toHaveBeenCalledWith(
    "maria@example.com",
    "123456",
    "NewStrongPass123!",
  );
});
it("shows a Cognito error without reporting success", async () => {
  auth.login.mockRejectedValue(
    Object.assign(new Error("denied"), { name: "NotAuthorizedException" }),
  );
  const user = setup();
  await user.type(screen.getByLabelText("E-mail"), "maria@example.com");
  await user.type(screen.getByLabelText("Senha"), "wrong");
  await user.click(screen.getByRole("button", { name: "Entrar" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "E-mail ou senha incorretos.",
  );
  expect(refresh).not.toHaveBeenCalled();
});
