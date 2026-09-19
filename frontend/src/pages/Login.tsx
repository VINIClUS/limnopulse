import { useState, type FormEvent } from "react";
import { useNavigate, useLocation } from "react-router";
import { ArrowRight, Eye, EyeOff, Mail, LockKeyhole } from "lucide-react";
import { Brand, Button, Benefits, ErrorNotice } from "../components/ui";
import {
  login,
  recover,
  finishRecovery,
  completeChallenge,
  devAuthEnabled,
  authConfigured,
} from "../lib/auth";
import { useSession } from "../lib/session";
export function Login({ onContact }: { onContact: () => void }) {
  const navigate = useNavigate(),
    location = useLocation(),
    session = useSession();
  const [email, setEmail] = useState(""),
    [password, setPassword] = useState(""),
    [remember, setRemember] = useState(true),
    [show, setShow] = useState(false),
    [busy, setBusy] = useState(false),
    [error, setError] = useState<unknown>(null),
    [mode, setMode] = useState<"login" | "recover" | "reset" | "challenge">(
      "login",
    ),
    [code, setCode] = useState(""),
    [message, setMessage] = useState(""),
    [challenge, setChallenge] = useState("");
  async function done() {
    await session.refresh();
    const target = location.state?.from;
    navigate(
      typeof target === "string" && target.startsWith("/") ? target : "/app",
      { replace: true },
    );
  }
  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      if (mode === "recover") {
        await recover(email);
        setMode("reset");
        setMessage(
          "Se houver uma conta para este e-mail, você receberá um código de recuperação.",
        );
      } else if (mode === "reset") {
        await finishRecovery(email, code, password);
        setMode("login");
        setPassword("");
        setMessage("Senha alterada. Entre com sua nova senha.");
      } else {
        const result =
          mode === "challenge"
            ? await completeChallenge(
                challenge.includes("NEW_PASSWORD") ? password : code,
              )
            : await login(email, password, remember);
        if (result.isSignedIn) await done();
        else {
          const step = result.nextStep?.signInStep || "";
          if (
            [
              "CONFIRM_SIGN_IN_WITH_NEW_PASSWORD_REQUIRED",
              "CONFIRM_SIGN_IN_WITH_SMS_CODE",
              "CONFIRM_SIGN_IN_WITH_TOTP_CODE",
              "CONFIRM_SIGN_IN_WITH_EMAIL_CODE",
            ].includes(step)
          ) {
            setChallenge(step);
            setMode("challenge");
            setPassword("");
          } else
            throw new Error(
              "Sua conta precisa de uma etapa adicional. Entre em contato com o administrador da operação.",
            );
        }
      }
    } catch (e) {
      const name = e instanceof Error ? e.name : "";
      setError(
        new Error(
          name === "NotAuthorizedException"
            ? "E-mail ou senha incorretos."
            : name === "CodeMismatchException"
              ? "Código inválido. Confira e tente novamente."
              : name === "LimitExceededException"
                ? "Muitas tentativas. Aguarde e tente novamente."
                : e instanceof Error
                  ? e.message
                  : "Não foi possível entrar. Tente novamente.",
        ),
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <main className="login-page">
      <div className="login-shell">
        <section className="login-panel">
          <Brand />
          <div className="login-main">
            <h1>
              {mode === "login"
                ? "Bem-vindo de volta"
                : mode === "recover"
                  ? "Recupere seu acesso"
                  : mode === "reset"
                    ? "Crie uma nova senha"
                    : "Confirme seu acesso"}
            </h1>
            <p className="login-subtitle">
              {mode === "login"
                ? "Acesse sua operação"
                : "Vamos ajudar você a continuar."}
            </p>
            {devAuthEnabled() && (
              <div className="dev-note">
                Ambiente local · autenticação de desenvolvimento
              </div>
            )}
            {!authConfigured && !devAuthEnabled() && (
              <p role="status">
                O acesso está aguardando a configuração de autenticação.
              </p>
            )}
            <form onSubmit={submit} className="login-form">
              {mode !== "challenge" && (
                <label>
                  E-mail
                  <div className="input-icon">
                    <Mail size={20} />
                    <input
                      type="email"
                      autoComplete="username"
                      required
                      placeholder="seu@e-mail.com"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                    />
                  </div>
                </label>
              )}
              {(mode === "login" ||
                mode === "reset" ||
                (mode === "challenge" &&
                  challenge.includes("NEW_PASSWORD"))) && (
                <label>
                  {mode === "login" ? "Senha" : "Nova senha"}
                  <div className="input-icon">
                    <LockKeyhole size={20} />
                    <input
                      type={show ? "text" : "password"}
                      autoComplete={
                        mode === "login" ? "current-password" : "new-password"
                      }
                      required
                      minLength={mode === "login" ? 1 : 12}
                      value={password}
                      placeholder="••••••••"
                      onChange={(e) => setPassword(e.target.value)}
                    />
                    <button
                      type="button"
                      aria-label={show ? "Ocultar senha" : "Mostrar senha"}
                      onClick={() => setShow(!show)}
                    >
                      {show ? <EyeOff size={20} /> : <Eye size={20} />}
                    </button>
                  </div>
                </label>
              )}
              {(mode === "reset" ||
                (mode === "challenge" &&
                  !challenge.includes("NEW_PASSWORD"))) && (
                <label>
                  Código de confirmação
                  <input
                    autoComplete="one-time-code"
                    required
                    value={code}
                    onChange={(e) => setCode(e.target.value)}
                  />
                </label>
              )}
              {mode === "login" && (
                <div className="login-options">
                  <label className="checkbox">
                    <input
                      type="checkbox"
                      checked={remember}
                      onChange={(e) => setRemember(e.target.checked)}
                    />
                    Manter conectado
                  </label>
                  <button
                    className="text-button"
                    type="button"
                    onClick={() => {
                      setMode("recover");
                      setError(null);
                      setMessage("");
                    }}
                  >
                    Esqueci minha senha?
                  </button>
                </div>
              )}
              {message && (
                <p className="info-notice" role="status">
                  {message}
                </p>
              )}
              {error != null && <ErrorNotice error={error} />}
              <Button
                type="submit"
                disabled={busy || (!authConfigured && !devAuthEnabled())}
              >
                {busy
                  ? "Aguarde…"
                  : mode === "login"
                    ? "Entrar"
                    : mode === "recover"
                      ? "Enviar código"
                      : mode === "reset"
                        ? "Salvar nova senha"
                        : "Confirmar"}{" "}
                <ArrowRight />
              </Button>
              {mode !== "login" && (
                <button
                  type="button"
                  className="text-button"
                  onClick={() => {
                    setMode("login");
                    setError(null);
                    setMessage("");
                  }}
                >
                  Voltar para entrar
                </button>
              )}
            </form>
            <div className="login-contact">
              <span>Ainda não usa o LimnoPulse?</span>
              <button className="text-button" onClick={onContact}>
                Conhecer o LimnoPulse <ArrowRight size={18} />
              </button>
            </div>
          </div>
          <Benefits />
        </section>
        <section className="login-photo" aria-label="Viveiros ao entardecer">
          <p className="photo-kicker">
            Água produtiva.
            <br />
            Um futuro sustentável.
          </p>
          <div className="login-photo-metrics">
            {[
              ["O₂", "6,4", "mg/L"],
              ["Temp.", "28,1", "°C"],
              ["pH", "7,3", ""],
            ].map(([name, value, unit]) => (
              <div key={name}>
                <small>{name}</small>
                <strong>
                  {value}
                  <em>{unit}</em>
                </strong>
                <svg viewBox="0 0 100 25" aria-hidden="true">
                  <path
                    d="M0 20 8 13 16 16 24 8 32 14 40 12 48 17 56 10 64 12 72 7 80 13 88 10 100 5"
                    fill="none"
                    stroke="#37c6e7"
                  />
                </svg>
                <small>● Ilustrativo</small>
              </div>
            ))}
          </div>
          <div className="login-photo-copy">
            <h2>
              Dados do viveiro.
              <br />
              Onde você estiver.
            </h2>
            <i />
            <Brand light />
          </div>
        </section>
      </div>
    </main>
  );
}
