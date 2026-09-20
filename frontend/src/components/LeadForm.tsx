import {
  useState,
  useEffect,
  useRef,
  type FormEvent,
  type ReactNode,
} from "react";
import { ArrowRight, CheckCircle2, X } from "lucide-react";
import { post } from "../lib/api";
import { Button, ErrorNotice } from "./ui";
export function LeadForm({
  source,
  children,
}: {
  source: string;
  children?: ReactNode;
}) {
  const [busy, setBusy] = useState(false),
    [sent, setSent] = useState(false),
    [error, setError] = useState<unknown>(null);
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    const data = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      await post(
        "/leads",
        {
          name: String(data.get("name")).trim(),
          email: String(data.get("email")).trim(),
          phone: String(data.get("phone") || "").trim() || null,
          property_name: String(data.get("property_name") || "").trim() || null,
          source,
          consent: data.get("consent") === "on",
        },
        true,
      );
      setSent(true);
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  }
  if (sent)
    return (
      <div className="lead-success" role="status">
        <CheckCircle2 size={48} />
        <h2>Interesse recebido!</h2>
        <p>
          Seus dados foram salvos. Nosso time poderá entrar em contato para
          conhecer sua operação.
        </p>
        <small>Nenhuma cobrança foi realizada.</small>
      </div>
    );
  return (
    <form onSubmit={submit} className="lead-form">
      <div className="form-grid">
        <label>
          Nome completo *
          <input
            name="name"
            autoComplete="name"
            required
            minLength={2}
            maxLength={120}
            placeholder="Seu nome"
          />
        </label>
        <label>
          E-mail *
          <input
            name="email"
            type="email"
            autoComplete="email"
            required
            maxLength={254}
            placeholder="voce@propriedade.com"
          />
        </label>
        <label>
          Telefone <span className="optional">(opcional)</span>
          <input
            name="phone"
            type="tel"
            autoComplete="tel"
            maxLength={30}
            placeholder="(11) 99999-9999"
          />
        </label>
        <label>
          Propriedade <span className="optional">(opcional)</span>
          <input
            name="property_name"
            maxLength={120}
            placeholder="Nome da propriedade"
          />
        </label>
      </div>
      {children}
      <label className="checkbox">
        <input type="checkbox" name="consent" required />{" "}
        <span>
          Autorizo o contato da equipe LimnoPulse pelos dados informados para
          apresentar a solução.
        </span>
      </label>
      <p className="privacy-note">
        Usaremos seus dados para responder ao seu interesse. Não pedimos dados
        de pagamento.
      </p>
      {error != null && <ErrorNotice error={error} />}
      <Button type="submit" disabled={busy}>
        {busy ? "Enviando…" : "Enviar interesse"} <ArrowRight size={18} />
      </Button>
    </form>
  );
}
export function ContactModal({
  open,
  onClose,
  source,
}: {
  open: boolean;
  onClose: () => void;
  source: string;
}) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    if (open) dialog.current?.showModal();
    else dialog.current?.close();
  }, [open]);
  return (
    <dialog
      ref={dialog}
      className="contact-dialog"
      onCancel={onClose}
      onClose={onClose}
      aria-labelledby="contact-title"
    >
      <button
        className="dialog-close"
        onClick={onClose}
        aria-label="Fechar contato"
      >
        <X />
      </button>
      {open && (
        <>
          <span className="eyebrow">VAMOS CONVERSAR</span>
          <h2 id="contact-title">Conheça o LimnoPulse</h2>
          <p>Conte um pouco sobre você e sua operação.</p>
          <LeadForm source={source} />
        </>
      )}
    </dialog>
  );
}
