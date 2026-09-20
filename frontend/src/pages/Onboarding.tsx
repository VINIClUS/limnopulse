import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import {
  ArrowRight,
  ArrowLeft,
  House,
  Fish,
  Radio,
  CheckCircle2,
  LogOut,
  Plus,
} from "lucide-react";
import { Brand, Button, ErrorNotice } from "../components/ui";
import { api, post, type Tenant, type Pond, type Device } from "../lib/api";
import { useSession } from "../lib/session";
type Draft = {
  step: number;
  name: string;
  city: string;
  tenant?: Tenant;
  ponds: { name: string; saved?: Pond }[];
  deviceName: string;
  device?: Device;
};
export function Onboarding() {
  const { user, exit } = useSession();
  return <OnboardingForm key={user!.id} user={user!} exit={exit} />;
}

function OnboardingForm({
  user,
  exit,
}: {
  user: { id: string };
  exit: () => Promise<void>;
}) {
  const navigate = useNavigate(),
    cache = useQueryClient();
  const key = `limnopulse:onboarding:${user!.id}`;
  const [draft, setDraft] = useState<Draft>(() => {
    try {
      const raw = sessionStorage.getItem(key);
      if (raw) return JSON.parse(raw);
    } catch {
      /* start a new draft */
    }
    return {
      step: 0,
      name: "",
      city: "",
      ponds: [{ name: "" }],
      deviceName: "",
    };
  });
  const [busy, setBusy] = useState(false),
    [error, setError] = useState<unknown>(null);
  function save(next: Draft) {
    setDraft(next);
    sessionStorage.setItem(key, JSON.stringify(next));
  }
  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    let next = { ...draft };
    try {
      if (draft.step === 0) {
        const body = {
          name: draft.name.trim(),
          city: draft.city.trim() || null,
        };
        const tenant = draft.tenant
          ? await api<Tenant>(`/tenants/${draft.tenant.tenant_id}`, {
              method: "PATCH",
              body: JSON.stringify({
                ...body,
                expected_version: draft.tenant.version,
              }),
            })
          : await post<Tenant>("/tenants", body);
        next = { ...next, tenant };
        save(next);
      }
      if (draft.step === 1) {
        for (let i = 0; i < draft.ponds.length; i++) {
          const p = next.ponds[i];
          if (p.saved && p.saved.name === p.name.trim()) continue;
          const path = `/tenants/${draft.tenant!.tenant_id}/ponds`;
          const saved = p.saved
            ? await api<Pond>(`${path}/${p.saved.pond_id}`, {
                method: "PATCH",
                body: JSON.stringify({
                  name: p.name.trim(),
                  expected_version: p.saved.version,
                }),
              })
            : await post<Pond>(path, { name: p.name.trim() });
          next = {
            ...next,
            ponds: next.ponds.map((row, j) =>
              j === i ? { ...row, saved } : row,
            ),
          };
          save(next);
        }
      }
      if (draft.step === 2 && draft.deviceName.trim() && !draft.device) {
        const device = await post<Device>(
          `/tenants/${draft.tenant!.tenant_id}/devices`,
          {
            name: draft.deviceName.trim(),
            pond_id: draft.ponds[0].saved!.pond_id,
          },
        );
        next = { ...next, device };
        save(next);
      }
      if (draft.step === 3) {
        localStorage.setItem(
          `limnopulse:tenant:${user!.id}`,
          draft.tenant!.tenant_id,
        );
        sessionStorage.removeItem(key);
        await cache.invalidateQueries({ queryKey: ["tenants"] });
        navigate("/app");
        return;
      }
      save({ ...next, step: draft.step + 1 });
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  }
  const titles = [
    "Dados da propriedade",
    "Cadastre seus viveiros",
    "Adicione um dispositivo",
    "Tudo pronto para começar",
  ];
  const Icon = [House, Fish, Radio, CheckCircle2][draft.step];
  return (
    <main className="onboarding-page">
      <header>
        <Brand />
        <button className="text-button" onClick={() => void exit()}>
          <LogOut size={20} /> Sair
        </button>
      </header>
      <ol className="onboarding-steps">
        {["Propriedade", "Viveiros", "Dispositivo", "Concluir"].map(
          (label, i) => (
            <li
              key={label}
              className={i <= draft.step ? "current" : ""}
              aria-current={i === draft.step ? "step" : undefined}
            >
              <span>{i < draft.step ? <CheckCircle2 size={22} /> : i + 1}</span>
              {label}
            </li>
          ),
        )}
      </ol>
      <h1>Configure sua operação</h1>
      <p className="onboarding-subtitle">
        Vamos preparar o LimnoPulse para sua propriedade.
      </p>
      <form className="card onboarding-card" onSubmit={submit}>
        <div className="section-heading">
          <span className="icon-circle">
            <Icon />
          </span>
          <div>
            <h2>{titles[draft.step]}</h2>
            <p>
              {
                [
                  "Comece informando os dados básicos da sua propriedade.",
                  "Adicione pelo menos um viveiro para acompanhar a água.",
                  "Etapa opcional. Você pode cadastrar um dispositivo depois.",
                  "Sua estrutura foi salva. Os dados aparecerão após o envio de telemetria.",
                ][draft.step]
              }
            </p>
          </div>
        </div>
        {draft.step === 0 && (
          <>
            <label>
              Nome da propriedade
              <input
                autoComplete="organization"
                required
                maxLength={120}
                value={draft.name}
                onChange={(e) => save({ ...draft, name: e.target.value })}
                placeholder="Ex.: Fazenda Santana"
              />
            </label>
            <label>
              Cidade <span className="optional">(opcional)</span>
              <input
                autoComplete="address-level2"
                maxLength={120}
                value={draft.city}
                onChange={(e) => save({ ...draft, city: e.target.value })}
                placeholder="Ex.: Panorama - SP"
              />
            </label>
          </>
        )}
        {draft.step === 1 && (
          <>
            <div className="pond-fields">
              {draft.ponds.map((p, i) => (
                <label key={i}>
                  Nome do viveiro {i + 1}
                  <input
                    required
                    maxLength={120}
                    value={p.name}
                    placeholder={`Viveiro ${String(i + 1).padStart(2, "0")}`}
                    onChange={(e) =>
                      save({
                        ...draft,
                        ponds: draft.ponds.map((row, j) =>
                          j === i ? { ...row, name: e.target.value } : row,
                        ),
                      })
                    }
                  />
                </label>
              ))}
            </div>
            <Button
              secondary
              onClick={() =>
                save({ ...draft, ponds: [...draft.ponds, { name: "" }] })
              }
            >
              <Plus size={18} /> Adicionar viveiro
            </Button>
          </>
        )}
        {draft.step === 2 && (
          <>
            <label>
              Nome do dispositivo
              <input
                maxLength={120}
                disabled={Boolean(draft.device)}
                value={draft.deviceName}
                onChange={(e) => save({ ...draft, deviceName: e.target.value })}
                placeholder="Ex.: Sensor do Viveiro 01"
              />
            </label>
            <p className="info-notice">
              O dispositivo será associado ao primeiro viveiro. O cadastro não
              confirma conexão: a instalação física e o envio de telemetria são
              etapas separadas.
            </p>
          </>
        )}
        {draft.step === 3 && (
          <div className="completion">
            <CheckCircle2 />
            <h3>{draft.tenant?.name}</h3>
            <p>
              {draft.ponds.filter((p) => p.saved).length} viveiro(s)
              cadastrado(s)
              <br />
              {draft.device
                ? "1 dispositivo cadastrado"
                : "Dispositivo pode ser adicionado depois"}
            </p>
          </div>
        )}
        {error != null && <ErrorNotice error={error} />}
        <div className="onboarding-actions">
          {draft.step > 0 && (
            <Button
              secondary
              disabled={busy}
              onClick={() => save({ ...draft, step: draft.step - 1 })}
            >
              <ArrowLeft size={18} /> Voltar
            </Button>
          )}
          <Button type="submit" disabled={busy}>
            {busy
              ? "Salvando…"
              : draft.step === 3
                ? "Ir para a visão geral"
                : draft.step === 2 && !draft.deviceName
                  ? "Pular por enquanto"
                  : "Continuar"}{" "}
            <ArrowRight size={20} />
          </Button>
        </div>
      </form>
      <div className="onboarding-bottom">
        <span>
          MONITORAR HOJE.
          <br />
          PRODUZIR SEMPRE.
        </span>
        <span>
          TECNOLOGIA
          <br />
          PARA ÁGUAS
          <br />
          QUE PRODUZEM
          <br />
          MAIS VIDA
        </span>
      </div>
    </main>
  );
}
