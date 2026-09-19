import { useState } from "react";
import { Link } from "react-router";
import { useQuery, useQueries } from "@tanstack/react-query";
import {
  House,
  Fish,
  Wifi,
  Bell,
  Settings,
  HelpCircle,
  LogOut,
  Activity,
  FlaskConical,
  Thermometer,
  ArrowRight,
  Lightbulb,
  Menu,
  X,
  RefreshCw,
} from "lucide-react";
import {
  api,
  type Tenant,
  type Pond,
  type Device,
  type Latest,
  type Summary,
  type Metric,
  type AlertEvent,
} from "../lib/api";
import { useSession } from "../lib/session";
import { Brand, Button, ErrorNotice, Empty } from "../components/ui";
import { WaterChart, metrics, formatNumber } from "../components/WaterChart";
export function Dashboard({ onContact }: { onContact: () => void }) {
  const { user, exit } = useSession();
  const [tenantId, setTenantId] = useState(
    () => localStorage.getItem(`limnopulse:tenant:${user!.id}`) || "",
  );
  const [menu, setMenu] = useState(false);
  const tenants = useQuery({
    queryKey: ["tenants", user!.id],
    queryFn: ({ signal }) => api<{ items: Tenant[] }>("/tenants", { signal }),
  });
  const tenant =
    tenants.data?.items.find((t) => t.tenant_id === tenantId) ||
    tenants.data?.items[0];
  function select(id: string) {
    setTenantId(id);
    localStorage.setItem(`limnopulse:tenant:${user!.id}`, id);
  }
  return (
    <div className="dashboard">
      <aside className={`sidebar ${menu ? "sidebar-open" : ""}`}>
        <Brand light tagline />
        <button
          className="sidebar-close"
          aria-label="Fechar navegação"
          onClick={() => setMenu(false)}
        >
          <X />
        </button>
        <nav aria-label="Navegação da operação">
          <a
            className="selected"
            href="#overview"
            onClick={() => setMenu(false)}
          >
            <House /> Visão geral
          </a>
          <a href="#ponds" onClick={() => setMenu(false)}>
            <Fish /> Viveiros
          </a>
          <a href="#devices" onClick={() => setMenu(false)}>
            <Wifi /> Dispositivos
          </a>
          <a href="#alerts" onClick={() => setMenu(false)}>
            <Bell /> Alertas
          </a>
          <Link to="/onboarding">
            <Settings /> Nova propriedade
          </Link>
        </nav>
        <div className="sidebar-support">
          <HelpCircle />
          <strong>Precisa de ajuda?</strong>
          <p>
            Fale com o time
            <br />
            do LimnoPulse.
          </p>
          <Button secondary onClick={onContact}>
            Falar com o suporte
          </Button>
        </div>
      </aside>
      <div className="dashboard-body">
        <header className="dashboard-header">
          <button
            className="menu-toggle"
            aria-label="Abrir navegação"
            onClick={() => setMenu(true)}
          >
            <Menu />
          </button>
          <label className="tenant-select">
            <House size={21} />
            <span className="sr-only">Propriedade</span>
            <select
              value={tenant?.tenant_id || ""}
              onChange={(e) => select(e.target.value)}
              aria-label="Propriedade"
            >
              {tenants.data?.items.map((t) => (
                <option key={t.tenant_id} value={t.tenant_id}>
                  {t.name}
                </option>
              ))}
              {!tenant && <option value="">Sua propriedade</option>}
            </select>
          </label>
          <div className="user-menu">
            <span className="avatar">
              {user!.email.slice(0, 2).toUpperCase()}
            </span>
            <span>
              {user!.email}
              <small>Sua operação</small>
            </span>
            <button
              className="icon-button"
              aria-label="Sair"
              onClick={() => void exit()}
            >
              <LogOut size={20} />
            </button>
          </div>
        </header>
        <main id="overview">
          {tenants.isPending ? (
            <Empty>Carregando propriedades…</Empty>
          ) : tenants.isError ? (
            <ErrorNotice
              error={tenants.error}
              retry={() => void tenants.refetch()}
            />
          ) : !tenant ? (
            <div className="card no-property">
              <h1>Bem-vindo ao LimnoPulse</h1>
              <p>Cadastre sua propriedade para começar a acompanhar a água.</p>
              <Link className="button" to="/onboarding">
                Configurar minha operação <ArrowRight />
              </Link>
            </div>
          ) : (
            <TenantOverview
              key={tenant.tenant_id}
              tenant={tenant}
              onContact={onContact}
            />
          )}
        </main>
      </div>
    </div>
  );
}
function TenantOverview({
  tenant,
  onContact,
}: {
  tenant: Tenant;
  onContact: () => void;
}) {
  const [pondId, setPondId] = useState(""),
    [metric, setMetric] = useState<Metric>("do_mg_l"),
    [period, setPeriod] = useState("24h");
  const base = `/tenants/${tenant.tenant_id}`;
  const ponds = useQuery({
    queryKey: ["ponds", tenant.tenant_id],
    queryFn: ({ signal }) =>
      api<{ items: Pond[] }>(`${base}/ponds`, { signal }),
  });
  const devices = useQuery({
    queryKey: ["devices", tenant.tenant_id],
    queryFn: ({ signal }) =>
      api<{ items: Device[] }>(`${base}/devices`, { signal }),
  });
  const alerts = useQuery({
    queryKey: ["alerts", tenant.tenant_id],
    queryFn: ({ signal }) =>
      api<{ items: AlertEvent[]; has_more: boolean }>(
        `${base}/alert-events/active?limit=100`,
        { signal },
      ),
    refetchInterval: 60000,
  });
  const pond =
    ponds.data?.items.find((p) => p.pond_id === pondId) || ponds.data?.items[0];
  const rows = ponds.data?.items || [];
  const latest = useQueries({
    queries: rows.map((p) => ({
      queryKey: ["latest", tenant.tenant_id, p.pond_id],
      queryFn: ({ signal }: { signal: AbortSignal }) =>
        api<Latest>(`${base}/ponds/${p.pond_id}/metrics/latest`, { signal }),
      refetchInterval: 60000,
    })),
  });
  const summary = useQuery({
    queryKey: ["summary", tenant.tenant_id, pond?.pond_id, period],
    queryFn: ({ signal }) =>
      api<Summary>(
        `${base}/ponds/${pond!.pond_id}/metrics/summary?period=${period}`,
        { signal },
      ),
    enabled: Boolean(pond),
    refetchInterval: 60000,
  });
  const current = latest[rows.findIndex((p) => p.pond_id === pond?.pond_id)];
  const stats = summary.data?.statistics[metric];
  const activeAlerts =
    alerts.data?.items.filter((a) =>
      ["open", "acknowledged"].includes(a.status),
    ) || [];
  const alertsTruncated = alerts.data?.has_more === true;
  const pondAlerts = activeAlerts.filter((a) => a.pond_id === pond?.pond_id);
  const value = current?.data?.[metric];
  const dataError = ponds.error || devices.error || alerts.error;
  const stale = current?.data?.measured_at
    ? Date.now() - new Date(current.data.measured_at).getTime() > 15 * 60000
    : true;
  return (
    <>
      <section className="dashboard-hero">
        <h1>Visão geral</h1>
        <p>Resumo da sua operação</p>
        <span>
          Dados de hoje.
          <br />
          Mais controle.
          <br />
          Mais resultados.
        </span>
      </section>
      <div className="dashboard-content">
        {dataError && (
          <ErrorNotice
            error={dataError}
            retry={() => {
              void ponds.refetch();
              void devices.refetch();
              void alerts.refetch();
            }}
          />
        )}
        <section className="stat-grid">
          <a className="card stat-card" href="#ponds">
            <span className="icon-square">
              <Fish />
            </span>
            <div>
              <h3>Viveiros</h3>
              <strong className="green">
                {ponds.data
                  ? rows.filter((p) => p.status === "active").length
                  : "—"}{" "}
                <small>ativos</small>
              </strong>
              <p>
                {ponds.data
                  ? `de ${rows.length} viveiros no total`
                  : "Carregando…"}
              </p>
            </div>
            <ArrowRight size={17} />
          </a>
          <a className="card stat-card" href="#devices">
            <span className="icon-square">
              <Wifi />
            </span>
            <div>
              <h3>Dispositivos</h3>
              <strong>{devices.data?.items.length ?? "—"}</strong>
              <p>Conexão não informada</p>
            </div>
            <ArrowRight size={17} />
          </a>
          <a className="card stat-card" href="#alerts">
            <span className="icon-square red">
              <Bell />
            </span>
            <div>
              <h3>Alertas</h3>
              <strong className={activeAlerts.length ? "red-text" : ""}>
                {alerts.isSuccess
                  ? alertsTruncated
                    ? `${activeAlerts.length}+`
                    : activeAlerts.length
                  : "—"}
              </strong>
              <p>{alerts.data ? "em aberto ou reconhecidos" : "Carregando…"}</p>
            </div>
            <ArrowRight size={17} />
          </a>
        </section>
        {ponds.isPending ? (
          <Empty>Carregando viveiros…</Empty>
        ) : !rows.length && !ponds.error ? (
          <Empty>Nenhum viveiro cadastrado nesta propriedade.</Empty>
        ) : (
          <div className="dashboard-columns">
            <section className="card quality-card">
              <div className="quality-heading">
                <Activity />
                <div>
                  <h2>Qualidade da água</h2>
                  <p>Acompanhe os principais parâmetros da sua operação.</p>
                </div>
                <button
                  className="icon-button"
                  aria-label="Atualizar métricas"
                  onClick={() => {
                    void summary.refetch();
                    void current?.refetch();
                  }}
                >
                  <RefreshCw size={17} />
                </button>
              </div>
              <div className="pond-selection">
                <label>
                  Viveiro
                  <select
                    aria-label="Viveiro"
                    value={pond?.pond_id || ""}
                    onChange={(e) => setPondId(e.target.value)}
                  >
                    {rows.map((p) => (
                      <option key={p.pond_id} value={p.pond_id}>
                        {p.name}
                      </option>
                    ))}
                  </select>
                </label>
                <small>
                  {current?.data?.measured_at
                    ? `Última leitura: ${new Date(current.data.measured_at).toLocaleString("pt-BR")}`
                    : "Sem leitura disponível"}
                </small>
              </div>
              <div
                className="metric-tabs"
                role="tablist"
                aria-label="Parâmetro"
              >
                {(["do_mg_l", "ph", "temp_c"] as Metric[]).map((m) => {
                  const Icon =
                    m === "ph"
                      ? FlaskConical
                      : m === "temp_c"
                        ? Thermometer
                        : Activity;
                  return (
                    <button
                      key={m}
                      role="tab"
                      aria-selected={metric === m}
                      className={metric === m ? "active" : ""}
                      onClick={() => setMetric(m)}
                    >
                      <Icon />
                      <span>
                        {metrics[m].name}
                        <small>
                          {metrics[m].unit || "Potencial hidrogeniônico"}
                        </small>
                      </span>
                    </button>
                  );
                })}
              </div>
              {current?.isError && (
                <ErrorNotice
                  error={current.error}
                  retry={() => void current.refetch()}
                />
              )}
              <div className="metric-summary">
                <div>
                  <small>Último valor</small>
                  <strong>
                    {current?.isPending ? "…" : formatNumber(value)}{" "}
                    <em>{metrics[metric].unit}</em>
                  </strong>
                  <span className={`badge ${stale ? "warning" : ""}`}>
                    {value == null
                      ? "Sem telemetria"
                      : stale
                        ? "Leitura antiga"
                        : alerts.isError
                          ? "Alertas indisponíveis"
                          : alerts.isPending
                            ? "Carregando alertas…"
                            : pondAlerts.length
                              ? "Ver alertas"
                              : "Sem alertas em aberto"}
                  </span>
                </div>
                <div>
                  <small>Média</small>
                  <strong>
                    {summary.isError ? "—" : formatNumber(stats?.mean)}
                  </strong>
                </div>
                <div>
                  <small>Mínimo</small>
                  <strong>
                    {summary.isError ? "—" : formatNumber(stats?.min)}
                  </strong>
                </div>
                <div>
                  <small>Máximo</small>
                  <strong>
                    {summary.isError ? "—" : formatNumber(stats?.max)}
                  </strong>
                </div>
              </div>
              <div className="chart-toolbar">
                <span>
                  {metrics[metric].short}{" "}
                  {metrics[metric].unit && `(${metrics[metric].unit})`}
                </span>
                <label>
                  <span className="sr-only">Período</span>
                  <select
                    aria-label="Período"
                    value={period}
                    onChange={(e) => setPeriod(e.target.value)}
                  >
                    <option value="24h">Últimas 24 horas</option>
                    <option value="7d">Últimos 7 dias</option>
                    <option value="30d">Últimos 30 dias</option>
                  </select>
                </label>
              </div>
              {summary.isPending ? (
                <Empty>Carregando histórico…</Empty>
              ) : summary.isError ? (
                <ErrorNotice
                  error={summary.error}
                  retry={() => void summary.refetch()}
                />
              ) : !summary.data?.series.some((p) => p[metric] != null) ? (
                <Empty>Sem telemetria neste período.</Empty>
              ) : (
                <WaterChart
                  data={summary.data.series}
                  metric={metric}
                  period={period}
                />
              )}
              <div className="quality-tip">
                <Lightbulb />
                <div>
                  <strong>
                    {pondAlerts.length
                      ? "Sua atenção faz a diferença"
                      : "Informação para decidir"}
                  </strong>
                  <p>
                    {pondAlerts.length
                      ? `${pondAlerts.length} alerta(s) requerem acompanhamento neste viveiro.`
                      : `${stats?.count ?? 0} amostras no período. Avalie o histórico e as condições da sua produção.`}
                  </p>
                </div>
              </div>
            </section>
            <div className="dashboard-right">
              <section className="card ponds-card" id="ponds">
                <div className="section-heading">
                  <Fish />
                  <div>
                    <h2>Viveiros</h2>
                    <p>Últimas leituras dos seus viveiros.</p>
                  </div>
                </div>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Viveiro</th>
                        <th>
                          O₂<small>(mg/L)</small>
                        </th>
                        <th>pH</th>
                        <th>
                          Temp.<small>(°C)</small>
                        </th>
                        <th>Alertas</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((p, i) => (
                        <tr
                          key={p.pond_id}
                          className={
                            pond?.pond_id === p.pond_id ? "selected-row" : ""
                          }
                        >
                          <th>
                            <button onClick={() => setPondId(p.pond_id)}>
                              {p.name}
                            </button>
                          </th>
                          <td>{formatNumber(latest[i]?.data?.do_mg_l)}</td>
                          <td>{formatNumber(latest[i]?.data?.ph)}</td>
                          <td>{formatNumber(latest[i]?.data?.temp_c)}</td>
                          <td>
                            <span
                              className={`badge ${activeAlerts.some((a) => a.pond_id === p.pond_id) ? "warning" : ""}`}
                            >
                              {!alerts.isSuccess
                                ? "—"
                                : activeAlerts.filter(
                                    (a) => a.pond_id === p.pond_id,
                                  ).length || "Nenhum"}
                            </span>
                            {latest[i]?.isError && (
                              <span title="Falha ao carregar leitura"> ⚠</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
              <section className="dashboard-photo">
                <span className="eyebrow">
                  TECNOLOGIA A SERVIÇO DO SEU RESULTADO
                </span>
                <h2>
                  Água saudável,
                  <br />
                  produção sustentável.
                </h2>
                <p>
                  Monitore, previna e tome decisões
                  <br />
                  com base em dados confiáveis.
                </p>
                <Button onClick={onContact}>
                  Falar com o time <ArrowRight size={17} />
                </Button>
              </section>
            </div>
          </div>
        )}
        <div className="operation-details">
          <details className="card" id="devices">
            <summary>
              Dispositivos ({devices.data?.items.length ?? "—"})
            </summary>
            <p>A API informa o cadastro, mas não a conexão online.</p>
            {devices.data?.items.map((d) => (
              <p key={d.device_id}>
                <strong>{d.name}</strong> ·{" "}
                {d.status === "active" ? "Cadastro ativo" : d.status} · Conexão
                não informada
              </p>
            ))}
          </details>
          <details className="card" id="alerts">
            <summary>
              Alertas da operação (
              {alerts.isSuccess
                ? alertsTruncated
                  ? `${activeAlerts.length}+`
                  : activeAlerts.length
                : "—"})
            </summary>
            {alertsTruncated && (
              <p role="status">
                Exibindo os 100 alertas mais recentes; as contagens podem estar
                incompletas.
              </p>
            )}
            {alerts.isError ? (
              <ErrorNotice error={alerts.error} />
            ) : alerts.isPending ? (
              <p>Carregando alertas…</p>
            ) : activeAlerts.length ? (
              activeAlerts.map((a) => (
                <p key={a.event_id}>
                  {rows.find((p) => p.pond_id === a.pond_id)?.name || a.pond_id}{" "}
                  · {a.metric} ·{" "}
                  {a.status === "open" ? "Em aberto" : "Reconhecido"}
                </p>
              ))
            ) : (
              <p>Nenhum alerta em aberto.</p>
            )}
          </details>
        </div>
      </div>
    </>
  );
}
