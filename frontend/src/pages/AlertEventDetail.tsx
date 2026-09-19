import { Link, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api, type AlertEvent } from "../lib/api";
import { Brand, Empty, ErrorNotice } from "../components/ui";

export function AlertEventDetail() {
  const { tenantId, eventId } = useParams();
  const event = useQuery({
    queryKey: ["alert-event", tenantId, eventId],
    queryFn: ({ signal }) =>
      api<AlertEvent>(`/tenants/${tenantId}/alert-events/${eventId}`, {
        signal,
      }),
    enabled: Boolean(tenantId && eventId),
  });

  return (
    <main className="empty-state" aria-labelledby="alert-detail-title">
      <Brand />
      {event.isPending ? (
        <Empty>Carregando alerta…</Empty>
      ) : event.isError ? (
        <ErrorNotice error={event.error} retry={() => void event.refetch()} />
      ) : (
        <article className="card">
          <p className="eyebrow">Detalhe do alerta</p>
          <h1 id="alert-detail-title">{event.data.rule_name || "Alerta"}</h1>
          <p>{event.data.metric || "Métrica não informada"}</p>
          <dl>
            <div>
              <dt>Status</dt>
              <dd>{event.data.status}</dd>
            </div>
            <div>
              <dt>Viveiro</dt>
              <dd>{event.data.pond_id}</dd>
            </div>
            <div>
              <dt>Severidade</dt>
              <dd>{event.data.severity || "—"}</dd>
            </div>
          </dl>
          <Link to="/app">Voltar para a visão geral</Link>
        </article>
      )}
    </main>
  );
}
