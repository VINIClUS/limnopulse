import { useId } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import type { Metric } from "../lib/api";
export const formatNumber = (value: number | null | undefined) =>
  value == null
    ? "—"
    : value.toLocaleString("pt-BR", {
        minimumFractionDigits: 1,
        maximumFractionDigits: 1,
      });
export const metrics: Record<
  Metric,
  { name: string; unit: string; short: string }
> = {
  do_mg_l: { name: "Oxigênio dissolvido", unit: "mg/L", short: "O₂" },
  ph: { name: "pH", unit: "", short: "pH" },
  temp_c: { name: "Temperatura", unit: "°C", short: "Temp." },
};
export function WaterChart({
  data,
  metric = "do_mg_l",
  dark = false,
  compact = false,
  period = "24h",
}: {
  data: ({ measured_at: string } & Partial<Record<Metric, number | null>>)[];
  metric?: Metric;
  dark?: boolean;
  compact?: boolean;
  period?: string;
}) {
  const id = useId().replace(/:/g, "");
  return (
    <div
      className={`water-chart ${compact ? "compact" : ""}`}
      role="img"
      aria-label={`Gráfico de ${metrics[metric].name}: ${data.length} pontos no período ${period}`}
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart
          data={data}
          margin={{ top: 12, right: 12, bottom: 0, left: -18 }}
        >
          <defs>
            <linearGradient id={id} x1="0" y1="0" x2="0" y2="1">
              <stop
                offset="0%"
                stopColor={dark ? "#26d6ed" : "#0782f5"}
                stopOpacity={0.27}
              />
              <stop offset="100%" stopColor="#36c8ed" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid
            strokeDasharray="0"
            stroke={dark ? "#ffffff14" : "#e9eff4"}
          />
          <XAxis
            dataKey="measured_at"
            minTickGap={32}
            tickFormatter={(v) =>
              new Date(v).toLocaleString(
                "pt-BR",
                period === "24h"
                  ? { hour: "2-digit", minute: "2-digit" }
                  : { day: "2-digit", month: "2-digit" },
              )
            }
            tick={{ fill: dark ? "#bdd8e8" : "#60738b", fontSize: 11 }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            width={48}
            tick={{ fill: dark ? "#bdd8e8" : "#60738b", fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            domain={metric === "ph" ? [0, 14] : ["auto", "auto"]}
          />
          <Tooltip
            formatter={(v) => [
              `${formatNumber(Number(v))} ${metrics[metric].unit}`,
              metrics[metric].name,
            ]}
            labelFormatter={(v) => new Date(String(v)).toLocaleString("pt-BR")}
            contentStyle={{
              background: dark ? "#082a47" : "#fff",
              border: "1px solid #bfd4e4",
              borderRadius: 8,
              color: dark ? "white" : "#09243d",
            }}
          />
          <Area
            type="monotone"
            dataKey={metric}
            stroke={dark ? "#22d0ef" : "#087ff5"}
            strokeWidth={2.5}
            fill={`url(#${id})`}
            isAnimationActive={false}
            connectNulls={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
export const demoSeries = Array.from({ length: 49 }, (_, i) => ({
  measured_at: new Date(Date.UTC(2026, 8, 19, 0, i * 30)).toISOString(),
  do_mg_l: 5.8 + Math.sin(i * 0.38) * 0.8 + Math.sin(i * 1.4) * 0.22,
  ph: 7.3,
  temp_c: 28.1,
}));
export function DemoMonitor({ small = false }: { small?: boolean }) {
  return (
    <div className={`demo-monitor ${small ? "small-monitor" : ""}`}>
      <div className="demo-heading">
        <div>
          <strong>{small ? "Viveiro 03" : "Fazenda Panorama"}</strong>
          <small>Panorama – SP</small>
        </div>
        <span className="badge">● Demonstração</span>
      </div>
      <div className="demo-metrics">
        {(["do_mg_l", "ph", "temp_c"] as Metric[]).map((m) => (
          <div key={m}>
            <small>{metrics[m].short}</small>
            <strong>
              {formatNumber({ do_mg_l: 6.4, ph: 7.3, temp_c: 28.1 }[m])}
              <em>{metrics[m].unit}</em>
            </strong>
            {!small && <span>Valor ilustrativo</span>}
          </div>
        ))}
      </div>
      <div className="demo-chart">
        <div>
          <small>Oxigênio dissolvido (mg/L)</small>
          <span>24h</span>
        </div>
        <WaterChart data={demoSeries} dark compact />
      </div>
      {!small && (
        <div className="demo-status">
          <span>✓</span>
          <div>
            Da água à decisão
            <small>Conheça sua operação com mais clareza.</small>
          </div>
        </div>
      )}
    </div>
  );
}
