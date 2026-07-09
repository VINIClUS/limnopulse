from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from typing import Any

import httpx

from limnopulse_api.core.errors import TelemetryQueryError
from limnopulse_api.domain.telemetry import MetricValue, TelemetryReading


class InfluxTelemetryRepository:
    def __init__(
        self,
        *,
        base_url: str,
        org: str,
        bucket: str,
        token: str,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.org = org
        self.bucket = bucket
        self.token = token
        self.timeout_seconds = timeout_seconds
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(base_url=base_url.rstrip("/"))

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def list_readings(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        limit: int,
        fields: tuple[str, ...],
    ) -> list[TelemetryReading]:
        csv_text = await self._query(
            self._readings_query(
                tenant_id=tenant_id,
                pond_id=pond_id,
                start=start,
                stop=stop,
                limit=limit,
                fields=fields,
            )
        )
        return self._readings_from_csv(csv_text, fields=fields)

    async def latest_metrics(
        self,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        fields: tuple[str, ...],
    ) -> TelemetryReading | None:
        csv_text = await self._query(
            self._latest_metrics_query(
                tenant_id=tenant_id,
                pond_id=pond_id,
                start=start,
                stop=stop,
                fields=fields,
            )
        )
        readings = self._readings_from_csv(csv_text, fields=fields)
        if not readings:
            return None
        return readings[0]

    async def _query(self, query: str) -> str:
        headers = {
            "Accept": "application/csv",
            "Content-Type": "application/vnd.flux",
        }
        if self.token:
            headers["Authorization"] = f"Token {self.token}"

        try:
            response = await self.client.post(
                "/api/v2/query",
                params={"org": self.org},
                content=query,
                headers=headers,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise TelemetryQueryError("InfluxDB query failed") from exc
        return response.text

    def _readings_query(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        limit: int,
        fields: tuple[str, ...],
    ) -> str:
        range_filter = (
            f"|> range(start: time(v: {self._flux_string(self._flux_time(start))}), "
            f"stop: time(v: {self._flux_string(self._flux_time(stop))}))"
        )
        pivot = (
            '|> pivot(rowKey: ["_time", "tenant_id", "pond_id", "device_id"], '
            'columnKey: ["_field"], valueColumn: "_value")'
        )
        return f"""from(bucket: {self._flux_string(self.bucket)})
  {range_filter}
  |> filter(fn: (r) => r["_measurement"] == "water_quality")
  |> filter(fn: (r) => r["tenant_id"] == {self._flux_string(tenant_id)})
  |> filter(fn: (r) => r["pond_id"] == {self._flux_string(pond_id)})
  |> filter(fn: (r) => contains(value: r["_field"], set: {self._flux_field_set(fields)}))
  {pivot}
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})"""

    def _latest_metrics_query(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: datetime,
        stop: datetime,
        fields: tuple[str, ...],
    ) -> str:
        range_filter = (
            f"|> range(start: time(v: {self._flux_string(self._flux_time(start))}), "
            f"stop: time(v: {self._flux_string(self._flux_time(stop))}))"
        )
        pivot = (
            '|> pivot(rowKey: ["_time", "tenant_id", "pond_id", "device_id"], '
            'columnKey: ["_field"], valueColumn: "_value")'
        )
        return f"""from(bucket: {self._flux_string(self.bucket)})
  {range_filter}
  |> filter(fn: (r) => r["_measurement"] == "water_quality")
  |> filter(fn: (r) => r["tenant_id"] == {self._flux_string(tenant_id)})
  |> filter(fn: (r) => r["pond_id"] == {self._flux_string(pond_id)})
  |> filter(fn: (r) => contains(value: r["_field"], set: {self._flux_field_set(fields)}))
  {pivot}
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)"""

    def _readings_from_csv(
        self, csv_text: str, *, fields: tuple[str, ...]
    ) -> list[TelemetryReading]:
        data_lines = [line for line in csv_text.splitlines() if line and not line.startswith("#")]
        if not data_lines:
            return []

        readings: list[TelemetryReading] = []
        reader = csv.DictReader(data_lines)
        for row in reader:
            timestamp_value = row.get("_time")
            if not timestamp_value:
                continue

            metrics: dict[str, MetricValue] = {}
            for field_name in fields:
                parsed_value = self._parse_metric_value(row.get(field_name))
                if parsed_value is not None:
                    metrics[field_name] = parsed_value
            if not metrics:
                continue

            readings.append(
                TelemetryReading(
                    timestamp=self._parse_timestamp(timestamp_value),
                    tenant_id=row.get("tenant_id") or "",
                    pond_id=row.get("pond_id") or "",
                    device_id=row.get("device_id") or None,
                    metrics=metrics,
                )
            )
        return readings

    def _parse_metric_value(self, value: str | None) -> MetricValue | None:
        if value is None or value == "":
            return None
        number = float(value)
        if number.is_integer() and "." not in value and "e" not in value.lower():
            return int(number)
        return number

    def _parse_timestamp(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _flux_time(self, value: datetime) -> str:
        if value.tzinfo is not None and value.utcoffset() is not None:
            value = value.astimezone(UTC)
        return value.isoformat().replace("+00:00", "Z")

    def _flux_field_set(self, fields: tuple[str, ...]) -> str:
        return "[" + ", ".join(self._flux_string(field) for field in fields) + "]"

    def _flux_string(self, value: str) -> str:
        return json.dumps(value)

    def __repr__(self) -> str:
        return f"InfluxTelemetryRepository(org={self.org!r}, bucket={self.bucket!r})"
