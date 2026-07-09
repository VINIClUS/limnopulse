from __future__ import annotations

import json
from typing import Any

from anyio import to_thread

from limnopulse_api.domain.telemetry import LatestMetrics, TelemetryReading, validate_flux_time_bound


class InfluxTelemetryRepository:
    def __init__(self, *, query_api: Any, org: str, bucket: str) -> None:
        self.query_api = query_api
        self.org = org
        self.bucket = bucket

    async def query_readings(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: str,
        stop: str | None,
        limit: int,
    ) -> list[TelemetryReading]:
        query = self._readings_query(
            tenant_id=tenant_id,
            pond_id=pond_id,
            start=start,
            stop=stop,
            limit=limit,
        )
        tables = await to_thread.run_sync(self.query_api.query, query, self.org)
        return [
            self._reading_from_values(record.values)
            for table in tables
            for record in table.records
        ]

    async def query_latest_metrics(self, *, tenant_id: str, pond_id: str) -> LatestMetrics:
        query = self._latest_query(tenant_id=tenant_id, pond_id=pond_id)
        tables = await to_thread.run_sync(self.query_api.query, query, self.org)
        latest_values: dict[str, Any] | None = None
        for table in tables:
            for record in table.records:
                if latest_values is None or record.values.get("_time") > latest_values.get("_time"):
                    latest_values = record.values
        if latest_values is not None:
            return self._latest_from_values(latest_values, tenant_id=tenant_id, pond_id=pond_id)
        return LatestMetrics(tenant_id=tenant_id, pond_id=pond_id)

    def _readings_query(
        self,
        *,
        tenant_id: str,
        pond_id: str,
        start: str,
        stop: str | None,
        limit: int,
    ) -> str:
        return "\n".join(
            [
                f"from(bucket: {self._flux_string(self.bucket)})",
                f"  |> range({self._range_args(start=start, stop=stop)})",
                self._water_quality_filters(tenant_id=tenant_id, pond_id=pond_id),
                '  |> pivot(rowKey:["_time", "tenant_id", "pond_id", "device_id"], columnKey: ["_field"], valueColumn: "_value")',
                '  |> sort(columns: ["_time"])',
                f"  |> limit(n: {limit})",
            ]
        )

    def _latest_query(self, *, tenant_id: str, pond_id: str) -> str:
        return "\n".join(
            [
                f"from(bucket: {self._flux_string(self.bucket)})",
                "  |> range(start: -24h)",
                self._water_quality_filters(tenant_id=tenant_id, pond_id=pond_id),
                "  |> last()",
                '  |> pivot(rowKey:["_time", "tenant_id", "pond_id"], columnKey: ["_field"], valueColumn: "_value")',
                '  |> sort(columns: ["_time"], desc: true)',
                "  |> limit(n: 1)",
            ]
        )

    def _water_quality_filters(self, *, tenant_id: str, pond_id: str) -> str:
        return "\n".join(
            [
                '  |> filter(fn: (r) => r["_measurement"] == "water_quality")',
                f'  |> filter(fn: (r) => r["tenant_id"] == {self._flux_string(tenant_id)})',
                f'  |> filter(fn: (r) => r["pond_id"] == {self._flux_string(pond_id)})',
            ]
        )

    def _range_args(self, *, start: str, stop: str | None) -> str:
        args = [f"start: {self._flux_time_bound(start)}"]
        if stop is not None:
            args.append(f"stop: {self._flux_time_bound(stop)}")
        return ", ".join(args)

    def _flux_time_bound(self, value: str) -> str:
        validated = validate_flux_time_bound(value)
        if validated.startswith("-"):
            return validated
        return f"time(v: {self._flux_string(validated)})"

    def _flux_string(self, value: str) -> str:
        return json.dumps(value)

    def _reading_from_values(self, values: dict[str, Any]) -> TelemetryReading:
        return TelemetryReading(
            measured_at=values["_time"],
            tenant_id=str(values["tenant_id"]),
            pond_id=str(values["pond_id"]),
            device_id=str(values["device_id"]),
            **self._water_quality_values(values),
        )

    def _latest_from_values(
        self,
        values: dict[str, Any],
        *,
        tenant_id: str,
        pond_id: str,
    ) -> LatestMetrics:
        return LatestMetrics(
            measured_at=values.get("_time"),
            tenant_id=str(values.get("tenant_id", tenant_id)),
            pond_id=str(values.get("pond_id", pond_id)),
            **self._water_quality_values(values),
        )

    def _water_quality_values(self, values: dict[str, Any]) -> dict[str, Any]:
        return {
            field_name: values[field_name]
            for field_name in (
                "temp_c",
                "ph",
                "do_mg_l",
                "turbidity_ntu",
                "salinity_ppt",
                "battery_v",
                "rssi",
                "seq",
            )
            if field_name in values and values[field_name] is not None
        }
