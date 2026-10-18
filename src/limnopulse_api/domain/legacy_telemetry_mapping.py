"""Legacy MQTT reading -> `SourceObservation`s (P2-06); pure, no I/O.

Tenant and device come only from the trusted context, and the Component is the legacy default
Component of that device. Payload keys other than `ts`, `seq` and the catalog `legacy_field`s,
including any tenant, pond, asset, site or deployment claim, are ignored.
"""

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import TypeAdapter, ValidationError

from limnopulse_api.domain.ids import default_component_id_for_legacy_device
from limnopulse_api.domain.metric_catalog import metric_catalog
from limnopulse_api.domain.telemetry_v2 import (
    MAX_QUARANTINE_EXCERPT_BYTES,
    MAX_SEQUENCE,
    QuarantinedObservation,
    QuarantineReason,
    SourceObservation,
    TelemetryDatetime,
    TrustedSourceContext,
)

MAX_QUARANTINE_DETAIL_LENGTH = 256

_TIMESTAMP = TypeAdapter(TelemetryDatetime)


@dataclass(frozen=True)
class LegacyMappingResult:
    observations: tuple[SourceObservation, ...]
    quarantined: tuple[QuarantinedObservation, ...]


def bounded_excerpt(source: object) -> tuple[str, bool]:
    """JSON of `source`, cut to `MAX_QUARANTINE_EXCERPT_BYTES` UTF-8 bytes; flags truncation."""
    text = json.dumps(source, ensure_ascii=False, sort_keys=True, default=str)
    encoded = text.encode("utf-8")
    if len(encoded) <= MAX_QUARANTINE_EXCERPT_BYTES:
        return text, False
    # A cut inside a multi-byte character drops that character, never exceeding the bound.
    return encoded[:MAX_QUARANTINE_EXCERPT_BYTES].decode("utf-8", errors="ignore"), True


def bounded_detail(detail: str) -> str:
    return detail[:MAX_QUARANTINE_DETAIL_LENGTH]


def _is_valid_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return False
    return not (isinstance(value, float) and not math.isfinite(value))


def _is_valid_sequence(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_SEQUENCE


def map_legacy_reading(
    context: TrustedSourceContext, payload: Mapping[str, Any]
) -> LegacyMappingResult:
    component_id = default_component_id_for_legacy_device(context.tenant_id, context.device_id)

    def quarantine(
        reason: QuarantineReason,
        detail: str,
        source: object,
        *,
        metric_key: str | None = None,
        observed_at: datetime | None = None,
    ) -> QuarantinedObservation:
        excerpt, truncated = bounded_excerpt(source)
        return QuarantinedObservation(
            reason=reason,
            tenant_id=context.tenant_id,
            device_id=context.device_id,
            component_id=component_id,
            metric_key=metric_key,
            observed_at=observed_at,
            received_at=context.received_at,
            source_event_id=None,
            detail=bounded_detail(detail),
            source_excerpt_json=excerpt,
            truncated=truncated,
        )

    try:
        observed_at = _TIMESTAMP.validate_python(payload.get("ts"))
    except ValidationError as exc:
        detail = "; ".join(str(error["msg"]) for error in exc.errors())
        record = quarantine(QuarantineReason.OBSERVED_AT_INVALID, detail, dict(payload))
        return LegacyMappingResult(observations=(), quarantined=(record,))

    quarantined: list[QuarantinedObservation] = []
    sequence: int | None = None
    seq = payload.get("seq")
    if _is_valid_sequence(seq):
        sequence = seq
    elif seq is not None:
        quarantined.append(
            quarantine(
                QuarantineReason.VALUE_INVALID,
                f"seq must be an integer in [0, {MAX_SEQUENCE}]",
                {"seq": seq},
                observed_at=observed_at,
            )
        )

    observations: list[SourceObservation] = []
    for metric in metric_catalog():
        if metric.legacy_field is None:
            continue
        value = payload.get(metric.legacy_field)
        if value is None:
            continue
        if not _is_valid_number(value):
            quarantined.append(
                quarantine(
                    QuarantineReason.VALUE_INVALID,
                    f"{metric.legacy_field} must be a finite number",
                    {metric.legacy_field: value},
                    metric_key=metric.metric_key,
                    observed_at=observed_at,
                )
            )
            continue
        observations.append(
            SourceObservation(
                component_id=component_id,
                metric_key=metric.metric_key,
                value=value,
                source_unit=metric.canonical_unit,
                observed_at=observed_at,
                sequence=sequence,
            )
        )
    return LegacyMappingResult(observations=tuple(observations), quarantined=tuple(quarantined))
