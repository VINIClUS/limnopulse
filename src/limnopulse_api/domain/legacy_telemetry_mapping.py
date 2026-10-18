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


_MAX_EXCERPT_DEPTH = 32
_MAX_EXCERPT_NODES = 4096
_MAX_EXCERPT_INTEGER_BITS = 256


class _Excerpter:
    """Builds a strict-JSON-safe copy with string keys, finite numbers and bounded size.

    Cycles, depth beyond `_MAX_EXCERPT_DEPTH` and more than `_MAX_EXCERPT_NODES` nodes are cut
    with a marker and set `cut`, so cyclic or heavily shared structures finish in bounded time.
    """

    def __init__(self) -> None:
        self.nodes = 0
        self.cut = False
        self._path: set[int] = set()

    def _marker(self, text: str) -> str:
        self.cut = True
        return text

    def copy(self, value: object, depth: int = 0) -> object:
        self.nodes += 1
        if self.nodes > _MAX_EXCERPT_NODES:
            return self._marker("<truncated>")
        if depth > _MAX_EXCERPT_DEPTH:
            return self._marker("<nested too deeply>")
        if value is None or isinstance(value, bool | str):
            return value
        if isinstance(value, int):
            return value if value.bit_length() <= _MAX_EXCERPT_INTEGER_BITS else _int_text(value)
        if isinstance(value, float):
            return value if math.isfinite(value) else repr(value)
        if isinstance(value, Mapping | list | tuple):
            if id(value) in self._path:
                return self._marker("<cycle>")
            self._path.add(id(value))
            try:
                if isinstance(value, Mapping):
                    return {
                        _key_text(key): self.copy(item, depth + 1) for key, item in value.items()
                    }
                return [self.copy(item, depth + 1) for item in value]
            finally:
                self._path.discard(id(value))
        return str(value)


def _int_text(value: int) -> str:
    return f"<integer of {value.bit_length()} bits>"


def _key_text(key: object) -> str:
    if isinstance(key, int) and not isinstance(key, bool):
        return str(key) if key.bit_length() <= _MAX_EXCERPT_INTEGER_BITS else _int_text(key)
    return str(key)


def _utf8_size(text: str) -> int:
    return len(text.encode("utf-8"))


def bounded_excerpt(source: object) -> tuple[str, bool]:
    """JSON of `source` within `MAX_QUARANTINE_EXCERPT_BYTES` UTF-8 bytes; flags truncation.

    Never raises for any payload value: non-JSON values are stringified, non-finite floats
    become strings, lone surrogates are escaped so the text is always encodable, and cycles or
    oversized structures are cut (and flagged as truncated). A
    truncated excerpt stays valid JSON: it is a JSON string holding the longest prefix of the
    full serialization that fits.
    """
    excerpter = _Excerpter()
    safe = excerpter.copy(source)
    text = json.dumps(safe, ensure_ascii=False, sort_keys=True, allow_nan=False)
    text = text.encode("utf-8", errors="backslashreplace").decode("utf-8")
    if _utf8_size(text) <= MAX_QUARANTINE_EXCERPT_BYTES:
        return text, excerpter.cut
    low, high = 0, len(text)
    while low < high:  # longest prefix whose JSON string literal fits the bound
        middle = (low + high + 1) // 2
        if (
            _utf8_size(json.dumps(text[:middle], ensure_ascii=False))
            <= MAX_QUARANTINE_EXCERPT_BYTES
        ):
            low = middle
        else:
            high = middle - 1
    return json.dumps(text[:low], ensure_ascii=False), True


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

    if not isinstance(payload, Mapping):
        record = quarantine(
            QuarantineReason.OBSERVED_AT_INVALID, "payload must be a JSON object", payload
        )
        return LegacyMappingResult(observations=(), quarantined=(record,))
    try:
        observed_at = _TIMESTAMP.validate_python(payload.get("ts"))
    except ValueError as exc:  # pydantic ValidationError, or an oversized integer input
        if isinstance(exc, ValidationError):
            detail = "; ".join(str(error["msg"]) for error in exc.errors())
        else:
            detail = "ts must be an ISO-8601 timestamp string"
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
