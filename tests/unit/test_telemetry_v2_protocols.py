import dataclasses
import inspect
import typing
from collections.abc import Sequence
from datetime import datetime, timedelta
from inspect import Parameter
from pathlib import Path
from typing import Literal

import pytest
from limnopulse_api.domain.telemetry_v2 import (
    CanonicalObservation,
    QualityState,
    StoredObservation,
)
from limnopulse_api.domain.telemetry_v2_schema import LegacyPoint
from limnopulse_api.repositories.telemetry_v2 import (
    LegacyObservationWriter,
    ObservationReader,
    ObservationSelector,
    ObservationWriter,
)

from limnopulse_api.core.errors import LimnopulseError, TelemetryWriteError

ROOT = Path(__file__).resolve().parents[2]
P = Parameter.POSITIONAL_OR_KEYWORD
K = Parameter.KEYWORD_ONLY

OBSERVATION_WRITER = {
    "write_observations": (
        [("observations", P, Sequence[CanonicalObservation])],
        type(None),
    ),
}
LEGACY_OBSERVATION_WRITER = {
    "write_legacy_points": ([("points", P, Sequence[LegacyPoint])], type(None)),
}
OBSERVATION_READER = {
    "query_observations": (
        [
            ("tenant_id", K, str),
            ("selector", K, ObservationSelector),
            ("metric_keys", K, frozenset[str] | None),
            ("quality_states", K, frozenset[QualityState] | None),
            ("start", K, datetime),
            ("stop", K, datetime),
            ("limit", K, int),
        ],
        list[StoredObservation],
    ),
    "query_latest_observations": (
        [
            ("tenant_id", K, str),
            ("selector", K, ObservationSelector),
            ("metric_keys", K, frozenset[str] | None),
            ("lookback", K, timedelta),
            ("now", K, datetime),
        ],
        list[StoredObservation],
    ),
}

PROTOCOLS = [
    (ObservationWriter, OBSERVATION_WRITER),
    (LegacyObservationWriter, LEGACY_OBSERVATION_WRITER),
    (ObservationReader, OBSERVATION_READER),
]


def protocol_methods(protocol: type) -> set[str]:
    return {
        name
        for name in dir(protocol)
        if not name.startswith("_") and inspect.iscoroutinefunction(getattr(protocol, name))
    }


@pytest.mark.parametrize(("protocol", "expected"), PROTOCOLS)
def test_protocol_method_set_is_frozen(protocol, expected) -> None:
    assert protocol_methods(protocol) == set(expected)
    public = {name for name in vars(protocol) if not name.startswith("_")}
    assert public == set(expected)


@pytest.mark.parametrize(("protocol", "expected"), PROTOCOLS)
def test_protocol_signatures_match_names_kinds_and_types(protocol, expected) -> None:
    for name, (parameters, return_type) in expected.items():
        method = getattr(protocol, name)
        assert inspect.iscoroutinefunction(method), name
        signature = inspect.signature(method)
        hints = typing.get_type_hints(method)
        actual = [
            (param.name, param.kind, hints[param.name])
            for param in signature.parameters.values()
            if param.name != "self"
        ]
        assert actual == parameters, name
        assert all(param.default is Parameter.empty for param in signature.parameters.values()), (
            name
        )
        assert hints["return"] == return_type, name


@pytest.mark.parametrize(("protocol", "_expected"), PROTOCOLS)
def test_protocols_are_runtime_checkable(protocol, _expected) -> None:
    assert getattr(protocol, "_is_runtime_protocol", False)
    assert getattr(protocol, "_is_protocol", False)


def _fake_class(name: str, methods: set[str]) -> type:
    async def method(self, *args, **kwargs):
        return None

    return type(name, (), {method_name: method for method_name in methods})


@pytest.mark.parametrize(("protocol", "expected"), PROTOCOLS)
def test_isinstance_accepts_complete_fakes_and_rejects_incomplete_ones(protocol, expected) -> None:
    complete = _fake_class("FakeComplete", set(expected))()
    assert isinstance(complete, protocol)

    for missing in expected:
        incomplete = _fake_class("FakeIncomplete", set(expected) - {missing})()
        assert not isinstance(incomplete, protocol), missing


def test_v2_protocol_module_does_not_reference_provider_identity_or_cache_terms() -> None:
    forbidden = ("redis", "aws_iot", "thing_name", "certificate_arn", "shadow_name")
    paths = [
        ROOT / "src/limnopulse_api/repositories/telemetry_v2.py",
        ROOT / "src/limnopulse_api/domain/telemetry_v2.py",
        ROOT / "src/limnopulse_api/domain/metric_catalog.py",
        ROOT / "src/limnopulse_api/domain/telemetry_v2_schema.py",
    ]
    for path in paths:
        text = path.read_text(encoding="utf-8").lower()
        assert not [term for term in forbidden if term in text], path


# --- selector -------------------------------------------------------------------------------


def test_observation_selector_is_frozen_dataclass() -> None:
    assert dataclasses.is_dataclass(ObservationSelector)
    fields = dataclasses.fields(ObservationSelector)
    assert [field.name for field in fields] == ["kind", "entity_id"]
    hints = typing.get_type_hints(ObservationSelector)
    assert hints["kind"] == Literal["asset", "component", "device"]
    assert hints["entity_id"] is str
    selector = ObservationSelector(kind="asset", entity_id="ast_" + "1" * 32)
    with pytest.raises(dataclasses.FrozenInstanceError):
        selector.entity_id = "ast_" + "2" * 32  # type: ignore[misc]


@pytest.mark.parametrize(
    ("kind", "entity_id"),
    [
        ("asset", "ast_" + "1" * 32),
        ("component", "cmp_" + "1" * 32),
        ("device", "local-device-001"),
        ("device", "dev_" + "1" * 32),
    ],
)
def test_observation_selector_accepts_valid_entities(kind, entity_id) -> None:
    selector = ObservationSelector(kind=kind, entity_id=entity_id)
    assert (selector.kind, selector.entity_id) == (kind, entity_id)


@pytest.mark.parametrize(
    ("kind", "entity_id"),
    [
        ("asset", "cmp_" + "1" * 32),
        ("asset", "pond_local_001"),
        ("component", "ast_" + "1" * 32),
        ("component", "cmp_" + "1" * 31),
        ("device", "dev_short"),
        ("device", "bad device"),
        ("device", "bad#device"),
        ("site", "site_" + "1" * 32),
        ("probe", "prb_" + "1" * 32),
    ],
)
def test_observation_selector_rejects_invalid_entities(kind, entity_id) -> None:
    with pytest.raises(ValueError):
        ObservationSelector(kind=kind, entity_id=entity_id)


# --- write error ----------------------------------------------------------------------------


@pytest.mark.parametrize("retryable", [True, False])
def test_telemetry_write_error_carries_keyword_only_retryable(retryable) -> None:
    error = TelemetryWriteError("influx write failed", retryable=retryable)
    assert isinstance(error, LimnopulseError)
    assert error.retryable is retryable
    assert str(error) == "influx write failed"


def test_telemetry_write_error_rejects_positional_retryable() -> None:
    with pytest.raises(TypeError):
        TelemetryWriteError("influx write failed", True)  # type: ignore[misc]
    parameter = inspect.signature(TelemetryWriteError).parameters["retryable"]
    assert parameter.kind is Parameter.KEYWORD_ONLY
