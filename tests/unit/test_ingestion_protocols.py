"""P3-00 ports, FastAPI dependency getters and the moved redacted-validation route class."""

import inspect
import typing
from collections.abc import Sequence
from datetime import datetime
from inspect import Parameter
from typing import Annotated, Any

import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from limnopulse_api.api import ingestion_dependencies
from limnopulse_api.api.redacted_validation import _RedactedValidationRoute
from limnopulse_api.api.v2.routers import integrations as integrations_router
from limnopulse_api.domain import ingestion as domain
from limnopulse_api.domain.alerts import AuditContext
from limnopulse_api.domain.devices_v2 import DeviceV2
from limnopulse_api.domain.ingestion import (
    AuthenticatedSource,
    ComponentIngestState,
    ConnectorHealth,
    IngestCredential,
    IngestMarks,
    IngestObservationBatch,
    IngressAdmission,
    IngressDefer,
    IngressDenial,
    IngressEnvelope,
    IngressFence,
    IngressQueueMessage,
    IngressReceipt,
    QuarantineEntry,
    QuarantineEntryStatus,
    QuarantinePage,
    TrustedIngress,
)
from limnopulse_api.domain.integrations import (
    DeviceIntegration,
    DeviceIntegrationStatus,
    IntegrationAccount,
    IntegrationAccountStatus,
)
from limnopulse_api.repositories import ingestion as ports

P = Parameter.POSITIONAL_OR_KEYWORD
K = Parameter.KEYWORD_ONLY
T = ("tenant_id", P, str)
IA = ("integration_account_id", P, str)

# protocol -> {method: ([(name, kind, annotation), ...], return annotation, is_async)}
PROTOCOLS: dict[str, dict[str, tuple[list[tuple[str, Any, Any]], Any, bool]]] = {
    "IngestCredentialRepository": {
        "get_ingest_credential": (
            [("ingest_credential_id", P, str)],
            IngestCredential | None,
            True,
        ),
        "list_ingest_credentials": ([T, IA], list[IngestCredential], True),
        "create_ingest_credential": (
            [("credential", P, IngestCredential), ("audit", P, AuditContext)],
            IngestCredential,
            True,
        ),
        "revoke_ingest_credential": (
            [
                T,
                IA,
                ("ingest_credential_id", P, str),
                ("expected_version", K, int),
                ("revoked_at", K, datetime),
                ("audit", K, AuditContext),
            ],
            IngestCredential,
            True,
        ),
    },
    "ConnectorAuthenticator": {
        "authenticate": ([("token", P, str)], AuthenticatedSource | None, True),
    },
    "ExternalIdentityResolver": {
        "resolve_external_identity": (
            [("provider_key", P, str), ("external_identity_hash", P, str)],
            DeviceIntegration | None,
            True,
        ),
    },
    "IntegrationLifecycleRepository": {
        "transition_integration_account": (
            [
                T,
                IA,
                ("target", P, IntegrationAccountStatus),
                ("expected_version", K, int),
                ("updated_at", K, datetime),
                ("audit", K, AuditContext),
            ],
            IntegrationAccount,
            True,
        ),
        "transition_device_integration": (
            [
                T,
                ("device_integration_id", P, str),
                ("target", P, DeviceIntegrationStatus),
                ("expected_version", K, int),
                ("updated_at", K, datetime),
                ("audit", K, AuditContext),
            ],
            DeviceIntegration,
            True,
        ),
    },
    "ConsistentDeviceReader": {
        "get_device": (
            [T, ("device_id", P, str), ("consistent", K, bool)],
            DeviceV2 | None,
            True,
        ),
    },
    "TrustedMapping": {
        "admit": (
            [("source", P, AuthenticatedSource), ("batch", P, IngestObservationBatch)],
            IngressAdmission | IngressDenial,
            True,
        ),
        "fence": (
            [("envelope", P, IngressEnvelope)],
            TrustedIngress | IngressFence | IngressDefer,
            True,
        ),
    },
    "IngressReceiptRepository": {
        "get_ingress_receipt": ([IA, ("key_sha256", P, str)], IngressReceipt | None, True),
        "claim_ingress_receipt": (
            [("receipt", P, IngressReceipt), ("now", K, datetime)],
            IngressReceipt,
            True,
        ),
        "mark_ingress_receipt_enqueued": (
            [IA, ("key_sha256", P, str), ("ingress_id", P, str), ("at", K, datetime)],
            None,
            True,
        ),
    },
    "IngressQueueProducer": {
        "send": ([("envelope", P, IngressEnvelope)], str, True),
        "send_raw": ([("body", P, str)], str, True),
    },
    "IngressQueueConsumer": {
        "receive": (
            [("max_messages", K, int), ("wait_seconds", K, int), ("visibility_seconds", K, int)],
            list[IngressQueueMessage],
            True,
        ),
        "delete": ([("message", P, IngressQueueMessage)], None, True),
        "change_visibility": (
            [("message", P, IngressQueueMessage), ("seconds", P, int)],
            None,
            True,
        ),
    },
    "QuarantineRepository": {
        "put_quarantine_entries": ([("entries", P, Sequence[QuarantineEntry])], None, True),
        "list_quarantine_entries": (
            [
                T,
                ("status", K, QuarantineEntryStatus | None),
                ("reason", K, str | None),
                ("after_sk", K, str | None),
                ("limit", K, int),
            ],
            QuarantinePage,
            True,
        ),
        "get_quarantine_entry": ([T, ("entry_key", P, str)], QuarantineEntry | None, True),
        "transition_quarantine_entry": (
            [
                T,
                ("entry_key", P, str),
                ("target", P, QuarantineEntryStatus),
                ("expected_version", K, int),
                ("at", K, datetime),
                ("replayed_ingress_id", K, str | None),
            ],
            QuarantineEntry,
            True,
        ),
    },
    "IngestStateRepository": {
        "get_component_states": (
            [T, ("component_ids", P, Sequence[str])],
            dict[str, ComponentIngestState],
            True,
        ),
        "apply_ingest_marks": ([("marks", P, IngestMarks)], None, True),
        "get_connector_health": ([T, IA], ConnectorHealth | None, True),
    },
    "IngressRateLimiter": {
        "try_acquire": ([("key", P, str), ("now", K, float)], float | None, False),
    },
    "IngressEntitlementCheck": {
        "allows": (
            [T, IA, ("observation_count", P, int)],
            bool,
            True,
        ),
    },
    "TelemetryMetrics": {
        "increment": (
            [("name", P, str), ("value", P, int), ("labels", Parameter.VAR_KEYWORD, str)],
            None,
            False,
        ),
        "observe": (
            [("name", P, str), ("value", P, float), ("labels", Parameter.VAR_KEYWORD, str)],
            None,
            False,
        ),
    },
}


def _members(protocol: type) -> set[str]:
    return {
        name
        for name, value in vars(protocol).items()
        if not name.startswith("_") and callable(value)
    }


@pytest.mark.parametrize("name", list(PROTOCOLS))
def test_protocol_member_sets_and_signatures_are_frozen(name: str) -> None:
    protocol = getattr(ports, name)
    expected = PROTOCOLS[name]

    assert getattr(protocol, "_is_protocol", False) is True
    assert getattr(protocol, "_is_runtime_protocol", False) is True
    assert _members(protocol) == set(expected)
    for method_name, (parameters, returns, is_async) in expected.items():
        method = getattr(protocol, method_name)
        signature = inspect.signature(method)
        hints = typing.get_type_hints(method)
        actual = [
            (parameter.name, parameter.kind, hints.get(parameter.name))
            for parameter in list(signature.parameters.values())[1:]
        ]
        assert actual == parameters, method_name
        assert hints.get("return", type(None)) == (type(None) if returns is None else returns)
        assert inspect.iscoroutinefunction(method) is is_async, method_name


def test_protocol_defaults_are_frozen() -> None:
    increment = inspect.signature(ports.TelemetryMetrics.increment).parameters
    listing = inspect.signature(ports.QuarantineRepository.list_quarantine_entries).parameters
    transition = inspect.signature(
        ports.QuarantineRepository.transition_quarantine_entry
    ).parameters

    assert increment["value"].default == 1
    assert listing["status"].default is None and listing["reason"].default is None
    assert listing["after_sk"].default is Parameter.empty
    assert listing["limit"].default is Parameter.empty
    assert transition["replayed_ingress_id"].default is None


def test_ports_module_defines_exactly_the_p3_protocols() -> None:
    defined = {
        name
        for name, value in vars(ports).items()
        if isinstance(value, type)
        and getattr(value, "_is_protocol", False)
        and value.__module__ == ports.__name__
    }

    assert defined == set(PROTOCOLS)


@pytest.mark.parametrize(
    "name",
    [
        "IngressAdmission",
        "IngressDenial",
        "TrustedIngress",
        "IngressFence",
        "IngressDefer",
        "IngressQueueMessage",
        "IngestMarks",
        "QuarantinePage",
    ],
)
def test_port_value_types_are_the_domain_types(name: str) -> None:
    assert getattr(ports, name) is getattr(domain, name)


def test_m1_protocols_stay_untouched() -> None:
    from limnopulse_api.repositories import v2_contracts

    assert "get_device" in vars(v2_contracts.DeviceReader)
    assert list(inspect.signature(v2_contracts.DeviceReader.get_device).parameters) == [
        "self",
        "tenant_id",
        "device_id",
    ]


# FastAPI dependencies --------------------------------------------------------------------------

STATE_NAMES = (
    "ingest_credential_repository",
    "connector_authenticator",
    "integration_lifecycle_repository",
    "trusted_mapping",
    "ingress_receipt_repository",
    "ingress_queue_producer",
    "ingress_preauth_global_rate_limiter",
    "ingress_preauth_rate_limiter",
    "ingress_rate_limiter",
    "ingress_entitlement_check",
    "telemetry_metrics",
)


def test_app_state_names_are_frozen() -> None:
    assert ingestion_dependencies.INGESTION_STATE_NAMES == STATE_NAMES


def _client(name: str, *, present: bool) -> TestClient:
    getter = getattr(ingestion_dependencies, f"get_{name}")
    app = FastAPI()

    @app.get("/probe")
    def probe(value: Annotated[Any, Depends(getter)]) -> dict[str, str]:
        return {"value": str(value)}

    if present:
        setattr(app.state, name, f"stub-{name}")
    return TestClient(app)


@pytest.mark.parametrize("name", STATE_NAMES)
def test_getters_return_503_when_state_is_missing(name: str) -> None:
    response = _client(name, present=False).get("/probe")

    assert response.status_code == 503
    assert response.json() == {"detail": "service unavailable"}


@pytest.mark.parametrize("name", STATE_NAMES)
def test_getters_return_the_state_attribute(name: str) -> None:
    response = _client(name, present=True).get("/probe")

    assert response.status_code == 200
    assert response.json() == {"value": f"stub-{name}"}


# Moved route class ---------------------------------------------------------------------------


def test_redacted_validation_route_lives_in_its_own_module() -> None:
    assert _RedactedValidationRoute.__module__ == "limnopulse_api.api.redacted_validation"
    assert integrations_router.router.route_class is _RedactedValidationRoute
    assert integrations_router._RedactedValidationRoute is _RedactedValidationRoute


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: int


def test_redacted_validation_route_drops_input_and_context() -> None:
    router = APIRouter(route_class=_RedactedValidationRoute)

    @router.post("/echo")
    def echo(body: _Body) -> dict[str, int]:
        return {"token": body.token}

    app = FastAPI()
    app.include_router(router)

    response = TestClient(app).post("/echo", json={"token": "zq9x-secret", "extra": "zq9x"})

    assert response.status_code == 422
    assert "zq9x" not in response.text
    detail = response.json()["detail"]
    assert {item["type"] for item in detail} == {"int_parsing", "extra_forbidden"}
    assert all("input" not in item and "ctx" not in item for item in detail)
