"""FastAPI getters for the frozen P3 `app.state` names; each answers 503 when unset."""

from typing import Any

from fastapi import Request

from limnopulse_api.api.dependencies import _get_state_dependency

INGESTION_STATE_NAMES = (
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


def get_ingest_credential_repository(request: Request) -> Any:
    return _get_state_dependency(request, "ingest_credential_repository")


def get_connector_authenticator(request: Request) -> Any:
    return _get_state_dependency(request, "connector_authenticator")


def get_integration_lifecycle_repository(request: Request) -> Any:
    return _get_state_dependency(request, "integration_lifecycle_repository")


def get_trusted_mapping(request: Request) -> Any:
    return _get_state_dependency(request, "trusted_mapping")


def get_ingress_receipt_repository(request: Request) -> Any:
    return _get_state_dependency(request, "ingress_receipt_repository")


def get_ingress_queue_producer(request: Request) -> Any:
    return _get_state_dependency(request, "ingress_queue_producer")


def get_ingress_preauth_global_rate_limiter(request: Request) -> Any:
    return _get_state_dependency(request, "ingress_preauth_global_rate_limiter")


def get_ingress_preauth_rate_limiter(request: Request) -> Any:
    return _get_state_dependency(request, "ingress_preauth_rate_limiter")


def get_ingress_rate_limiter(request: Request) -> Any:
    return _get_state_dependency(request, "ingress_rate_limiter")


def get_ingress_entitlement_check(request: Request) -> Any:
    return _get_state_dependency(request, "ingress_entitlement_check")


def get_telemetry_metrics(request: Request) -> Any:
    return _get_state_dependency(request, "telemetry_metrics")
