"""`POST /ingest/v2/observations` (P3 D3): the generic HTTPS ingress route. Unmounted here;
P3-10 mounts it only when `INGEST_V2_ENABLED`.

The route declares no FastAPI body or header parameter, because the frozen route order puts
authentication and the three rate limits before any body read. It reads the headers itself,
streams the body under `MAX_INGEST_BODY_BYTES` (the `api/telegram_webhook.py` pattern), parses
with `IngestObservationBatch.model_validate_json` and builds every 422 from the redacted
validation detail, so it never raises `RequestValidationError` and never echoes input. Steps
1-4 and 8-16 run in `IngressService`; every rejection is a fixed body (`IngestRejected`).
"""

import re
import time
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.datastructures import Headers

from limnopulse_api.api.ingestion_dependencies import (
    get_connector_authenticator,
    get_ingress_entitlement_check,
    get_ingress_preauth_global_rate_limiter,
    get_ingress_preauth_rate_limiter,
    get_ingress_queue_producer,
    get_ingress_rate_limiter,
    get_ingress_receipt_repository,
    get_telemetry_metrics,
    get_trusted_mapping,
)
from limnopulse_api.api.redacted_validation import redacted_validation_detail
from limnopulse_api.domain.ingestion import (
    CONNECTOR_VERSION_PATTERN,
    MAX_INGEST_BODY_BYTES,
    MAX_VALIDATION_ERRORS,
    IngestErrorDetail,
    IngestObservationBatch,
)
from limnopulse_api.domain.v2_common import IDEMPOTENCY_KEY_MAX_LENGTH, IDEMPOTENCY_KEY_MIN_LENGTH
from limnopulse_api.services.ingestion import IngestRejected, IngressService

INGEST_OBSERVATIONS_PATH = "/ingest/v2/observations"
_CONTENT_LENGTH = re.compile(r"[0-9]+")
_CONNECTOR_VERSION = re.compile(CONNECTOR_VERSION_PATTERN)
_IDEMPOTENCY_LOC = ["header", "idempotency-key"]
_CONNECTOR_LOC = ["header", "x-connector-version"]

router = APIRouter(tags=["ingest-v2"])


def _utc_now() -> datetime:
    return datetime.now(UTC)


def get_ingress_service(request: Request) -> IngressService:
    """Composes the service from the frozen `app.state` ports; a missing one is a 503."""
    return IngressService(
        authenticator=get_connector_authenticator(request),
        preauth_global_limiter=get_ingress_preauth_global_rate_limiter(request),
        preauth_limiter=get_ingress_preauth_rate_limiter(request),
        account_limiter=get_ingress_rate_limiter(request),
        receipts=get_ingress_receipt_repository(request),
        trusted_mapping=get_trusted_mapping(request),
        entitlements=get_ingress_entitlement_check(request),
        producer=get_ingress_queue_producer(request),
        metrics=get_telemetry_metrics(request),
        clock=_utc_now,
        monotonic=time.monotonic,
    )


def _check_media_type(service: IngressService, headers: Headers) -> None:
    """Step 5: `application/json` (optionally `charset=utf-8`), no content coding."""
    content_type = headers.get("content-type")
    supported = content_type is not None
    if supported:
        media_type, *parameters = (part.strip() for part in content_type.split(";"))
        supported = media_type.lower() == "application/json" and all(
            parameter.lower().replace('"', "") == "charset=utf-8" for parameter in parameters
        )
    encodings = headers.getlist("content-encoding")
    if not supported or any(value.strip().lower() != "identity" for value in encodings):
        raise service.reject(
            415, IngestErrorDetail.UNSUPPORTED_MEDIA_TYPE.value, outcome="unsupported_media"
        )


def _too_large(service: IngressService) -> IngestRejected:
    return service.reject(413, IngestErrorDetail.TOO_LARGE.value, outcome="too_large")


async def _read_body(service: IngressService, request: Request) -> bytes:
    """Step 6: a declared length is checked before reading; the stream stops at the limit."""
    declared = request.headers.get("content-length")
    if declared is not None and (
        _CONTENT_LENGTH.fullmatch(declared) is None or int(declared) > MAX_INGEST_BODY_BYTES
    ):
        raise _too_large(service)
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_INGEST_BODY_BYTES:
            raise _too_large(service)
        body.extend(chunk)
    return bytes(body)


def _header_errors(headers: Headers) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    key = headers.get("idempotency-key")
    if key is None:
        errors.append({"loc": _IDEMPOTENCY_LOC, "msg": "field required", "type": "missing"})
    elif len(key) < IDEMPOTENCY_KEY_MIN_LENGTH:
        errors.append({"loc": _IDEMPOTENCY_LOC, "msg": "invalid value", "type": "string_too_short"})
    elif len(key) > IDEMPOTENCY_KEY_MAX_LENGTH:
        errors.append({"loc": _IDEMPOTENCY_LOC, "msg": "invalid value", "type": "string_too_long"})
    version = headers.get("x-connector-version")
    if version is not None and _CONNECTOR_VERSION.fullmatch(version) is None:
        errors.append(
            {"loc": _CONNECTOR_LOC, "msg": "invalid value", "type": "string_pattern_mismatch"}
        )
    return errors


def _parse(
    service: IngressService, headers: Headers, body: bytes
) -> tuple[IngestObservationBatch, str, str | None]:
    """Step 7: headers and body together; at most 20 redacted entries, never the input."""
    errors = _header_errors(headers)
    batch = None
    try:
        batch = IngestObservationBatch.model_validate_json(body)
    except ValidationError as error:
        errors.extend(redacted_validation_detail(error))
    if errors or batch is None:
        raise service.reject(422, errors[:MAX_VALIDATION_ERRORS], outcome="invalid")
    return batch, headers["idempotency-key"], headers.get("x-connector-version")


@router.post(INGEST_OBSERVATIONS_PATH, status_code=202, response_model=None)
async def ingest_observations(
    request: Request, service: Annotated[IngressService, Depends(get_ingress_service)]
) -> JSONResponse:
    headers = request.headers
    try:
        source = await service.authenticate(headers.get("authorization"))
        _check_media_type(service, headers)
        body = await _read_body(service, request)
        batch, idempotency_key, connector_version = _parse(service, headers, body)
        accepted = await service.admit(
            source,
            batch,
            idempotency_key=idempotency_key,
            connector_version=connector_version,
        )
    except IngestRejected as rejection:
        return JSONResponse(
            {"detail": rejection.detail},
            status_code=rejection.status_code,
            headers=rejection.headers,
        )
    return JSONResponse(
        {
            "ingress_id": accepted.ingress_id,
            "accepted_observations": accepted.accepted_observations,
            "replayed": accepted.replayed,
        },
        status_code=202,
    )
