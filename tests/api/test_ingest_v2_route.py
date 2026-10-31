"""P3-07: `POST /ingest/v2/observations` (unmounted router) follows the frozen route order table:
exact statuses, bodies and headers, and every step short-circuits.

The route is driven with raw ASGI calls whose `receive` counts the body bytes handed to the app,
so "no body read on 401/429/415" and "nothing read past the limit on 413" are measured, not
assumed. The ports are the P3-00 fakes, installed under the frozen `app.state` names.
"""

import asyncio
import json
import logging
from typing import Any

import pytest
from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.routing import APIRoute

from limnopulse_api.adapters.dynamodb_v2 import idempotency_key_hash
from limnopulse_api.api import ingest_v2
from limnopulse_api.core.errors import IngressQueueError
from limnopulse_api.domain.ingestion import (
    MAX_INGEST_BODY_BYTES,
    IngressReceiptState,
    ingest_token,
)
from limnopulse_api.domain.integrations import DeviceIntegrationStatus
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    CREDENTIAL,
    DEFAULT_COMPONENT,
    EXTERNAL_DEVICE,
    OBSERVED,
    OTHER_COMPONENT,
    SECRET,
    component,
    credential,
    device,
    device_integration,
    integration_account,
)
from tests.support.ingestion_fakes import (
    FakeConnectorAuthenticator,
    FakeEntitlementCheck,
    FakeIngressQueue,
    FakeIngressReceiptRepository,
    FakeTrustedMapping,
    InMemoryRateLimiter,
    KitSubject,
    RecordingTelemetryMetrics,
)

PATH = "/ingest/v2/observations"
TOKEN = ingest_token(CREDENTIAL, SECRET)
KEY = "route-key-0001-SENTINEL"
SENTINEL = "SENTINELp307"
STATE_NAMES = {
    "connector_authenticator": "authenticator",
    "trusted_mapping": "mapping",
    "ingress_receipt_repository": "receipts",
    "ingress_queue_producer": "queue",
    "ingress_entitlement_check": "entitlements",
}


class FixedLimiter:
    def __init__(self, retry_after: float | None) -> None:
        self.retry_after = retry_after
        self.calls = 0

    def try_acquire(self, key: str, *, now: float) -> float | None:
        self.calls += 1
        return self.retry_after


def generous_limiter() -> InMemoryRateLimiter:
    return InMemoryRateLimiter(capacity=1_000_000, refill_per_second=1_000_000.0, max_keys=1_000)


class Route:
    def __init__(self) -> None:
        self.subject = KitSubject(
            authenticator=lambda s: FakeConnectorAuthenticator(s.store),
            mapping=lambda s: FakeTrustedMapping(s.store),
            receipts=lambda s: FakeIngressReceiptRepository(s.store),
            queue=lambda s: FakeIngressQueue(s.faults),
            entitlements=lambda s: FakeEntitlementCheck(s.faults),
        )
        self.metrics = RecordingTelemetryMetrics()
        self.app = FastAPI()
        self.app.include_router(ingest_v2.router)
        for state_name, port in STATE_NAMES.items():
            setattr(self.app.state, state_name, getattr(self.subject, port))
        self.app.state.ingress_preauth_global_rate_limiter = generous_limiter()
        self.app.state.ingress_preauth_rate_limiter = generous_limiter()
        self.app.state.ingress_rate_limiter = generous_limiter()
        self.app.state.telemetry_metrics = self.metrics

    async def seed(self, *components: Any) -> None:
        await self.subject.seed_account(integration_account())
        await self.subject.seed_credential(credential())
        await self.subject.seed_device_integration(device_integration())
        await self.subject.seed_device(device())
        for item in components or (component(), component(DEFAULT_COMPONENT)):
            await self.subject.seed_component(item)

    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(item["body"]) for item in self.subject.queue.messages.values()]

    def outcomes(self) -> list[str]:
        return [
            record["labels"]["outcome"]
            for record in self.metrics.records
            if record["metric"] == "ingest_requests_total"
        ]


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes, bytes_read: int):
        self.status = status
        self.headers = headers
        self.body = body
        self.bytes_read = bytes_read

    def json(self) -> Any:
        return json.loads(self.body)


def reading(index: int = 0, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "component_id": COMPONENT,
        "metric_key": "temperature",
        "value": 27.5 + index,
        "source_unit": "C",
        "observed_at": OBSERVED.isoformat(),
    }
    payload.update(overrides)
    return {key: value for key, value in payload.items() if value is not None}


def payload(*observations: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "external_device_id": EXTERNAL_DEVICE,
        "observations": list(observations) or [reading(0), reading(1)],
        **extra,
    }


def encode(document: Any) -> bytes:
    return json.dumps(document).encode()


def headers(**overrides: str | None) -> dict[str, str]:
    base: dict[str, str | None] = {
        "authorization": f"Bearer {TOKEN}",
        "content-type": "application/json",
        "idempotency-key": KEY,
        "x-connector-version": "fw-1.0",
    }
    base.update({name.replace("_", "-"): value for name, value in overrides.items()})
    return {name: value for name, value in base.items() if value is not None}


async def post(
    app: FastAPI,
    body: bytes = b"",
    *,
    header_map: dict[str, str] | None = None,
    chunks: list[bytes] | None = None,
    content_length: str | None = "auto",
) -> Response:
    """One raw ASGI request. `chunks` streams the body (no Content-Length unless given)."""
    parts = list(chunks) if chunks is not None else [body]
    request_headers = dict(header_map if header_map is not None else headers())
    if content_length == "auto" and chunks is None:
        request_headers["content-length"] = str(len(body))
    elif content_length not in (None, "auto"):
        request_headers["content-length"] = content_length
    bytes_read = 0
    finished = False

    async def receive() -> dict[str, Any]:
        nonlocal bytes_read, finished
        if not finished:
            chunk = parts.pop(0) if parts else b""
            bytes_read += len(chunk)
            finished = not parts
            return {"type": "http.request", "body": chunk, "more_body": not finished}
        await asyncio.sleep(0)
        return {"type": "http.disconnect"}

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": PATH,
        "raw_path": PATH.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [
            (name.lower().encode(), value.encode()) for name, value in request_headers.items()
        ],
        "client": ("203.0.113.9", 50000),
        "server": ("testserver", 443),
    }
    await asyncio.wait_for(app(scope, receive, send), timeout=10)
    start = next(message for message in sent if message["type"] == "http.response.start")
    response_headers = {
        name.decode().lower(): value.decode() for name, value in start.get("headers", [])
    }
    body_bytes = b"".join(
        message.get("body", b"") for message in sent if message["type"] == "http.response.body"
    )
    return Response(start["status"], response_headers, body_bytes, bytes_read)


@pytest.fixture
def route() -> Route:
    return Route()


# The router itself --------------------------------------------------------------------------


def test_router_declares_no_body_or_header_parameters() -> None:
    (endpoint,) = [
        item for item in ingest_v2.router.routes if isinstance(item, APIRoute) and item.path == PATH
    ]

    assert endpoint.methods == {"POST"}
    assert endpoint.body_field is None
    pending, seen = [endpoint.dependant], 0
    while pending:  # the endpoint and every sub-dependency
        dependant = pending.pop()
        seen += 1
        assert dependant.body_params == []
        assert dependant.header_params == []
        assert dependant.query_params == []
        pending.extend(dependant.dependencies)
    assert seen >= 1


def test_router_stays_unmounted_while_ingest_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """P3-10 mounts the router only behind `INGEST_V2_ENABLED` (D15); off, it is absent."""
    from limnopulse_api.core.config import Settings
    from limnopulse_api.main import create_app

    monkeypatch.delenv("INGEST_V2_ENABLED", raising=False)
    for overrides in ({}, {"api_v2_enabled": True}):
        settings = Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)
        paths = create_app(settings).openapi()["paths"]
        assert ingest_v2.INGEST_OBSERVATIONS_PATH not in paths, overrides


# Step 16 and the receipt replay table ------------------------------------------------------


@pytest.mark.asyncio
async def test_accepted_request_is_202_after_the_queue_write(route: Route) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()))

    assert response.status == 202
    body = response.json()
    assert set(body) == {"ingress_id", "accepted_observations", "replayed"}
    assert body["accepted_observations"] == 2 and body["replayed"] is False
    assert body["ingress_id"].startswith("ing_") and len(body["ingress_id"]) == 36
    assert response.headers["content-type"].startswith("application/json")
    assert [item["ingress_id"] for item in route.bodies()] == [body["ingress_id"]]
    assert route.bodies()[0]["connector_version"] == "fw-1.0"
    assert route.outcomes() == ["accepted"]


@pytest.mark.asyncio
async def test_same_request_replays_with_the_same_ingress_id(route: Route) -> None:
    await route.seed()
    first = await post(route.app, encode(payload()))

    again = await post(route.app, encode(payload()))

    assert again.status == 202
    assert again.json() == {**first.json(), "replayed": True}
    assert len(route.bodies()) == 1
    assert route.outcomes() == ["accepted", "replayed"]


@pytest.mark.asyncio
async def test_same_key_different_request_is_409(route: Route) -> None:
    await route.seed()
    await post(route.app, encode(payload()))

    response = await post(route.app, encode(payload(reading(5))))

    assert (response.status, response.json()) == (
        409,
        {"detail": "idempotency key reused with a different request"},
    )
    assert len(route.bodies()) == 1


@pytest.mark.asyncio
async def test_failed_send_is_503_and_the_retry_is_accepted(route: Route) -> None:
    await route.seed()
    route.subject.fail_next("send", IngressQueueError("down", retryable=True))

    failed = await post(route.app, encode(payload()))

    assert (failed.status, failed.json()) == (503, {"detail": "service unavailable"})
    stored = route.subject.store.receipts[(ACCOUNT, idempotency_key_hash(KEY))]
    assert stored.state == IngressReceiptState.PENDING

    retried = await post(route.app, encode(payload()))

    assert retried.status == 202
    assert retried.json() == {
        "ingress_id": stored.ingress_id,
        "accepted_observations": 2,
        "replayed": True,
    }


# Steps 1-4 ---------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization", [None, "", "Basic abc", f"Bearer {TOKEN}!", f"Bearer {TOKEN[:-1]}"]
)
async def test_step1_is_401_with_www_authenticate_and_no_body_read(
    route: Route, authorization: str | None
) -> None:
    await route.seed()

    response = await post(
        route.app, encode(payload()), header_map=headers(authorization=authorization)
    )

    assert (response.status, response.json()) == (401, {"detail": "unauthorized"})
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.bytes_read == 0
    assert route.bodies() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state_name",
    [
        "ingress_preauth_global_rate_limiter",
        "ingress_preauth_rate_limiter",
        "ingress_rate_limiter",
    ],
)
async def test_rate_limits_are_429_with_retry_after_and_no_body_read(
    route: Route, state_name: str
) -> None:
    await route.seed()
    setattr(route.app.state, state_name, FixedLimiter(3.2))

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (429, {"detail": "rate limited"})
    assert response.headers["retry-after"] == "4"
    assert response.bytes_read == 0
    assert route.outcomes() == ["rate_limited"]


@pytest.mark.asyncio
async def test_preauth_limits_run_before_any_store_read(route: Route) -> None:
    await route.seed()
    route.app.state.ingress_preauth_rate_limiter = FixedLimiter(1.0)
    route.subject.fail_next("get_ingest_credential", ClientError({"Error": {}}, "GetItem"))

    response = await post(route.app, encode(payload()))

    assert response.status == 429
    # The armed store fault is still pending: the credential row was never read.
    with pytest.raises(ClientError):
        route.subject.store.get_ingest_credential(CREDENTIAL)


@pytest.mark.asyncio
async def test_step3_unknown_credential_is_401_and_reads_no_body(route: Route) -> None:
    await route.seed()
    token = ingest_token("ic_" + "f" * 32, SECRET)

    response = await post(
        route.app, encode(payload()), header_map=headers(authorization=f"Bearer {token}")
    )

    assert (response.status, response.json()) == (401, {"detail": "unauthorized"})
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_step3_store_failure_is_503(route: Route) -> None:
    await route.seed()
    route.subject.fail_next(
        "get_ingest_credential",
        ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, "GetItem"),
    )

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (503, {"detail": "service unavailable"})
    assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_missing_state_is_503(route: Route) -> None:
    await route.seed()
    del route.app.state.ingress_queue_producer

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (503, {"detail": "service unavailable"})


# Step 5: media type --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"content_type": None},
        {"content_type": "text/plain"},
        {"content_type": "application/jsonx"},
        {"content_type": "application/json; charset=latin-1"},
        {"content_type": "application/x-www-form-urlencoded"},
        {"content_encoding": "gzip"},
        {"content_encoding": "br"},
        {"content_encoding": ""},
    ],
)
async def test_step5_unsupported_media_is_415_without_reading_the_body(
    route: Route, overrides: dict[str, str | None]
) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()), header_map=headers(**overrides))

    assert (response.status, response.json()) == (415, {"detail": "unsupported media type"})
    assert response.bytes_read == 0
    assert route.bodies() == []
    assert route.outcomes() == ["unsupported_media"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"content_type": "application/json; charset=utf-8"},
        {"content_type": "application/json; charset=UTF-8"},
        {"content_type": "Application/JSON"},
        {"content_encoding": "identity"},
    ],
)
async def test_step5_accepted_media_types(route: Route, overrides: dict[str, str]) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()), header_map=headers(**overrides))

    assert response.status == 202


@pytest.mark.asyncio
async def test_step5_runs_after_authentication(route: Route) -> None:
    await route.seed()
    unknown = ingest_token("ic_" + "f" * 32, SECRET)  # passes the grammar, fails step 3

    response = await post(
        route.app,
        encode(payload()),
        header_map=headers(authorization=f"Bearer {unknown}", content_type="text/plain"),
    )

    assert response.status == 401


@pytest.mark.asyncio
async def test_step5_runs_after_the_account_bucket(route: Route) -> None:
    await route.seed()
    route.app.state.ingress_rate_limiter = FixedLimiter(1.0)

    response = await post(
        route.app, encode(payload()), header_map=headers(content_type="text/plain")
    )

    assert response.status == 429


@pytest.mark.asyncio
async def test_step5_runs_before_the_size_check(route: Route) -> None:
    await route.seed()

    response = await post(
        route.app,
        encode(payload()),
        header_map=headers(content_type="text/plain"),
        content_length=str(MAX_INGEST_BODY_BYTES + 1),
    )

    assert response.status == 415
    assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_step6_runs_before_header_validation(route: Route) -> None:
    await route.seed()

    response = await post(
        route.app,
        encode(payload()),
        header_map=headers(idempotency_key=None),
        content_length=str(MAX_INGEST_BODY_BYTES + 1),
    )

    assert response.status == 413


# Step 6: bounded body ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_step6_declared_length_over_the_limit_is_413_without_reading(route: Route) -> None:
    await route.seed()

    response = await post(
        route.app,
        encode(payload()),
        content_length=str(MAX_INGEST_BODY_BYTES + 1),
    )

    assert (response.status, response.json()) == (413, {"detail": "request body too large"})
    assert response.bytes_read == 0
    assert route.outcomes() == ["too_large"]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["abc", "-1", "1.5", "12abc", "", "0x10", "+10", " 10"])
async def test_step6_malformed_content_length_is_413(route: Route, value: str) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()), content_length=value)

    assert (response.status, response.json()) == (413, {"detail": "request body too large"})
    assert response.bytes_read == 0


@pytest.mark.asyncio
async def test_step6_streamed_body_stops_reading_at_the_limit(route: Route) -> None:
    await route.seed()
    chunk = b" " * 16_384
    chunks = [chunk] * 20  # 320 KiB without a Content-Length

    response = await post(route.app, chunks=chunks, content_length=None)

    assert (response.status, response.json()) == (413, {"detail": "request body too large"})
    assert MAX_INGEST_BODY_BYTES < response.bytes_read <= MAX_INGEST_BODY_BYTES + len(chunk)
    assert route.subject.store.receipts == {}


@pytest.mark.asyncio
async def test_step6_understated_content_length_is_still_bounded(route: Route) -> None:
    await route.seed()
    chunk = b" " * 16_384

    response = await post(route.app, chunks=[chunk] * 20, content_length="10")

    assert response.status == 413
    assert response.bytes_read <= MAX_INGEST_BODY_BYTES + len(chunk)


@pytest.mark.asyncio
async def test_step6_a_body_of_exactly_the_limit_is_read(route: Route) -> None:
    await route.seed()
    document = encode(payload())
    exact = document + b" " * (MAX_INGEST_BODY_BYTES - len(document))

    accepted = await post(route.app, exact)
    over = await post(route.app, exact + b" ", header_map=headers(idempotency_key="route-key-0002"))

    assert accepted.status == 202
    assert over.status == 413


# Step 7: redacted validation --------------------------------------------------------------


def _assert_redacted_422(response: Response) -> list[dict[str, Any]]:
    assert response.status == 422
    detail = response.json()["detail"]
    assert isinstance(detail, list) and 1 <= len(detail) <= 20
    for item in detail:
        assert set(item) == {"loc", "msg", "type"}
        assert item["msg"] in {"invalid value", "field required", "extra fields not permitted"}
    return detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b"{not json",
        b"",
        b"[]",
        encode({"observations": []}),
        encode(payload(tenant_id="tnt_x")),
        encode(payload(site_id="s")),
        encode({"external_device_id": EXTERNAL_DEVICE, "observations": [reading(0, value=None)]}),
        encode({"external_device_id": " padded", "observations": [reading(0)]}),
        b'{"external_device_id":"sensor-01","observations":[{"component_id":"'
        + COMPONENT.encode()
        + b'","metric_key":"temperature","value":NaN,"source_unit":"C","observed_at":"'
        + OBSERVED.isoformat().encode()
        + b'"}]}',
        b'{"external_device_id":"sensor-01","observations":[{"component_id":"'
        + COMPONENT.encode()
        + b'","metric_key":"temperature","value":Infinity,"source_unit":"C","observed_at":"'
        + OBSERVED.isoformat().encode()
        + b'"}]}',
    ],
    ids=[
        "invalid_json",
        "empty",
        "array",
        "no_observations",
        "tenant_authority",
        "site_authority",
        "missing_value",
        "bad_external_id",
        "nan",
        "infinity",
    ],
)
async def test_step7_invalid_bodies_are_a_redacted_422_never_500(route: Route, body: bytes) -> None:
    await route.seed()

    response = await post(route.app, body)

    _assert_redacted_422(response)
    assert route.subject.store.receipts == {}
    assert route.bodies() == []
    assert route.outcomes() == ["invalid"]


@pytest.mark.asyncio
async def test_step7_never_echoes_input(route: Route) -> None:
    await route.seed()
    body = payload(
        reading(0, source_metadata={f"{SENTINEL}_key": "v"}),
        reading(1, component_id=f"cmp_{SENTINEL}"),
        reading(2, source_unit=SENTINEL + "u" * 20),
        reading(3, metric_key=SENTINEL),
    )
    body[SENTINEL] = SENTINEL

    response = await post(
        route.app,
        encode(body),
        header_map=headers(
            idempotency_key=f"x{SENTINEL}" * 20, x_connector_version=f"{SENTINEL} !"
        ),
    )

    _assert_redacted_422(response)
    assert SENTINEL not in response.body.decode()


@pytest.mark.asyncio
async def test_step7_caps_the_detail_at_20_entries(route: Route) -> None:
    await route.seed()
    body = payload(*[reading(index, value="bad", source_unit="") for index in range(50)])

    response = await post(route.app, encode(body))

    detail = _assert_redacted_422(response)
    assert len(detail) == 20


@pytest.mark.asyncio
async def test_step7_caps_body_and_header_errors_together_at_20(route: Route) -> None:
    await route.seed()
    body = payload(*[reading(index, value="bad", source_unit="") for index in range(50)])

    response = await post(
        route.app,
        encode(body),
        header_map=headers(idempotency_key="short", x_connector_version="has space"),
    )

    detail = _assert_redacted_422(response)
    assert len(detail) == 20


@pytest.mark.asyncio
async def test_step7_more_than_32_distinct_components_is_the_model_422(route: Route) -> None:
    await route.seed()
    body = payload(*[reading(index, component_id=f"cmp_{index:032x}") for index in range(33)])

    response = await post(route.app, encode(body))

    assert response.status == 422
    assert response.json() == {
        "detail": [{"loc": ["body"], "msg": "invalid value", "type": "value_error"}]
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "loc"),
    [
        ({"idempotency_key": None}, ["header", "idempotency-key"]),
        ({"idempotency_key": "k" * 7}, ["header", "idempotency-key"]),
        ({"idempotency_key": "k" * 129}, ["header", "idempotency-key"]),
        ({"x_connector_version": "has space"}, ["header", "x-connector-version"]),
        ({"x_connector_version": "v" * 65}, ["header", "x-connector-version"]),
        ({"x_connector_version": ""}, ["header", "x-connector-version"]),
    ],
)
async def test_step7_header_rules_are_a_422_with_header_locations(
    route: Route, overrides: dict[str, str | None], loc: list[str]
) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()), header_map=headers(**overrides))

    detail = _assert_redacted_422(response)
    assert [item["loc"] for item in detail] == [loc]
    assert route.subject.store.receipts == {}


@pytest.mark.asyncio
async def test_step7_missing_idempotency_key_is_field_required(route: Route) -> None:
    await route.seed()

    response = await post(route.app, encode(payload()), header_map=headers(idempotency_key=None))

    assert response.json() == {
        "detail": [
            {"loc": ["header", "idempotency-key"], "msg": "field required", "type": "missing"}
        ]
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("key", "version"), [("k" * 8, None), ("k" * 128, "v" * 64)])
async def test_step7_header_bounds_are_inclusive(
    route: Route, key: str, version: str | None
) -> None:
    await route.seed()

    response = await post(
        route.app,
        encode(payload()),
        header_map=headers(idempotency_key=key, x_connector_version=version),
    )

    assert response.status == 202
    body = route.bodies()[0]
    assert body.get("connector_version") == version


@pytest.mark.asyncio
async def test_step7_more_than_1000_observations_is_a_422(route: Route) -> None:
    await route.seed()
    valid = reading(0, component_id=None)
    body = {"external_device_id": EXTERNAL_DEVICE, "observations": [valid] * 1_001}
    raw = json.dumps(body, separators=(",", ":")).encode()
    assert len(raw) <= MAX_INGEST_BODY_BYTES

    response = await post(route.app, raw)

    assert response.json() == {
        "detail": [{"loc": ["body", "observations"], "msg": "invalid value", "type": "too_long"}]
    }


@pytest.mark.asyncio
async def test_step7_runs_before_the_receipt_lookup(route: Route) -> None:
    await route.seed()
    await post(route.app, encode(payload()))

    response = await post(route.app, encode(payload(reading(0, value="bad"))))

    _assert_redacted_422(response)


@pytest.mark.asyncio
async def test_the_request_hash_is_canonical(route: Route) -> None:
    await route.seed()
    first = await post(route.app, encode(payload()))
    document = payload()
    reordered = {
        "observations": [dict(reversed(list(item.items()))) for item in document["observations"]],
        "external_device_id": document["external_device_id"],
    }

    again = await post(route.app, json.dumps(reordered, indent=2).encode())

    assert again.status == 202
    assert again.json() == {**first.json(), "replayed": True}


# Steps 9-14 over the route ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_step9_inactive_integration_is_403(route: Route) -> None:
    await route.seed()
    await route.subject.seed_device_integration(
        device_integration(status=DeviceIntegrationStatus.DECOMMISSIONED, version=2)
    )

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (403, {"detail": "forbidden"})
    assert route.subject.store.receipts == {}


@pytest.mark.asyncio
async def test_step9_unknown_external_device_is_the_same_403(route: Route) -> None:
    await route.seed()

    response = await post(
        route.app,
        encode({"external_device_id": "sensor-unknown", "observations": [reading(0)]}),
    )

    assert (response.status, response.json()) == (403, {"detail": "forbidden"})


@pytest.mark.asyncio
async def test_step10_component_rule_body(route: Route) -> None:
    await route.seed()

    response = await post(
        route.app, encode(payload(reading(0), reading(1, component_id=OTHER_COMPONENT)))
    )

    assert response.status == 422
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "observations", 1, "component_id"],
                "msg": "component_id is not valid for this device",
                "type": "value_error",
            }
        ]
    }


@pytest.mark.asyncio
async def test_step11_entitlement_denial_is_403(route: Route) -> None:
    await route.seed()
    route.subject.entitlements.allow = False

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (403, {"detail": "forbidden"})
    assert route.bodies() == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation", ["get_ingress_receipt", "resolve_external_identity", "get_component"]
)
async def test_store_failures_after_the_body_are_503(route: Route, operation: str) -> None:
    await route.seed()
    route.subject.fail_next(
        operation,
        ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, "GetItem"),
    )

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (503, {"detail": "service unavailable"})
    assert route.bodies() == []


@pytest.mark.asyncio
async def test_a_mark_failure_after_the_send_is_still_202(route: Route) -> None:
    await route.seed()
    route.subject.fail_next(
        "mark_ingress_receipt_enqueued",
        ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, "UpdateItem"),
    )

    response = await post(route.app, encode(payload()))

    assert response.status == 202
    assert response.json()["replayed"] is False
    assert len(route.bodies()) == 1


@pytest.mark.asyncio
async def test_a_raw_store_error_from_the_send_is_503(route: Route) -> None:
    await route.seed()
    route.subject.fail_next(
        "send", ClientError({"Error": {"Code": "InternalError", "Message": "x"}}, "SendMessage")
    )

    response = await post(route.app, encode(payload()))

    assert (response.status, response.json()) == (503, {"detail": "service unavailable"})
    assert route.bodies() == []


# Redaction -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_route_logs_and_metrics_never_carry_secrets(
    route: Route, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    await route.seed()
    await post(route.app, encode(payload()))
    await post(route.app, encode(payload(reading(7))))
    await post(route.app, b"{bad", header_map=headers(idempotency_key="other-key-SENTINEL"))
    route.subject.fail_next("send", IngressQueueError("down", retryable=True))
    await post(
        route.app, encode(payload()), header_map=headers(idempotency_key="third-key-SENTINEL")
    )
    await post(route.app, encode(payload()), header_map=headers(authorization=f"Bearer {TOKEN}x"))

    text = caplog.text + json.dumps(route.metrics.records)
    secrets = (
        TOKEN,
        SECRET,
        KEY,
        "other-key-SENTINEL",
        "third-key-SENTINEL",
        EXTERNAL_DEVICE,
        credential().secret_sha256,
        credential().fingerprint,
    )
    for secret in secrets:
        assert secret not in text
