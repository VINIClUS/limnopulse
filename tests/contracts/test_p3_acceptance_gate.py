"""P3-11 acceptance gate: the service-free gates, the runbook and the ADR phase split (D19).

- **Gates here:** G11 (limits), G12 (closed metric sets; the storage and log scan is the
  real-service variant), G13 (switch matrix) and G14 (storage rules, optional cache).
- **Real-service gates:** G1-G10, G12 and G15 run on ElasticMQ, DynamoDB Local and InfluxDB in
  `tests/ingestion/test_p3_acceptance_ingestion.py` (`make verify-ingestion`).
  `test_every_gate_has_a_named_test` keeps the G1-G15 -> test id mapping complete.
- **Documents:** `docs/p3-ingestion-rollout-runbook.md` (every topic the plan and the #103
  plan-sync list), the `**Phase split (P3).**` paragraphs of ADR-006, ADR-007 and ADR-016, the
  README link line and the P2 runbook cross-links. Commands and test ids cited by the runbook
  are resolved against the code, so the runbook cannot drift from it.
- **D16 (#104, A48):** the owner chose decision (a), HTTPS only. The runbook's D16 owner gate,
  the P2 runbook's `D16 precondition` and the plan's A48 amendment record it, and none of them
  may still wait for a legacy MQTT producer.
"""

import ast
import hashlib
import itertools
import json
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute

from limnopulse_api.adapters.dynamodb_v2 import DynamoV2KeyBuilder, idempotency_key_hash
from limnopulse_api.adapters.sqs_ingestion import SqsIngressQueue
from limnopulse_api.adapters.telemetry_metrics_log import (
    METRICS_LOGGER,
    StructuredLogTelemetryMetrics,
)
from limnopulse_api.api.openapi_contract import (
    iter_effective_api_routes,
    render_v1_openapi_contract,
)
from limnopulse_api.core.config import Settings
from limnopulse_api.core.errors import IngressMessageTooLarge
from limnopulse_api.domain.ingestion import (
    INGEST_METRICS,
    MAX_INGEST_BODY_BYTES,
    MAX_INGEST_OBSERVATIONS,
    MAX_INGRESS_MESSAGE_BYTES,
)
from limnopulse_api.main import create_app
from limnopulse_api.services.ingress_rate_limit import ingress_rate_limiters
from limnopulse_api.workers import telemetry_ingest
from scripts.admin import telemetry_dlq, telemetry_quarantine
from tests.api.test_ingest_v2_route import (
    Route,
    encode,
    headers,
    payload,
    post,
    reading,
)
from tests.contracts.test_p2_acceptance_gate import fenced_blocks, markdown_section
from tests.support.ingestion_contract_kits import (
    ACCOUNT,
    COMPONENT,
    CREDENTIAL,
    TENANT,
    envelope_of_message_bytes,
    oversized_envelope,
)

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs/p3-ingestion-rollout-runbook.md"
RUNBOOK_LINK = "docs/p3-ingestion-rollout-runbook.md"
P2_RUNBOOK = ROOT / "docs/p2-telemetry-rollout-runbook.md"
PLAN = ROOT / "docs/superpowers/plans/2026-10-30-limnopulse-v4-p3-ingress-integration.md"
README = ROOT / "README.md"
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
INGESTION_GATE = ROOT / "tests/ingestion/test_p3_acceptance_ingestion.py"
ADR_DIR = ROOT / "docs/adr"
ADR_006 = ADR_DIR / "ADR-006-telemetry-has-three-timestamps.md"
ADR_007 = ADR_DIR / "ADR-007-influx-v2-dual-write-migration.md"
ADR_016 = ADR_DIR / "ADR-016-eventbridge-is-selective-sqs-is-durable.md"
COMPOSE_PRODUCTION = ROOT / "compose.production.yaml"
GATES = range(1, 16)


# --- Gate map completeness ----------------------------------------------------------------------


def _gate_tests(path: Path) -> dict[int, list[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: dict[int, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            match = re.fullmatch(r"test_gate(\d+)_\w+", node.name)
            if match:
                found.setdefault(int(match.group(1)), []).append(node.name)
    return found


def test_every_gate_has_a_named_test() -> None:
    here = _gate_tests(Path(__file__))
    there = _gate_tests(INGESTION_GATE)
    assert set(here) | set(there) == set(GATES)
    for gate in GATES:
        names = here.get(gate, []) + there.get(gate, [])
        assert names, f"G{gate} has no test_gate{gate}_ test"
    # The service-free gates are proven here, the others against real services.
    assert set(here) == {11, 12, 13, 14}
    assert set(there) == {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15}


# --- G11: limits ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gate11_limits_reject_before_any_queue_admission() -> None:
    route = Route()
    await route.seed()

    declared = await post(route.app, b"{}", content_length=str(MAX_INGEST_BODY_BYTES + 1))
    assert (declared.status, declared.json()) == (413, {"detail": "request body too large"})
    assert declared.bytes_read == 0

    chunk = b" " * 65_536
    streamed = await post(route.app, chunks=[chunk] * 4, content_length=None)
    assert streamed.status == 413
    assert MAX_INGEST_BODY_BYTES < streamed.bytes_read <= MAX_INGEST_BODY_BYTES + len(chunk)

    unsupported = await post(
        route.app, encode(payload()), header_map=headers(content_type="text/plain")
    )
    assert (unsupported.status, unsupported.json()) == (415, {"detail": "unsupported media type"})
    assert unsupported.bytes_read == 0

    compact = reading(0, component_id=None)
    raw = json.dumps(
        {
            "external_device_id": "sensor-01",
            "observations": [compact] * (MAX_INGEST_OBSERVATIONS + 1),
        },
        separators=(",", ":"),
    ).encode()
    assert len(raw) <= MAX_INGEST_BODY_BYTES
    too_many = await post(route.app, raw)
    assert (too_many.status, too_many.json()["detail"][0]["type"]) == (422, "too_long")
    short_key = await post(
        route.app, encode(payload()), header_map=headers(idempotency_key="short")
    )
    assert short_key.status == 422
    assert short_key.json()["detail"][0]["loc"] == ["header", "idempotency-key"]

    # 429 with Retry-After from the real per-account limiter, before the receipt and the queue.
    limiters = ingress_rate_limiters(
        Settings(
            _env_file=None,
            app_env="test",
            ingest_rate_limit_per_minute=1,
            ingest_rate_limit_burst=1,
        )
    )
    route.app.state.ingress_rate_limiter = limiters.account
    first = await post(
        route.app, encode(payload()), header_map=headers(idempotency_key="g11-first-0001")
    )
    assert first.status == 202
    limited = await post(
        route.app, encode(payload()), header_map=headers(idempotency_key="g11-limited-0001")
    )
    assert (limited.status, limited.json()) == (429, {"detail": "rate limited"})
    assert int(limited.headers["retry-after"]) >= 1
    assert limited.bytes_read == 0
    assert len(route.bodies()) == 1, "only the first request reached the queue"
    assert (
        await route.subject.receipts.get_ingress_receipt(
            ACCOUNT, idempotency_key_hash("g11-limited-0001")
        )
        is None
    ), "no receipt is claimed for a rate-limited request"
    assert route.outcomes().count("rate_limited") == 1


class _RecordingSqs:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, str]:
        self.sent.append(kwargs)
        return {"MessageId": f"m-{len(self.sent)}"}


@pytest.mark.asyncio
async def test_gate11_envelope_size_guard_never_sends_an_oversized_message() -> None:
    client = _RecordingSqs()
    queue = SqsIngressQueue(client=client, queue_url="https://sqs.invalid/queue")

    with pytest.raises(IngressMessageTooLarge):
        await queue.send(oversized_envelope())
    assert client.sent == []

    await queue.send(envelope_of_message_bytes(MAX_INGRESS_MESSAGE_BYTES))
    assert len(client.sent) == 1


# --- G12: closed metric names and labels ---------------------------------------------------------

# The plan's Metrics table, with every closed enum written out (never derived from production).
PLAN_METRICS: dict[str, dict[str, frozenset[str]]] = {
    "ingest_requests_total": {
        "outcome": frozenset(
            {
                "accepted",
                "replayed",
                "unauthorized",
                "rate_limited",
                "unsupported_media",
                "too_large",
                "invalid",
                "conflict",
                "forbidden",
                "unavailable",
            }
        )
    },
    "ingest_admission_denied_total": {
        "reason": frozenset(
            {
                "identity_unresolved",
                "integration_mismatch",
                "integration_inactive",
                "account_inactive",
                "device_inactive",
                "component_required",
                "component_invalid",
                "entitlement_denied",
            }
        )
    },
    "ingest_observations_admitted_total": {},
    "ingress_messages_total": {
        "outcome": frozenset({"processed", "fenced", "retry", "permanent"}),
        # The worker failure table's categories.
        "category": frozenset(
            {
                "envelope_invalid",
                "fenced",
                "fenced_unverified",
                "deferred",
                "unexpected",
                "store_unavailable",
                "write_retryable",
                "write_permanent",
                "contract_violation",
                "processed",
            }
        ),
    },
    "ingress_observations_total": {"result": frozenset({"accepted", "quarantined"})},
    "ingress_quarantined_total": {
        "reason": frozenset(
            {
                "metric_unknown",
                "unit_unsupported",
                "value_invalid",
                "observed_at_invalid",
                "component_not_owned",
                "component_not_measuring",
                "deployment_unresolved",
            }
        )
    },
    "ingress_fenced_total": {
        "reason": frozenset(
            {
                "identity_unresolved",
                "integration_mismatch",
                "integration_decommissioned",
                "account_revoked",
                "device_decommissioned",
            }
        )
    },
    "ingress_deferred_total": {
        "reason": frozenset({"integration_not_active", "account_not_active", "device_not_active"})
    },
    "ingress_quality_flags_total": {
        "flag": frozenset(
            {
                "delayed",
                "clock_skew_future",
                "clock_skew_past_extreme",
                "replay",
                "duplicate",
                "sequence_gap",
                "sequence_regression",
            }
        )
    },
    "ingress_sequence_events_total": {
        "event": frozenset({"first", "contiguous", "gap", "regression", "duplicate", "late"})
    },
    "ingress_write_failures_total": {"retryable": frozenset({"true", "false"})},
    "ingress_normalization_duration_seconds": {},
    "ingress_queue_age_seconds": {},
    "ingress_dlq_redriven_total": {"result": frozenset({"redriven", "failed"})},
    "ingress_quarantine_replayed_total": {"result": frozenset({"replayed", "skipped", "failed"})},
}


def test_gate12_metrics_use_the_closed_name_and_label_sets(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert {name: dict(labels) for name, labels in INGEST_METRICS.items()} == {
        name: {label: frozenset(str(v) for v in values) for label, values in labels.items()}
        for name, labels in PLAN_METRICS.items()
    }
    caplog.set_level(logging.INFO, logger=METRICS_LOGGER)
    # #116: the API sets `propagate=False` on the metrics logger; capture on it directly.
    metrics_logger = logging.getLogger(METRICS_LOGGER)
    monkeypatch.setattr(metrics_logger, "propagate", False)
    monkeypatch.setattr(metrics_logger, "handlers", [caplog.handler])
    metrics = StructuredLogTelemetryMetrics()
    metrics.increment("ingress_fenced_total", reason="account_revoked")
    for name, labels in (
        ("ingress_fenced_total", {"reason": TENANT}),  # an open value
        ("ingress_fenced_total", {"reason": "account_revoked", "tenant": TENANT}),  # open label
        ("ingress_tenant_total", {}),  # an unknown name
        ("ingest_requests_total", {"outcome": CREDENTIAL}),
    ):
        with pytest.raises(ValueError):
            metrics.increment(name, **labels)
    lines = [json.loads(record.getMessage()) for record in caplog.records]
    assert lines == [
        {"metric": "ingress_fenced_total", "value": 1, "labels": {"reason": "account_revoked"}}
    ]
    for value in (TENANT, CREDENTIAL, ACCOUNT, COMPONENT):
        assert value not in caplog.text


# --- G13: switch matrix --------------------------------------------------------------------------

SWITCHES = (
    "api_v2_enabled",
    "api_v2_telemetry_enabled",
    "telemetry_v2_write_enabled",
    "ingest_v2_enabled",
)
FLAG_ENVIRONMENT = (
    "API_V2_ENABLED",
    "API_V2_TELEMETRY_ENABLED",
    "TELEMETRY_V2_WRITE_ENABLED",
    "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED",
    "V1_TELEMETRY_READ_SOURCE",
    "V1_POND_DEVICE_PROJECTION_ENABLED",
    "INGEST_V2_ENABLED",
    "SQS_ENDPOINT_URL",
    "SQS_TELEMETRY_INGRESS_QUEUE_URL",
    "SQS_TELEMETRY_INGRESS_DLQ_URL",
)
QUEUE_URL = "http://localhost:9324/000000000000/limnopulse-telemetry-ingress"
CREDENTIALS_PATH = (
    "/v2/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials"
)
D15_OPERATIONS = frozenset(
    {
        ("/ingest/v2/observations", "post"),
        (CREDENTIALS_PATH, "post"),
        (CREDENTIALS_PATH, "get"),
        (CREDENTIALS_PATH + "/{ingest_credential_id}/revoke", "post"),
        ("/v2/tenants/{tenant_id}/integration-accounts/{integration_account_id}/status", "post"),
        (
            (
                "/v2/tenants/{tenant_id}/devices/{device_id}/integrations/{device_integration_id}"
                "/status"
            ),
            "post",
        ),
    }
)


def _operations(app: FastAPI) -> frozenset[tuple[str, str]]:
    return frozenset(
        (route.path_format, method.lower())
        for route in iter_effective_api_routes(app)
        for method in route.methods
    )


def _switch_app(**switches: bool) -> FastAPI:
    values: dict[str, Any] = {"app_env": "test", "auth_mode": "dev", **switches}
    if switches.get("ingest_v2_enabled"):
        values["sqs_telemetry_ingress_queue_url"] = QUEUE_URL
    return create_app(Settings(_env_file=None, **values))


def test_gate13_switch_matrix_keeps_v1_and_mounts_exactly_the_switched_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in FLAG_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    golden = GOLDEN.read_text(encoding="utf-8")
    base = _operations(_switch_app())
    m1 = _operations(_switch_app(api_v2_enabled=True)) - base
    telemetry = (
        _operations(_switch_app(api_v2_enabled=True, api_v2_telemetry_enabled=True)) - base - m1
    )

    def tenant_routes(operations: frozenset[tuple[str, str]] | set[tuple[str, str]]) -> int:
        return len([op for op in operations if "{tenant_id}" in op[0]])

    # The M1 count of 53 tenant routes (28 /v1 + 25 /v2) is unchanged; ingest adds exactly 5.
    assert tenant_routes(base | m1) == 53
    assert len(telemetry) == 3
    assert not D15_OPERATIONS & (base | m1 | telemetry)

    valid = 0
    for values in itertools.product((False, True), repeat=len(SWITCHES)):
        switches = dict(zip(SWITCHES, values, strict=True))
        ingest_ready = switches["api_v2_enabled"] and switches["telemetry_v2_write_enabled"]
        if (switches["ingest_v2_enabled"] and not ingest_ready) or (
            switches["api_v2_telemetry_enabled"] and not switches["api_v2_enabled"]
        ):
            with pytest.raises(ValueError):
                _switch_app(**switches)
            continue
        app = _switch_app(**switches)
        valid += 1
        assert render_v1_openapi_contract(app) == golden, switches
        expected = set(base)
        if switches["api_v2_enabled"]:
            expected |= m1
            if switches["api_v2_telemetry_enabled"]:
                expected |= telemetry
        if switches["ingest_v2_enabled"]:
            expected |= D15_OPERATIONS
        assert _operations(app) == expected, switches
        if switches["ingest_v2_enabled"]:
            telemetry_routes = 3 if switches["api_v2_telemetry_enabled"] else 0
            assert tenant_routes(_operations(app)) == 53 + telemetry_routes + 5
    assert valid == 8


@pytest.mark.parametrize(
    ("environment", "case"),
    [
        (
            {"TELEMETRY_V2_WRITE_ENABLED": "false", "SQS_TELEMETRY_INGRESS_QUEUE_URL": QUEUE_URL},
            "TELEMETRY_V2_WRITE_ENABLED is off",
        ),
        (
            {"TELEMETRY_V2_WRITE_ENABLED": "true", "SQS_TELEMETRY_INGRESS_QUEUE_URL": ""},
            "SQS_TELEMETRY_INGRESS_QUEUE_URL is empty",
        ),
    ],
)
def test_gate13_worker_exits_2_unless_it_can_write(
    tmp_path: Path, environment: dict[str, str], case: str
) -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in FLAG_ENVIRONMENT and not key.startswith(("AWS_", "INFLUXDB_"))
    }
    env.update({"APP_ENV": "test", "AUTH_MODE": "dev", **environment})
    completed = subprocess.run(
        [sys.executable, "-m", "limnopulse_api.workers.telemetry_ingest"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == telemetry_ingest.EXIT_REFUSED == 2, (case, completed.stderr)
    # Refused by the start guard itself, not by an unrelated settings error.
    assert f"refused to start: {case}" in completed.stderr, completed.stderr


# --- G14: storage rules and the optional cache ---------------------------------------------------

SRC = ROOT / "src/limnopulse_api"
LISTING_PREFIXES = (
    "SITE#",
    "ASSET#",
    "DEVICE#",
    "COMPONENT#",
    "CAPABILITY#",
    "INTEGRATION_ACCOUNT#",
    "DEVICE_INTEGRATION#",
    "DEPLOYMENT#",
    "POND#",
    "IDEMPOTENCY#",
)
P3_MODULES = (
    "adapters/dynamodb_ingest_credentials.py",
    "adapters/dynamodb_ingress_receipts.py",
    "adapters/dynamodb_quarantine.py",
    "adapters/dynamodb_ingest_state.py",
    "adapters/sqs_ingestion.py",
    "adapters/telemetry_metrics_log.py",
    "api/ingest_v2.py",
    "api/ingestion_dependencies.py",
    "api/v2/routers/ingest_credentials.py",
    "api/v2/routers/integration_lifecycle.py",
    "services/connector_auth.py",
    "services/ingest_credentials.py",
    "services/ingest_processor.py",
    "services/ingest_state.py",
    "services/ingestion.py",
    "services/ingress_entitlements.py",
    "services/ingress_rate_limit.py",
    "services/ingress_redrive.py",
    "services/integration_lifecycle.py",
    "services/trusted_mapping.py",
    "workers/telemetry_ingest.py",
    "workers/telemetry_ingest_loop.py",
)


def _python_sources() -> dict[Path, str]:
    roots = [SRC, ROOT / "scripts/admin"]
    return {
        path: path.read_text(encoding="utf-8")
        for root in roots
        for path in sorted(root.rglob("*.py"))
    }


def test_gate14_no_scan_no_new_index_no_new_table_and_no_cache_dependency() -> None:
    sources = _python_sources()
    # No Scan anywhere; GSIs are queried only by the pre-P3 alert-event adapter.
    for path, text in sources.items():
        assert not re.search(r"\.scan\(|[\"']Scan[\"']|paginate\([\"']scan", text), path
        assert "GlobalSecondaryIndex" not in text and "create_table" not in text, path
    users = {
        path.relative_to(SRC).as_posix() for path, text in sources.items() if "IndexName" in text
    }
    assert users == {"adapters/alert_events.py"}

    # No new table and no new GSI: two tables and the four pre-P3 indexes, locally and in tofu.
    init = (ROOT / "scripts/dev/init_dynamodb.py").read_text(encoding="utf-8")
    assert sorted(re.findall(r'"IndexName": "(\w+)"', init)) == sorted(
        {
            "AlertEvaluationByDue",
            "AlertEventsByTenantTime",
            "ActiveAlertEventsByTenantTime",
            "NotificationRelayByAvailableAt",
        }
    )
    tofu = "\n".join(p.read_text(encoding="utf-8") for p in (ROOT / "infra/opentofu").glob("*.tf"))
    assert len(re.findall(r'resource "aws_dynamodb_table"', tofu)) == 2

    # No prefix collision in the tenant partition; every other P3 row has its own partition.
    keys = DynamoV2KeyBuilder()
    tenant_sks = [
        keys.ingest_credential_listing(TENANT, ACCOUNT, CREDENTIAL)["SK"],
        keys.ingest_credential_listing_prefix(ACCOUNT),
        keys.ingest_credential_slot(TENANT, ACCOUNT, 1)["SK"],
        keys.ingest_credential_slot(TENANT, ACCOUNT, 2)["SK"],
    ]
    for sk in tenant_sks:
        for prefix in LISTING_PREFIXES:
            assert not sk.startswith(prefix) and not prefix.startswith(sk.split("#")[0] + "#"), (
                sk,
                prefix,
            )
    own_partitions = {
        keys.ingest_credential(CREDENTIAL)["PK"].split("#")[0],
        keys.ingress_receipt(ACCOUNT, "a" * 64)["PK"].split("#")[0],
        keys.quarantine_partition(TENANT).split("#")[0],
    }
    assert own_partitions == {"INGEST_CREDENTIAL", "INGRESS_RECEIPT", "QUARANTINE"}
    assert keys.component_ingest_state(COMPONENT)["SK"] == "INGEST_STATE#CURRENT"
    assert keys.connector_health(ACCOUNT)["SK"] == "CONNECTOR_HEALTH#CURRENT"

    # No correctness dependency on the optional cache: no P3 module touches it ...
    for module in P3_MODULES:
        text = (SRC / module).read_text(encoding="utf-8")
        assert not re.search(
            r"redis|cache_repository|repositories\.cache|CacheRepository", text, re.IGNORECASE
        ), module


def test_gate14_the_composed_ingest_route_has_no_cache_dependency() -> None:
    app = _switch_app(api_v2_enabled=True, telemetry_v2_write_enabled=True, ingest_v2_enabled=True)
    routes = [
        getattr(route, "original_route", route)
        for route in iter_effective_api_routes(app)
        if route.path_format == "/ingest/v2/observations"
    ]
    assert len(routes) == 1 and isinstance(routes[0], APIRoute)

    def calls(dependant: Any) -> list[Any]:
        found = [dependant.call]
        for dependency in dependant.dependencies:
            found.extend(calls(dependency))
        return found

    names = {getattr(call, "__qualname__", repr(call)) for call in calls(routes[0].dependant)}
    assert not {name for name in names if re.search(r"cache|redis|membership", name, re.IGNORECASE)}


# --- Runbook -------------------------------------------------------------------------------------

RUNBOOK_SECTIONS = (
    "Preconditions",
    "Enablement order",
    "Production environment",
    "Alarms and deadlines",
    "DLQ drill",
    "Quarantine reconciliation drill",
    "Credentials",
    "Decommissioning",
    "Rollback",
    "Owner actions",
    "ADR gate map",
    "Acceptance matrix",
)


def runbook() -> str:
    assert RUNBOOK.is_file(), f"missing {RUNBOOK.relative_to(ROOT)}"
    return RUNBOOK.read_text(encoding="utf-8")


def section(name: str) -> str:
    return " ".join(markdown_section(runbook(), name).split())


def numbered_items(text: str) -> list[str]:
    items = re.split(r"^\d+\. ", text, flags=re.MULTILINE)[1:]
    return [" ".join(item.split()) for item in items]


def test_runbook_has_the_sections_in_order() -> None:
    headings = re.findall(r"^## (.+?)\s*$", runbook(), flags=re.MULTILINE)
    assert headings == list(RUNBOOK_SECTIONS)
    assert runbook().startswith("# P3 Ingestion Rollout Runbook\n")


def test_runbook_preconditions() -> None:
    text = section("Preconditions")
    for pattern in (r"G1.{0,8}G15", r"#103", r"`?telemetry_ingress`?", r"dark", r"D16"):
        assert re.search(pattern, text), pattern


def test_runbook_records_the_section_21_2_queue_properties() -> None:
    """Final Acceptance Matrix row §21.2: the runbook states the queue properties P3-02 provisions,
    each read from OpenTofu so the document follows the infrastructure."""
    text = section("Preconditions")
    tofu = (ROOT / "infra/opentofu/ingestion_queues.tf").read_text(encoding="utf-8")
    retention = [int(v) for v in re.findall(r"message_retention_seconds\s*=\s*(\d+)", tofu)]
    assert sorted(retention) == [4 * 86_400, 14 * 86_400]
    visibility = re.search(r"visibility_timeout_seconds\s*=\s*(\d+)", tofu).group(1)
    long_poll = re.search(r"receive_wait_time_seconds\s*=\s*(\d+)", tofu).group(1)
    for pattern in (
        r"SSE",
        r"4 days",
        r"14 days",
        rf"\b{visibility} ?s",
        rf"\b{long_poll} ?s",
        r"`telemetry-ingress@1`",
        r"opaque",
        r"least privilege",
        r"limnopulse-telemetry-ingress-dlq",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


ENABLEMENT_STEPS = (
    ("infrastructure flag", (r"`telemetry_ingress`", r"tofu|OpenTofu")),
    ("worker", (r"`TELEMETRY_V2_WRITE_ENABLED`", r"telemetry_ingest", r"principal|IAM user")),
    ("endpoint", (r"`INGEST_V2_ENABLED`",)),
    ("pilot onboarding", (r"account.*credential.*DeviceIntegration.*activ", r"pilot")),
    ("divergence checks", (r"divergence",)),
    ("legacy dual write", (r"`TELEMETRY_LEGACY_DUAL_WRITE_ENABLED`", r"G7")),
    (
        "remaining P2 steps",
        (r"p2-telemetry-rollout-runbook\.md", r"\[D16 owner gate\]\(#d16-owner-gate\)"),
    ),
)


def test_runbook_enablement_order_is_the_plan_order() -> None:
    items = numbered_items(markdown_section(runbook(), "Enablement order"))
    assert len(items) == len(ENABLEMENT_STEPS)
    for item, (step, patterns) in zip(items, ENABLEMENT_STEPS, strict=True):
        for pattern in patterns:
            assert re.search(pattern, item), (step, pattern)
    # Legacy dual write only after G7 passed; never before the worker writes v2.
    assert re.search(r"only after .*G7", items[5])


def _worker_interpolations() -> set[str]:
    text = COMPOSE_PRODUCTION.read_text(encoding="utf-8")
    start = text.index("  telemetry-ingest-worker:")
    following = re.search(r"^  [a-z][a-z0-9-]*:\s*$", text[start + 3 :], flags=re.MULTILINE)
    block = text[start : start + 3 + following.start()] if following else text[start:]
    return set(re.findall(r"\$\{([A-Z0-9_]+)", block))


def test_runbook_production_environment_covers_the_worker_env_and_principal() -> None:
    text = section("Production environment")
    names = _worker_interpolations()
    assert {
        "TELEMETRY_INGEST_INFLUXDB_TOKEN",
        "TELEMETRY_INGEST_AWS_ACCESS_KEY_ID",
        "TELEMETRY_INGEST_AWS_SECRET_ACCESS_KEY",
    } <= names
    for name in sorted(names):
        assert f"`{name}`" in text, name
    for pattern in (
        r"write token",
        r"never .{0,40}(bootstrap|admin) .{0,20}token",
        r"`telemetry_ingest_iam_user_name`",
        r"access key",
        r"empty",
        r"exit(s)? 2|exit code 2",
        r"`stop_grace_period`",
        r"(exceed|above|more than|greater)[^.]{0,60}drain.{0,40}wait.{0,40}5 ?s",
        r"`--env-file`",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def test_runbook_alarms_and_deadlines() -> None:
    text = section("Alarms and deadlines")
    tofu = (ROOT / "infra/opentofu/ingestion_queues.tf").read_text(encoding="utf-8")
    max_receive_count = int(re.search(r"maxReceiveCount\s*=\s*(\d+)", tofu).group(1))
    suffixes = re.findall(
        r'alarm_name\s*=\s*"\$\{var\.project_name\}-\$\{var\.environment\}'
        r'-([a-z-]+)"',
        tofu,
    )
    assert suffixes
    for suffix in suffixes:
        assert suffix in text, suffix
    for pattern in (
        r"queue[- ]age",
        r"DLQ",
        rf"`maxReceiveCount`.{{0,20}}\b{max_receive_count}\b",
        r"20.{0,3}45 minutes",
        r"14 days",
        r"quarantine",
        r"reconcil",
        r"stopped worker",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def _commands(text: str, script: str) -> list[list[str]]:
    found = []
    for block in fenced_blocks(text, "bash"):
        for line in block.splitlines():
            match = re.search(rf"scripts/admin/{script}\.py\s+(.*)$", line.strip())
            if match:
                found.append(match.group(1).split())
    return found


def _substitute(argv: list[str]) -> list[str]:
    values = {
        "<tenant_id>": "tnt_local_001",
        "<entry_key>": "2026-10-31T00:00:00.000000Z#qtn_x",
        "<integration_account_id>": "ia_" + "1" * 32,
    }
    return [values.get(arg, arg) for arg in argv]


def test_runbook_dlq_drill_commands_parse_and_dry_run_is_the_default() -> None:
    raw = markdown_section(runbook(), "DLQ drill")
    commands = _commands(raw, "telemetry_dlq")
    subcommands = [argv[0] for argv in commands]
    assert "sample" in subcommands
    assert ["redrive"] in [argv[:1] for argv in commands if "--apply" not in argv]
    assert any(argv[0] == "redrive" and "--apply" in argv for argv in commands)
    for argv in commands:
        args = telemetry_dlq._parser().parse_args(_substitute(argv))
        assert args.command in {"sample", "redrive"}
        if args.command == "redrive" and "--apply" not in argv:
            assert args.apply is False
    text = " ".join(raw.split())
    for pattern in (
        r"dry run",
        r"diagnos",
        r"`--apply`",
        r"G7",
        r"test_gate7_\w+",
        r"byte",
        r"ran|rehears",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def test_runbook_quarantine_drill_commands_parse_and_cover_reruns() -> None:
    raw = markdown_section(runbook(), "Quarantine reconciliation drill")
    commands = _commands(raw, "telemetry_quarantine")
    assert {argv[0] for argv in commands} >= {"list", "replay", "dismiss"}
    assert any(argv[0] == "replay" and "--apply" in argv for argv in commands)
    for argv in commands:
        telemetry_quarantine._parser().parse_args(_substitute(argv))
    text = " ".join(raw.split())
    for pattern in (
        r"dry run",
        r"deployment_unresolved",
        r"`replayed`",
        r"`dismissed`",
        r"partial",
        r"new replay id|new replay `ingress_id`",
        r"#116",
        r"14 days",
        r"G8",
        r"test_gate8_\w+",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def test_runbook_credentials_rotation_revocation_and_lost_issuance() -> None:
    text = section("Credentials")
    for pattern in (
        r"rotat",
        r"two (active )?(credentials|slots)",
        r"revok",
        r"next request",
        r"lost",
        r"slot",
        r"fingerprint",
        r"once",
        r"`Cache-Control: no-store`",
        r"does not fence|never fences|not fence",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def test_runbook_decommissioning_and_d22() -> None:
    text = section("Decommissioning")
    for pattern in (
        r"decommission",
        r"D22",
        r"before P3-04",
        r"re-?create",
        r"403",
        r"fenced",
        r"stays claimed|remains claimed|never released",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def test_runbook_rollback_order_keeps_the_queued_messages() -> None:
    raw = markdown_section(runbook(), "Rollback")
    items = numbered_items(raw)
    assert items, "the rollback order must be a numbered list"

    def first(pattern: str) -> int:
        matches = [i for i, item in enumerate(items) if re.search(pattern, item)]
        assert matches, pattern
        return matches[0]

    pilots = first(r"pilot")
    endpoint = first(r"`INGEST_V2_ENABLED=false`")
    worker = first(r"`TELEMETRY_V2_WRITE_ENABLED=false`")
    assert pilots < endpoint < worker
    dual_write = first(r"`TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=false`")
    assert dual_write <= worker
    text = " ".join(raw.split())
    for pattern in (r"§24", r"retain", r"never purge|do not purge", r"DLQ", r"legacy"):
        assert re.search(pattern, text, re.IGNORECASE), pattern


def required_checks() -> list[str]:
    """The status-check contexts the owner can require: the `verify.yml` job keys plus `sonar`."""
    verify = (ROOT / ".github/workflows/verify.yml").read_text(encoding="utf-8")
    jobs = verify[verify.index("\njobs:") :]
    names = re.findall(r"^  ([a-z][a-z0-9_-]*):\s*$", jobs, flags=re.MULTILINE)
    assert {"python", "go", "ingestion"} <= set(names)
    return [*names, "sonar"]


def test_runbook_owner_actions() -> None:
    text = section("Owner actions")
    for pattern in (
        r"D16",
        r"HTTPS only \(#104\)",
        r"### D16 owner gate",
        r"Backlog",
        r"owner",
        r"production switch",
        r"#15",
        r"required status checks",
        r"ruleset",
    ):
        assert re.search(pattern, text, re.IGNORECASE), pattern
    for check in required_checks():
        assert f"`{check}`" in text, check


# --- D16 read switches: #104 decision (a), HTTPS only (A48) --------------------------------------

D16_DECISION = re.compile(r"HTTPS only \(#104\)")
D16_OWNER_GATE_LINK = "(#d16-owner-gate)"
EVALUATOR_FLIP = "`ALERT_EVALUATOR_TELEMETRY_SOURCE=v2`"
V1_READ_FLIP = "`V1_TELEMETRY_READ_SOURCE=v2`"
# The wording of the open producer-path decision that #104 settled: nothing waits for a producer.
D16_SUPERSEDED = (
    "DEFERRED → #104",
    "D16 stays open",
    "D16 open gate",
    "still need a producer",
    "until #104 closes D16",
    "producer-path decision",
    "decomposed only after",
)
D16_OWNER_GATE = (
    ("no legacy MQTT device", (r"[Cc]onfirm", r"still has no legacy MQTT device", r"stay off")),
    (
        "keyed divergence",
        (
            r"keyed divergence checks",
            r"p2-telemetry-rollout-runbook\.md#keyed-divergence-checks",
            r"D16 window",
            r"pass criterion",
        ),
    ),
    ("evaluator flip", (re.escape(EVALUATOR_FLIP), r"one environment first", r"D15")),
    ("/v1 read flip", (re.escape(V1_READ_FLIP), r"retention", r"legacy read window")),
)


def test_runbook_preconditions_name_the_d16_decision() -> None:
    text = section("Preconditions")
    assert D16_DECISION.search(text)
    assert EVALUATOR_FLIP in text and V1_READ_FLIP in text
    assert D16_OWNER_GATE_LINK in text


def test_runbook_owner_actions_record_the_d16_https_only_decision() -> None:
    text = section("Owner actions")
    assert D16_DECISION.search(text)
    for pattern in (
        r"[Pp]roduction has no legacy MQTT device",
        r"0 domain items as of 2026-10-31",
        r"issues/15#issuecomment-6548338824",
        r"P3 HTTPS ingest",
        r"`POST /ingest/v2/observations`",
        r"ingest credential",
        r"[Nn]o MQTT → v2 producer is built",
        r"#104 stays open until",
    ):
        assert re.search(pattern, text), pattern


def test_runbook_d16_owner_gate_is_the_ordered_owner_checklist() -> None:
    owner_actions = markdown_section(runbook(), "Owner actions")
    raw = markdown_section(owner_actions, "D16 owner gate", level=3)
    assert re.search(r"owner", raw) and re.search(r"in this order", raw)
    items = numbered_items(raw)
    assert len(items) == len(D16_OWNER_GATE)
    for item, (step, patterns) in zip(items, D16_OWNER_GATE, strict=True):
        for pattern in patterns:
            assert re.search(pattern, item), (step, pattern)
    # Each read switch has its own step: the evaluator first, the `/v1` reads last.
    for index, item in enumerate(items):
        assert (EVALUATOR_FLIP in item) == (index == 2), index
        assert (V1_READ_FLIP in item) == (index == 3), index


def test_runbook_adr_gate_map_d16_row_records_the_decision() -> None:
    raw = markdown_section(runbook(), "ADR gate map")
    (row,) = [line for line in raw.splitlines() if line.startswith("| ADR-007 / D16")]
    assert D16_DECISION.search(row)
    assert D16_OWNER_GATE_LINK in row
    assert "DEFERRED" not in row
    assert "::test_gate15_https_path_matches_the_independent_legacy_writer`" in row


def test_runbook_no_longer_waits_for_a_legacy_mqtt_producer() -> None:
    text = " ".join(runbook().split())
    for phrase in D16_SUPERSEDED:
        assert phrase not in text, phrase


def test_p2_runbook_d16_precondition_records_the_https_only_decision() -> None:
    raw = markdown_section(P2_RUNBOOK.read_text(encoding="utf-8"), "D16 precondition")
    text = " ".join(raw.split())
    assert D16_DECISION.search(text)
    for pattern in (
        r"[Pp]roduction has no legacy MQTT device",
        r"P3 HTTPS ingest",
        r"[Nn]o MQTT → v2 producer is built",
        r"legacy MQTT devices? keeps? both read switches off",
        r"\]\(p3-ingestion-rollout-runbook\.md#d16-owner-gate\)",
    ):
        assert re.search(pattern, text), pattern
    assert "after P3 delivers a producer" not in text


def _plan_rows(heading: str, first_cell: str) -> list[str]:
    raw = markdown_section(PLAN.read_text(encoding="utf-8"), heading)
    return [line for line in raw.splitlines() if line.startswith(f"| {first_cell}")]


def test_plan_a48_amendment_records_the_d16_https_only_decision() -> None:
    rows = _plan_rows("Audit amendments (r2)", "A")
    ids = [int(re.match(r"\| A(\d+) \|", row).group(1)) for row in rows]
    assert ids == list(range(1, 49)), "A48 must be the next and last amendment"
    row = rows[-1]
    assert row.startswith("| A48 | owner decision on #104 |")
    assert D16_DECISION.search(row)
    for pattern in (
        r"[Pp]roduction has no legacy MQTT device",
        r"0 domain items as of 2026-10-31",
        r"P3 HTTPS ingest",
        r"[Nn]o MQTT → v2 producer is built",
        r"#104 stays open until",
        r"[Aa]pplied inline",
    ):
        assert re.search(pattern, row), pattern
    gate = row[row.index("D16 owner gate") :]
    steps = ("no legacy MQTT device", "keyed divergence", EVALUATOR_FLIP, V1_READ_FLIP)
    positions = [gate.find(step) for step in steps]
    assert -1 not in positions and positions == sorted(positions), positions


def test_plan_applies_a48_inline() -> None:
    (decision,) = _plan_rows("Entry Decisions (frozen by this plan)", "D16 |")
    (gate_map,) = _plan_rows("Final Acceptance Matrix", "ADR-007 / D16")
    for row in (decision, gate_map):
        assert D16_DECISION.search(row) and "A48" in row, row[:40]
    assert "stays open after P3" not in decision
    text = " ".join(PLAN.read_text(encoding="utf-8").split())
    for phrase in D16_SUPERSEDED:
        assert phrase not in text, phrase


TEST_ID = re.compile(r"`(tests/[\w/]+\.py)::(test_\w+)")


def _assert_test_id(path: str, name: str) -> None:
    file = ROOT / path
    assert file.is_file(), path
    assert re.search(rf"^(async )?def {name}\b", file.read_text(encoding="utf-8"), re.MULTILINE), (
        path,
        name,
    )


def test_runbook_adr_gate_map_cites_existing_tests() -> None:
    raw = markdown_section(runbook(), "ADR gate map")
    rows = [line for line in raw.splitlines() if line.startswith("| ADR-")]
    for adr in ("ADR-001", "ADR-006", "ADR-007", "ADR-016", "ADR-019"):
        cited = [row for row in rows if row.startswith(f"| {adr}")]
        assert cited, adr
        assert any(TEST_ID.search(row) or "DEFERRED" in row for row in cited), adr
    for path, name in TEST_ID.findall(raw):
        _assert_test_id(path, name)


def test_runbook_acceptance_matrix_maps_every_gate() -> None:
    raw = markdown_section(runbook(), "Acceptance matrix")
    rows = {
        int(match.group(1)): line
        for line in raw.splitlines()
        if (match := re.match(r"\| G(\d+) \|", line))
    }
    assert set(rows) == set(GATES)
    for gate, row in rows.items():
        cited = TEST_ID.findall(row)
        assert cited, f"G{gate}"
        assert any(name.startswith(f"test_gate{gate}_") for _path, name in cited), f"G{gate}"
        for path, name in cited:
            _assert_test_id(path, name)
    text = " ".join(raw.split())
    for owner in (r"DEFERRED → #20", r"DEFERRED → #17"):
        assert re.search(owner, text), owner
    # G15 keeps the legacy-device decision next to its proof: #104 chose HTTPS only (A48).
    assert D16_DECISION.search(rows[15]) and D16_OWNER_GATE_LINK in rows[15]
    assert "DEFERRED → #104" not in text


def test_every_test_id_in_the_runbook_exists() -> None:
    cited = TEST_ID.findall(runbook())
    assert len(cited) >= 15
    for path, name in cited:
        _assert_test_id(path, name)


AWS_ACCESS_KEY_ID = re.compile(r"(?<![A-Z0-9])(?:AKIA|ASIA)[0-9A-Z]{16}(?![A-Z0-9])")
# 40 characters of the secret-key alphabet, mixed case, bounded on both sides (a hex digest is not).
AWS_SECRET_ACCESS_KEY = re.compile(
    r"(?<![A-Za-z0-9/+=])(?=[A-Za-z0-9/+]*[a-z])(?=[A-Za-z0-9/+]*[A-Z])"
    r"[A-Za-z0-9/+]{40}(?![A-Za-z0-9/+=])"
)


def test_runbook_never_carries_a_secret() -> None:
    text = runbook()
    assert not re.search(r"lpi_ic_[0-9a-f]{32}_[A-Za-z0-9_-]{43}", text)
    assert not re.search(r"Bearer [A-Za-z0-9_-]{20,}", text)
    assert "local-dev-token" not in text
    # #116: AWS access key ids and 40-character secret access keys, with positive controls built
    # at runtime so neither pattern can silently stop matching.
    assert AWS_ACCESS_KEY_ID.search("id " + "AKIA" + "Q7" * 8 + " end")
    secret_control = "wJalrXUtnFEMI/K7MDENG/bPxRfiCY" + "Ab3/xY9+Zq"
    assert AWS_SECRET_ACCESS_KEY.search(f"key {secret_control}.")
    assert not AWS_SECRET_ACCESS_KEY.search("sha " + "0123456789abcdef" * 2 + "01234567")
    assert not AWS_ACCESS_KEY_ID.search(text)
    assert not AWS_SECRET_ACCESS_KEY.search(text)


def test_runbook_and_p2_runbook_cross_link() -> None:
    assert "](p2-telemetry-rollout-runbook.md)" in runbook()
    p2 = P2_RUNBOOK.read_text(encoding="utf-8")
    links = [line for line in p2.splitlines() if "](p3-ingestion-rollout-runbook.md)" in line]
    assert links, "the P2 runbook must link the P3 runbook"
    assert any("P3" in line and "redrive" in line for line in links)
    assert "P3" in p2 and "redrive" in p2


def test_readme_links_the_p3_runbook_without_the_placeholder() -> None:
    text = README.read_text(encoding="utf-8")
    assert f"[P3 ingestion rollout runbook]({RUNBOOK_LINK})" in text
    assert "(added by P3-11)" not in text
    assert (ROOT / RUNBOOK_LINK).is_file()


# --- ADR phase split (D19) -----------------------------------------------------------------------

HEADINGS = [
    "Context",
    "Decision",
    "Consequences",
    "V4 traceability",
    "Implementation gate",
    "Non-goals",
]
# sha256 of the `## Implementation gate` paragraphs before the P3 paragraph, on main before P3-11.
ADR_GATE_SHA256 = MappingProxyType(
    {
        ADR_006: "e7fc13c626081f37721947f50151c3f1cb1f8fd2d5e93836318994143664d51a",
        ADR_007: "55d809bd3439541515631804c34d4286a031349dfb964f471782a0224d251239",
        ADR_016: "7242c4de30b994bd844148d1e9d237c85f5742136589f058a7dfb1a189501576",
    }
)
ADR_REFERENCES = {
    ADR_006: (r"\bP3\b", r"\bP6\b", r"#17\b", r"quarantine", r"connector health", r"INGEST_STATE"),
    ADR_007: (r"\bP3\b", r"redrive", r"\bD16\b", r"Backlog", r"\bG15\b"),
    ADR_016: (r"\bP3\b", r"Idempotency-Key", r"(?i)entitlement", r"#20\b", r"DLQ redrive"),
}


@pytest.mark.parametrize("adr", list(ADR_REFERENCES), ids=lambda p: p.name[:7])
def test_adr_implementation_gate_ends_with_the_p3_phase_split(adr: Path) -> None:
    text = adr.read_text(encoding="utf-8")
    assert re.findall(r"^## (.+?)\s*$", text, flags=re.MULTILINE) == HEADINGS
    assert text.count("**Phase split (P3).**") == 1
    paragraphs = [
        p.strip()
        for p in markdown_section(text, "Implementation gate").strip().split("\n\n")
        if p.strip()
    ]
    assert paragraphs[-1].startswith("**Phase split (P3).**")
    before = "\n\n".join(paragraphs[:-1])
    assert hashlib.sha256(before.encode("utf-8")).hexdigest() == ADR_GATE_SHA256[adr]
    if adr in (ADR_006, ADR_007):
        assert paragraphs[-2].startswith("**Phase split (P2).**")
    phase_split = " ".join(paragraphs[-1].split())
    for reference in ADR_REFERENCES[adr]:
        assert re.search(reference, phase_split), reference
    assert "](../p3-ingestion-rollout-runbook.md)" in phase_split


def test_adr_016_records_the_entitlement_deferral_and_the_pre_enqueue_hook() -> None:
    paragraphs = markdown_section(ADR_016.read_text(encoding="utf-8"), "Implementation gate")
    phase_split = " ".join(paragraphs.strip().split("\n\n")[-1].split())
    assert re.search(r"entitlement enforcement[^.]*\bP4\b[^.]*#20", phase_split, re.IGNORECASE)
    assert re.search(r"\bP3\b[^.]*\bships\b[^.]*pre-enqueue[^.]*hook", phase_split, re.IGNORECASE)
    assert not re.search(r"(?:\bnot|n't|\bnever) ship", phase_split, re.IGNORECASE)  # #116


def test_adr_016_phase_split_avoids_the_forbidden_phrases() -> None:
    from tests.unit.test_architecture_inventory import FORBIDDEN_ADR_GATE_PATTERNS

    text = ADR_016.read_text(encoding="utf-8")
    for pattern in FORBIDDEN_ADR_GATE_PATTERNS[ADR_016.name]:
        assert not re.search(pattern, text, re.IGNORECASE), pattern
