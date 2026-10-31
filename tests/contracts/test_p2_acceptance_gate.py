"""P2 acceptance gate (P2-09): the Epic #22 exit invariants that need no InfluxDB, plus the
rollout runbook, the ADR phase-split paragraphs and the README link.

The real-InfluxDB invariants (gates 1, 2, 4, 6, 7, 8, 9 and the runbook's keyed divergence
checks) live in `tests/influx/test_p2_acceptance_influx.py`, which reuses the helpers below.
Every test here drives the code merged by P2-00 … P2-08; nothing is re-implemented.

Gate map (plan Task 10): 3 trusted tenant, 5 Deployment at event time, 6 unit conversion and
persisted versions (encoded line), 8 out-of-range (encoded line), 10 flag matrix, 11 runbook.

Machine-readable runbook contract (`docs/p2-telemetry-rollout-runbook.md`):
- `## ` sections in this order: `RUNBOOK_SECTIONS`; topic sections are `- ` lists, and each
  topic's patterns must match inside one top-level item.
- `## Flags`: a table whose rows start with `` `ENV` | `default` ``.
- `## Release gate`: exactly one `bash` block with exactly `RELEASE_GATE_COMMANDS` (one command
  per line, `#` comments allowed), and a `### ADR gate map` table whose first column is each
  `ADR_REQUIREMENTS` label and whose proofs are test ids written as `` `path::name` `` or
  `DEFERRED → #15|#17` with a reason.
- `## Keyed divergence checks`: three `flux` blocks whose first line is
  `// divergence-check: points|latest|summary`, using the `DIVERGENCE_PLACEHOLDERS`. Each
  returns only divergent rows: `_time`, `device_id`, `field` (points, legacy field names plus
  `seq`); `device_id`, `field` (latest); `field` (summary: `temp_c`, `ph`, `do_mg_l`).
"""

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count, product
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from limnopulse_api.adapters.influxdb_v2_writer import InfluxObservationWriter
from limnopulse_api.api.openapi_contract import build_v1_openapi_contract, iter_effective_api_routes
from limnopulse_api.core.config import Settings
from limnopulse_api.domain.deployments import Deployment, DeploymentStatus
from limnopulse_api.domain.entities import Device, Pond, Tenant
from limnopulse_api.domain.ids import (
    default_component_id_for_legacy_device,
    deployment_id_for_legacy_device,
    new_deployment_id,
)
from limnopulse_api.domain.legacy_projection import (
    asset_and_profile_for_legacy_pond,
    component_for_legacy_device,
    default_site_for_tenant,
    deployment_for_legacy_device,
    device_v2_from_legacy,
)
from limnopulse_api.domain.legacy_telemetry_mapping import map_legacy_reading
from limnopulse_api.domain.telemetry_v2 import (
    METRIC_CATALOG_VERSION,
    QUALITY_POLICY_VERSION,
    UNIT_MAPPING_VERSION,
    QuarantineReason,
    SourceObservation,
    SourceType,
    TrustedSourceContext,
)
from limnopulse_api.domain.telemetry_v2_schema import LEGACY_MEASUREMENT, OBSERVATION_MEASUREMENT
from limnopulse_api.domain.v2_common import IdempotencyReceipt, new_idempotency_receipt
from limnopulse_api.main import create_app
from limnopulse_api.services.telemetry_normalizer import TelemetryNormalizer
from limnopulse_api.services.telemetry_pipeline import PipelineOutcome, TelemetryPipeline
from limnopulse_api.services.telemetry_v2_writes import (
    ObservationWriteService,
    TelemetryWriteOutcome,
)
from scripts.dev import ingest_reading_v2
from tests.contracts.m1_fakes import InMemoryM1Repositories

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests/contracts/openapi/v1.json"
RUNBOOK = ROOT / "docs/p2-telemetry-rollout-runbook.md"
RUNBOOK_LINK = "docs/p2-telemetry-rollout-runbook.md"
README = ROOT / "README.md"
ADR_006 = ROOT / "docs/adr/ADR-006-telemetry-has-three-timestamps.md"
ADR_007 = ROOT / "docs/adr/ADR-007-influx-v2-dual-write-migration.md"
MAKEFILE = ROOT / "Makefile"
CI_WORKFLOW = ROOT / ".github/workflows/verify.yml"
EXAMPLE_READING = ROOT / "examples/telemetry/reading.local.json"
GATE_FILES = (
    "tests/contracts/test_p2_acceptance_gate.py",
    "tests/influx/test_p2_acceptance_influx.py",
)

TENANT = "tnt_gate_a"
OTHER_TENANT = "tnt_gate_b"
DEVICE = "gate-device-001"
WEST = "pond_west"
EAST = "pond_east"
CREATED = datetime(2026, 1, 1, tzinfo=UTC)
# The Device's first Deployment (to WEST) starts at CREATED; it moves to EAST at MOVED_AT.
MOVED_AT = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)
NOW = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
COMPONENT = default_component_id_for_legacy_device(TENANT, DEVICE)

# The five P2 switches: env name -> (Settings field or None for the Go evaluator, default).
SWITCHES: Mapping[str, tuple[str | None, str]] = MappingProxyType(
    {
        "TELEMETRY_V2_WRITE_ENABLED": ("telemetry_v2_write_enabled", "false"),
        "TELEMETRY_LEGACY_DUAL_WRITE_ENABLED": ("telemetry_legacy_dual_write_enabled", "false"),
        "API_V2_TELEMETRY_ENABLED": ("api_v2_telemetry_enabled", "false"),
        "V1_TELEMETRY_READ_SOURCE": ("v1_telemetry_read_source", "legacy"),
        "ALERT_EVALUATOR_TELEMETRY_SOURCE": (None, "legacy"),
    }
)
FLAG_ENVIRONMENT = (
    "API_V2_ENABLED",
    "V1_POND_DEVICE_PROJECTION_ENABLED",
    *(name for name, (setting, _default) in SWITCHES.items() if setting is not None),
)
M1_V2_PATHS = frozenset(
    {
        "/v2/tenants/{tenant_id}/sites",
        "/v2/tenants/{tenant_id}/sites/{site_id}",
        "/v2/tenants/{tenant_id}/assets",
        "/v2/tenants/{tenant_id}/assets/{asset_id}",
        "/v2/tenants/{tenant_id}/devices",
        "/v2/tenants/{tenant_id}/devices/{device_id}",
        "/v2/tenants/{tenant_id}/devices/{device_id}/components",
        "/v2/tenants/{tenant_id}/devices/{device_id}/integrations",
        "/v2/tenants/{tenant_id}/components/{component_id}",
        "/v2/tenants/{tenant_id}/components/{component_id}/capabilities",
        "/v2/tenants/{tenant_id}/integration-accounts",
        "/v2/tenants/{tenant_id}/deployments",
        "/v2/tenants/{tenant_id}/deployments/{deployment_id}/end",
    }
)
TELEMETRY_OPERATIONS = frozenset(
    {
        ("/v2/tenants/{tenant_id}/metric-definitions", "get"),
        ("/v2/tenants/{tenant_id}/observations", "get"),
        ("/v2/tenants/{tenant_id}/observations/latest", "get"),
    }
)
V1_TENANT_ROUTE_COUNT = 28
M1_TENANT_ROUTE_COUNT = 53
TELEMETRY_TENANT_ROUTE_COUNT = 56

_receipts = count()


# --- shared helpers (also used by tests/influx/test_p2_acceptance_influx.py) -------------------


def receipt(tenant_id: str) -> IdempotencyReceipt:
    digest = f"{next(_receipts):064x}"
    return new_idempotency_receipt(
        tenant_id=tenant_id,
        scope="p2-09/gate",
        key_hash=digest,
        request_hash=digest,
        response_snapshot_json="{}",
        created_at=CREATED,
    )


async def seed_legacy_device(
    repos: InMemoryM1Repositories,
    *,
    tenant_id: str,
    device_id: str,
    pond_id: str,
    created_at: datetime = CREATED,
    other_ponds: Sequence[str] = (),
) -> dict[str, str]:
    """Seed a migrated tenant: default Site, pond Assets, the legacy Device, its default Component
    and an open Deployment to `pond_id` from `created_at`. Returns pond -> asset ID."""
    tenant = Tenant(
        tenant_id=tenant_id, name=tenant_id, created_at=CREATED, updated_at=CREATED, version=1
    )
    site = default_site_for_tenant(tenant)
    if (tenant_id, site.site_id) not in repos.sites:
        await repos.create_site(site, receipt(tenant_id))
    assets: dict[str, str] = {}
    for pond in (pond_id, *other_ponds):
        asset = asset_and_profile_for_legacy_pond(
            Pond(
                tenant_id=tenant_id,
                pond_id=pond,
                name=pond,
                created_at=CREATED,
                updated_at=CREATED,
                version=1,
            ),
            site.site_id,
        )
        if (tenant_id, asset.asset.asset_id) not in repos.assets:
            await repos.create_asset(asset.asset, asset.pond_profile, receipt(tenant_id))
        assets[pond] = asset.asset.asset_id
    device = Device(
        tenant_id=tenant_id,
        pond_id=pond_id,
        device_id=device_id,
        name=device_id,
        created_at=created_at,
        updated_at=created_at,
        version=1,
    )
    await repos.create_device(device_v2_from_legacy(device), receipt(tenant_id))
    await repos.create_component(component_for_legacy_device(device), receipt(tenant_id))
    await repos.start_deployment(
        deployment_for_legacy_device(device, assets[pond_id], site.site_id), receipt(tenant_id)
    )
    return assets


async def relocate(
    repos: InMemoryM1Repositories,
    *,
    tenant_id: str,
    device_id: str,
    to_asset_id: str,
    ended_at: datetime,
    started_at: datetime,
) -> Deployment:
    """End the Component's current Deployment at `ended_at`; start a new one at `started_at`."""
    component_id = default_component_id_for_legacy_device(tenant_id, device_id)
    current = await repos.get_current_deployment(tenant_id, component_id)
    assert current is not None
    await repos.end_deployment(
        tenant_id, current.deployment_id, expected_version=current.version, ended_at=ended_at
    )
    return await repos.start_deployment(
        Deployment(
            tenant_id=tenant_id,
            deployment_id=new_deployment_id(),
            component_id=component_id,
            site_id=current.site_id,
            asset_id=to_asset_id,
            started_at=started_at,
            status=DeploymentStatus.ACTIVE,
            created_at=started_at,
            updated_at=started_at,
            version=1,
        ),
        receipt(tenant_id),
    )


class RecordingWriteApi:
    """A synchronous `WriteApi` stand-in that keeps every line the production writer sends."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.calls = 0

    def write(self, *, bucket: str, org: str, record: list[str], write_precision: Any) -> None:
        self.calls += 1
        self.lines.extend(record)


def legacy_context(
    tenant_id: str = TENANT,
    device_id: str = DEVICE,
    *,
    received_at: datetime = NOW,
    replay: bool = False,
    source_type: SourceType = SourceType.LEGACY_MQTT,
    integration_id: str | None = None,
) -> TrustedSourceContext:
    return TrustedSourceContext(
        tenant_id=tenant_id,
        device_id=device_id,
        integration_id=integration_id,
        integration_type="legacy_mqtt",
        source_type=source_type,
        connector_version=None,
        received_at=received_at,
        replay=replay,
    )


def build_pipeline(
    repos: InMemoryM1Repositories,
    write_api: Any,
    *,
    clock: Any = lambda: NOW,
    v2_write_enabled: bool = True,
    legacy_dual_write_enabled: bool = True,
    bucket: str = "raw",
    org: str = "org",
) -> TelemetryPipeline:
    """The production pipeline (mapper, normalizer, write service, Influx writer) over fakes."""
    writer = InfluxObservationWriter(write_api=write_api, bucket=bucket, org=org)
    return TelemetryPipeline(
        normalizer=TelemetryNormalizer(components=repos, deployments=repos, clock=clock),
        write_service=ObservationWriteService(
            writer=writer,
            legacy_writer=writer,
            site_assets=repos,
            v2_write_enabled=v2_write_enabled,
            legacy_dual_write_enabled=legacy_dual_write_enabled,
        ),
    )


def example_payload(observed_at: datetime | str, **overrides: Any) -> dict[str, Any]:
    payload = json.loads(EXAMPLE_READING.read_text(encoding="utf-8"))
    if isinstance(observed_at, datetime):
        observed_at = observed_at.isoformat().replace("+00:00", "Z")
    payload["ts"] = observed_at
    payload.update(overrides)
    return payload


@dataclass(frozen=True)
class Line:
    measurement: str
    tags: Mapping[str, str]
    fields: Mapping[str, str]
    time_ns: int


def _split_unescaped(text: str, separator: str) -> list[str]:
    parts, current, quoted, escaped = [], [], False, False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            current.append(char)
            escaped = True
        elif char == '"':
            current.append(char)
            quoted = not quoted
        elif char == separator and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def parse_line(line: str) -> Line:
    """Parse one line-protocol line; field values keep their raw encoding (`"s"`, `1i`, `2.5`)."""
    head, fields, timestamp = _split_unescaped(line, " ")
    measurement, *tag_pairs = _split_unescaped(head, ",")
    tags = dict(pair.split("=", 1) for pair in tag_pairs)
    field_map = dict(pair.split("=", 1) for pair in _split_unescaped(fields, ","))
    return Line(measurement=measurement, tags=tags, fields=field_map, time_ns=int(timestamp))


def string_field(value: str) -> str:
    assert value.startswith('"') and value.endswith('"'), value
    return value[1:-1].replace('\\"', '"').replace("\\\\", "\\")


def v2_lines(lines: Sequence[str]) -> list[Line]:
    return [p for p in map(parse_line, lines) if p.measurement == OBSERVATION_MEASUREMENT]


def legacy_lines(lines: Sequence[str]) -> list[Line]:
    return [p for p in map(parse_line, lines) if p.measurement == LEGACY_MEASUREMENT]


def runbook_text() -> str:
    assert RUNBOOK.is_file(), f"missing {RUNBOOK.relative_to(ROOT)}"
    return RUNBOOK.read_text(encoding="utf-8")


def markdown_section(text: str, heading: str, level: int = 2) -> str:
    """The body under `<#*level> heading`, up to the next heading of the same or a higher level."""
    marks = "#" * level
    match = re.search(rf"^{marks} {re.escape(heading)}[ \t]*$", text, flags=re.MULTILINE)
    assert match is not None, f"missing heading {marks} {heading}"
    rest = text[match.end() :]
    following = re.search(rf"^#{{1,{level}}} ", rest, flags=re.MULTILINE)
    return rest[: following.start()] if following else rest


def fenced_blocks(text: str, language: str) -> list[str]:
    return re.findall(rf"^```{language}[ \t]*\n(.*?)^```[ \t]*$", text, re.MULTILINE | re.DOTALL)


DIVERGENCE_CHECKS = ("points", "latest", "summary")
DIVERGENCE_PLACEHOLDERS = (
    "<bucket>",
    "<tenant_id>",
    "<pond_id>",
    "<asset_id>",
    "<component_ids>",
    "<start>",
    "<stop>",
)


def divergence_checks() -> dict[str, str]:
    """The runbook's keyed divergence checks: `flux` blocks whose first line is
    `// divergence-check: <name>`."""
    section = markdown_section(runbook_text(), "Keyed divergence checks")
    checks: dict[str, str] = {}
    for block in fenced_blocks(section, "flux"):
        first = block.splitlines()[0].strip() if block.strip() else ""
        match = re.fullmatch(r"// divergence-check: (\w+)", first)
        if match:
            assert match.group(1) not in checks, f"duplicate check {match.group(1)}"
            checks[match.group(1)] = block
    return checks


def fill_divergence_check(
    query: str,
    *,
    bucket: str,
    tenant_id: str,
    pond_id: str,
    asset_id: str,
    component_ids: Sequence[str],
    start: datetime,
    stop: datetime,
) -> str:
    values = {
        "<bucket>": bucket,
        "<tenant_id>": tenant_id,
        "<pond_id>": pond_id,
        "<asset_id>": asset_id,
        "<component_ids>": ", ".join(json.dumps(c) for c in component_ids),
        "<start>": start.isoformat().replace("+00:00", "Z"),
        "<stop>": stop.isoformat().replace("+00:00", "Z"),
    }
    for placeholder, value in values.items():
        query = query.replace(placeholder, value)
    leftover = re.findall(r"<[a-z_]+>", query)
    assert not leftover, f"unknown placeholders {leftover}"
    return query


# --- Gate 3: no device-supplied tenant authority -----------------------------------------------

OWNERSHIP_CLAIMS = ("tenant_id", "site_id", "asset_id", "deployment_id", "device_id", "pond_id")


@pytest.mark.parametrize("claim", OWNERSHIP_CLAIMS)
def test_gate3_source_observation_rejects_every_ownership_field(claim: str) -> None:
    valid = {
        "component_id": COMPONENT,
        "metric_key": "water.temperature",
        "value": 25.0,
        "source_unit": "degC",
        "observed_at": NOW,
    }
    SourceObservation.model_validate(valid)
    assert claim not in SourceObservation.model_fields

    with pytest.raises(ValidationError):
        SourceObservation.model_validate({**valid, claim: "tnt_attacker"})


@pytest.mark.asyncio
async def test_gate3_payload_ownership_claims_never_reach_stored_points() -> None:
    repos = InMemoryM1Repositories(clock=lambda: NOW)
    assets = await seed_legacy_device(repos, tenant_id=TENANT, device_id=DEVICE, pond_id=WEST)
    # The claimed tenant exists and owns a Device and Pond of the claimed IDs, so a mapper that
    # honored the payload would write there instead of quarantining.
    other_assets = await seed_legacy_device(
        repos, tenant_id=OTHER_TENANT, device_id="claimed-device", pond_id=EAST
    )
    write_api = RecordingWriteApi()
    payload = example_payload(
        MOVED_AT,
        tenant_id=OTHER_TENANT,
        pond_id=EAST,
        device_id="claimed-device",
        site_id="site_claimed",
        asset_id=other_assets[EAST],
        deployment_id=deployment_id_for_legacy_device(OTHER_TENANT, "claimed-device"),
        component_id=default_component_id_for_legacy_device(OTHER_TENANT, "claimed-device"),
    )

    outcome = await build_pipeline(repos, write_api).ingest_legacy_reading(
        legacy_context(), payload
    )

    assert outcome.quarantined == ()
    stored = v2_lines(write_api.lines)
    assert len(stored) == 7
    for point in stored:
        assert point.tags["tenant_id"] == TENANT
        assert point.tags["device_id"] == DEVICE
        assert point.tags["component_id"] == COMPONENT
        assert point.tags["asset_id"] == assets[WEST]
        assert point.tags["deployment_id"] == deployment_id_for_legacy_device(TENANT, DEVICE)
    [legacy] = legacy_lines(write_api.lines)
    assert (legacy.tags["tenant_id"], legacy.tags["pond_id"], legacy.tags["device_id"]) == (
        TENANT,
        WEST,
        DEVICE,
    )
    assert OTHER_TENANT not in "\n".join(write_api.lines)
    assert "claimed-device" not in "\n".join(write_api.lines)


def test_gate3_mapper_derives_the_component_from_the_trusted_context_only() -> None:
    payload = example_payload(MOVED_AT, tenant_id=OTHER_TENANT, device_id="claimed-device")

    mapped = map_legacy_reading(legacy_context(), payload)

    assert mapped.quarantined == ()
    assert {o.component_id for o in mapped.observations} == {COMPONENT}
    assert len(mapped.observations) == 7


def test_gate3_dev_cli_takes_the_tenant_only_from_its_arguments(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    for name in FLAG_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    reading = tmp_path / "reading.json"
    reading.write_text(
        json.dumps(example_payload(MOVED_AT, tenant_id=OTHER_TENANT, device_id="claimed")),
        encoding="utf-8",
    )
    calls: list[tuple[TrustedSourceContext, Mapping[str, Any]]] = []

    async def fake_ingest(settings, context, payload):
        calls.append((context, payload))
        return PipelineOutcome(
            written=TelemetryWriteOutcome(v2_points=7, legacy_points=1, legacy_excluded=0),
            quarantined=(),
            counts=MappingProxyType({"accepted": 7}),
        )

    monkeypatch.setattr(ingest_reading_v2, "_ingest", fake_ingest)

    code = ingest_reading_v2.main(
        ["--tenant", TENANT, "--device", DEVICE, "--file", "reading.json"]
    )

    assert code == 0
    [(context, payload)] = calls
    assert (context.tenant_id, context.device_id) == (TENANT, DEVICE)
    mapped = map_legacy_reading(context, payload)
    assert {o.component_id for o in mapped.observations} == {COMPONENT}


# --- Gate 5: Deployment at event time, relocation, replay; no fallback --------------------------


@dataclass
class Relocated:
    repos: InMemoryM1Repositories
    assets: dict[str, str]
    west_deployment: str
    east_deployment: str
    write_api: RecordingWriteApi = field(default_factory=RecordingWriteApi)

    def pipeline(self) -> TelemetryPipeline:
        return build_pipeline(self.repos, self.write_api)


async def relocated(*, gap: timedelta = timedelta(0)) -> Relocated:
    """DEVICE was on WEST from CREATED until MOVED_AT, then on EAST from MOVED_AT + gap."""
    repos = InMemoryM1Repositories(clock=lambda: NOW)
    assets = await seed_legacy_device(
        repos, tenant_id=TENANT, device_id=DEVICE, pond_id=WEST, other_ponds=(EAST,)
    )
    east = await relocate(
        repos,
        tenant_id=TENANT,
        device_id=DEVICE,
        to_asset_id=assets[EAST],
        ended_at=MOVED_AT,
        started_at=MOVED_AT + gap,
    )
    return Relocated(
        repos=repos,
        assets=assets,
        west_deployment=deployment_id_for_legacy_device(TENANT, DEVICE),
        east_deployment=east.deployment_id,
    )


def _owners(lines: Sequence[str]) -> set[tuple[str, str]]:
    return {(p.tags["asset_id"], p.tags["deployment_id"]) for p in v2_lines(lines)}


@pytest.mark.asyncio
async def test_gate5_each_reading_resolves_the_deployment_of_its_event_time() -> None:
    state = await relocated()
    pipeline = state.pipeline()

    before = await pipeline.ingest_legacy_reading(
        legacy_context(), example_payload(MOVED_AT - timedelta(hours=1))
    )
    before_lines = list(state.write_api.lines)
    at_move = await pipeline.ingest_legacy_reading(legacy_context(), example_payload(MOVED_AT))
    at_move_lines = state.write_api.lines[len(before_lines) :]

    assert before.quarantined == () and at_move.quarantined == ()
    assert _owners(before_lines) == {(state.assets[WEST], state.west_deployment)}
    assert [p.tags["pond_id"] for p in legacy_lines(before_lines)] == [WEST]
    # The interval is half-open: the move instant belongs to the new Deployment.
    assert _owners(at_move_lines) == {(state.assets[EAST], state.east_deployment)}
    assert [p.tags["pond_id"] for p in legacy_lines(at_move_lines)] == [EAST]


@pytest.mark.asyncio
async def test_gate5_replay_after_the_move_keeps_the_original_deployment() -> None:
    state = await relocated()
    observed_at = MOVED_AT - timedelta(minutes=30)
    first_receipt = observed_at + timedelta(seconds=2)
    # A replay carries the first-receipt context (D5): same received_at, replay=True.
    context = legacy_context(received_at=first_receipt, replay=True)

    outcome = await state.pipeline().ingest_legacy_reading(context, example_payload(observed_at))

    assert outcome.quarantined == ()
    stored = v2_lines(state.write_api.lines)
    assert len(stored) == 7
    assert _owners(state.write_api.lines) == {(state.assets[WEST], state.west_deployment)}
    assert all(
        json.loads(string_field(p.fields["quality_flags_json"])) == ["replay"] for p in stored
    )
    assert [p.tags["pond_id"] for p in legacy_lines(state.write_api.lines)] == [WEST]


@pytest.mark.asyncio
async def test_gate5_unresolved_deployment_is_quarantined_without_fallback() -> None:
    # A one-hour gap between the two Deployments; EAST is the current Deployment.
    state = await relocated(gap=timedelta(hours=1))
    in_gap = MOVED_AT + timedelta(minutes=30)
    before_first = CREATED - timedelta(days=1)
    observations = [
        SourceObservation(
            component_id=COMPONENT,
            metric_key="water.temperature",
            value=25.0,
            source_unit="degC",
            observed_at=observed_at,
            sequence=index,
        )
        for index, observed_at in enumerate((in_gap, before_first))
    ]

    outcome = await state.pipeline().ingest(legacy_context(), observations)

    assert [q.reason for q in outcome.quarantined] == [QuarantineReason.DEPLOYMENT_UNRESOLVED] * 2
    assert [q.observed_at for q in outcome.quarantined] == [in_gap, before_first]
    assert all(q.tenant_id == TENANT and q.device_id == DEVICE for q in outcome.quarantined)
    assert outcome.written == TelemetryWriteOutcome(v2_points=0, legacy_points=0, legacy_excluded=0)
    assert state.write_api.lines == []
    assert outcome.counts["quarantined.deployment_unresolved"] == 2


# --- Gate 6 and gate 8 on the encoded points ----------------------------------------------------


async def _ingest_sources(sources: Sequence[dict[str, Any]]) -> tuple[PipelineOutcome, list[str]]:
    repos = InMemoryM1Repositories(clock=lambda: NOW)
    await seed_legacy_device(repos, tenant_id=TENANT, device_id=DEVICE, pond_id=WEST)
    write_api = RecordingWriteApi()
    observations = [
        SourceObservation(component_id=COMPONENT, observed_at=MOVED_AT, sequence=3, **source)
        for source in sources
    ]
    outcome = await build_pipeline(repos, write_api).ingest(legacy_context(), observations)
    return outcome, write_api.lines


@pytest.mark.asyncio
async def test_gate6_unit_conversion_is_applied_and_versions_are_persisted() -> None:
    outcome, lines = await _ingest_sources(
        [
            {"metric_key": "water.temperature", "value": 77.0, "source_unit": "degF"},
            {"metric_key": "water.dissolved_oxygen", "value": 6400, "source_unit": "ug/L"},
            {"metric_key": "device.battery_voltage", "value": 3820, "source_unit": "mV"},
        ]
    )

    assert outcome.quarantined == ()
    by_metric = {p.tags["metric_key"]: p for p in v2_lines(lines)}
    assert set(by_metric) == {
        "water.temperature",
        "water.dissolved_oxygen",
        "device.battery_voltage",
    }
    expected = {
        "water.temperature": (25.0, "degF"),
        "water.dissolved_oxygen": (6.4, "ug/L"),
        "device.battery_voltage": (3.82, "mV"),
    }
    for metric_key, (value, source_unit) in expected.items():
        point = by_metric[metric_key]
        assert float(point.fields["value"]) == pytest.approx(value, rel=1e-12, abs=1e-12)
        assert string_field(point.fields["source_unit"]) == source_unit
        assert string_field(point.fields["catalog_version"]) == METRIC_CATALOG_VERSION
        assert string_field(point.fields["unit_mapping_version"]) == UNIT_MAPPING_VERSION
        assert string_field(point.fields["quality_policy_version"]) == QUALITY_POLICY_VERSION
        assert "source_unit" not in point.tags
    [legacy] = legacy_lines(lines)
    assert float(legacy.fields["temp_c"]) == pytest.approx(25.0, rel=1e-12, abs=1e-12)
    assert float(legacy.fields["do_mg_l"]) == pytest.approx(6.4, rel=1e-12, abs=1e-12)


@pytest.mark.asyncio
async def test_gate8_out_of_range_is_stored_with_its_state_and_provenance() -> None:
    outcome, lines = await _ingest_sources(
        [
            {"metric_key": "water.dissolved_oxygen", "value": 75.0, "source_unit": "mg/L"},
            {"metric_key": "water.ph", "value": 7.2, "source_unit": "pH"},
        ]
    )

    assert outcome.quarantined == ()
    assert outcome.counts["state.out_of_range"] == 1
    by_metric = {p.tags["metric_key"]: p for p in v2_lines(lines)}
    dissolved_oxygen = by_metric["water.dissolved_oxygen"]
    assert float(dissolved_oxygen.fields["value"]) == 75.0
    assert string_field(dissolved_oxygen.fields["quality_state"]) == "out_of_range"
    assert "plausible_range" in json.loads(
        string_field(dissolved_oxygen.fields["quality_provenance_json"])
    )
    assert string_field(by_metric["water.ph"].fields["quality_state"]) == "valid"
    assert "plausible_range" not in json.loads(
        string_field(by_metric["water.ph"].fields["quality_provenance_json"])
    )
    # D15: the value still reaches legacy reads.
    [legacy] = legacy_lines(lines)
    assert float(legacy.fields["do_mg_l"]) == 75.0


# --- Gate 10: flag matrix --------------------------------------------------------------------


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, app_env="test", auth_mode="dev", **overrides)


def _v2_operations(app: FastAPI) -> set[tuple[str, str]]:
    paths = app.openapi()["paths"]
    return {
        (path, method)
        for path, methods in paths.items()
        if path == "/v2" or path.startswith("/v2/")
        for method in methods
    }


def _tenant_route_count(app: FastAPI) -> int:
    return sum("{tenant_id}" in r.path_format for r in iter_effective_api_routes(app))


VALID_V2_COMBINATIONS = ((False, False), (True, False), (True, True))
FLAG_MATRIX = [
    {
        "api_v2_enabled": api_v2,
        "api_v2_telemetry_enabled": telemetry,
        "telemetry_v2_write_enabled": v2_write,
        "telemetry_legacy_dual_write_enabled": legacy_write,
        "v1_telemetry_read_source": read_source,
    }
    for (api_v2, telemetry), v2_write, legacy_write, read_source in product(
        VALID_V2_COMBINATIONS, (False, True), (False, True), ("legacy", "v2")
    )
]


@pytest.fixture
def isolated_flags(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in FLAG_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_gate10_flag_matrix_covers_every_valid_combination() -> None:
    assert len(FLAG_MATRIX) == 24
    assert len({tuple(sorted(flags.items())) for flags in FLAG_MATRIX}) == 24


@pytest.mark.parametrize(
    "flags", FLAG_MATRIX, ids=lambda flags: ",".join(f"{k}={v}" for k, v in flags.items())
)
def test_gate10_every_valid_combination_keeps_the_v1_golden_and_exact_routes(
    isolated_flags: pytest.MonkeyPatch, flags: dict[str, object]
) -> None:
    app = create_app(_settings(**flags))

    assert build_v1_openapi_contract(app) == json.loads(GOLDEN.read_text(encoding="utf-8"))
    operations = _v2_operations(app)
    if not flags["api_v2_enabled"]:
        assert operations == set()
        assert _tenant_route_count(app) == V1_TENANT_ROUTE_COUNT
    elif not flags["api_v2_telemetry_enabled"]:
        assert {path for path, _method in operations} == M1_V2_PATHS
        assert not operations & TELEMETRY_OPERATIONS
        assert _tenant_route_count(app) == M1_TENANT_ROUTE_COUNT
    else:
        assert {path for path, _method in operations} == M1_V2_PATHS | {
            path for path, _method in TELEMETRY_OPERATIONS
        }
        assert TELEMETRY_OPERATIONS <= operations
        assert _tenant_route_count(app) == TELEMETRY_TENANT_ROUTE_COUNT


@pytest.mark.parametrize(
    ("v2_write", "legacy_write", "read_source"),
    list(product((False, True), (False, True), ("legacy", "v2"))),
)
def test_gate10_telemetry_routes_without_api_v2_are_a_validation_error(
    isolated_flags: pytest.MonkeyPatch, v2_write: bool, legacy_write: bool, read_source: str
) -> None:
    with pytest.raises(ValidationError) as excinfo:
        _settings(
            api_v2_enabled=False,
            api_v2_telemetry_enabled=True,
            telemetry_v2_write_enabled=v2_write,
            telemetry_legacy_dual_write_enabled=legacy_write,
            v1_telemetry_read_source=read_source,
        )

    assert "API_V2_TELEMETRY_ENABLED requires API_V2_ENABLED" in str(excinfo.value)


def test_gate10_switch_env_names_and_defaults(isolated_flags: pytest.MonkeyPatch) -> None:
    defaults = _settings()
    for name, (setting, default) in SWITCHES.items():
        if setting is None:
            continue
        assert str(getattr(defaults, setting)).lower() == default, name
        assert str(Settings.model_fields[setting].default).lower() == default, name

    isolated_flags.setenv("API_V2_ENABLED", "true")
    isolated_flags.setenv("API_V2_TELEMETRY_ENABLED", "true")
    isolated_flags.setenv("TELEMETRY_V2_WRITE_ENABLED", "true")
    isolated_flags.setenv("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED", "true")
    isolated_flags.setenv("V1_TELEMETRY_READ_SOURCE", "v2")
    switched = _settings()

    assert switched.api_v2_telemetry_enabled is True
    assert switched.telemetry_v2_write_enabled is True
    assert switched.telemetry_legacy_dual_write_enabled is True
    assert switched.v1_telemetry_read_source == "v2"


def test_gate10_invalid_combination_from_env_never_echoes_input(
    isolated_flags: pytest.MonkeyPatch,
) -> None:
    isolated_flags.setenv("API_V2_TELEMETRY_ENABLED", "true")
    isolated_flags.setenv("V1_TELEMETRY_READ_SOURCE", "v2-secret-looking-value")

    with pytest.raises(ValidationError) as excinfo:
        _settings()

    assert "v2-secret-looking-value" not in str(excinfo.value)


# --- Gate 11: runbook ---------------------------------------------------------------------------

RUNBOOK_SECTIONS = (
    "Flags",
    "Release gate",
    "D16 precondition",
    "Rollout sequence",
    "Keyed divergence checks",
    "Stop-dual-write criteria",
    "Rollback",
    "Retention",
    "Known divergences and limitations",
    "Out of scope",
)
# Exactly the issue's verification block: the gate and CI run these commands.
RELEASE_GATE_COMMANDS = frozenset(
    {
        (
            "uv run --locked --extra dev pytest -q tests/contracts/test_p2_acceptance_gate.py "
            "tests/unit/test_architecture_inventory.py"
        ),
        "scripts/dev/run_local_influxd.sh",
        (
            "RUN_INFLUX_INTEGRATION=1 uv run --locked --extra dev pytest -q "
            "tests/influx/test_p2_acceptance_influx.py"
        ),
        "make verify-influx",
        "make verify-python",
        "make verify-go",
        "uv run --locked --extra dev python scripts/dev/export_v1_openapi.py --check",
        "git diff --check",
    }
)
# Plan "Final Acceptance Matrix" requirements, verbatim (backticks dropped).
ADR_REQUIREMENTS = (
    "ADR-005 unit conversion",
    "ADR-005 two probes for one metric",
    "ADR-005 trusted tenant mapping",
    "ADR-005 /v1 pivot equivalence",
    "ADR-005 bounded tag cardinality",
    "ADR-005 out-of-range never valid, with provenance",
    "ADR-005 mapping versions persisted; replay after revision keeps version and identity",
    "ADR-006 delayed/duplicated/replayed/out-of-order",
    "ADR-006 clock-skew flag",
    "ADR-006 deployment at event time; no fallback",
    "ADR-006 quarantine persistence + connector health",
    "ADR-006 current health uses receive/ingest time",
    "ADR-006 event-time alert windows, completeness delay, order independence",
    (
        "ADR-007 duplicate/ordering/conversion/cardinality/evaluator parity/visibility before "
        "enabling v2 reads"
    ),
    "ADR-007 outage after durable acceptance: no ack, recovery without loss",
    "ADR-007 each write path disabled independently",
)
# Requirements the plan proves with a P2-09 gate: the row must cite that gate's test(s).
GATE_PROVEN = MappingProxyType(
    {
        "ADR-005 unit conversion": ("test_gate6_",),
        "ADR-005 two probes for one metric": ("test_gate2_",),
        "ADR-005 trusted tenant mapping": ("test_gate3_",),
        "ADR-005 /v1 pivot equivalence": ("test_gate1_",),
        "ADR-005 bounded tag cardinality": ("test_gate7_",),
        "ADR-005 out-of-range never valid, with provenance": ("test_gate8_",),
        "ADR-006 delayed/duplicated/replayed/out-of-order": ("test_gate4_",),
        "ADR-006 deployment at event time; no fallback": ("test_gate5_",),
        (
            "ADR-007 duplicate/ordering/conversion/cardinality/evaluator parity/visibility before "
            "enabling v2 reads"
        ): ("test_gate4_", "test_gate6_", "test_gate7_", "test_gate1_"),
        "ADR-007 outage after durable acceptance: no ack, recovery without loss": ("test_gate9_",),
    }
)
DEFERRED = MappingProxyType(
    {
        "ADR-006 quarantine persistence + connector health": "#15",
        "ADR-006 current health uses receive/ingest time": "#17",
        "ADR-007 outage after durable acceptance: no ack, recovery without loss": "#15",
    }
)
# Rows with no P2 test at all: only the deferral.
DEFERRED_ONLY = frozenset({"ADR-006 quarantine persistence + connector health"})
TEST_ID = re.compile(r"`((?:tests|internal|cmd)/[\w./-]+\.(?:py|go))::(\w+)`")


def _table_rows(section: str) -> list[list[str]]:
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line.startswith("|") or re.fullmatch(r"\|[\s:|-]+\|", line):
            continue
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return rows


def _test_exists(path: str, name: str) -> bool:
    source = ROOT / path
    if not source.is_file():
        return False
    text = source.read_text(encoding="utf-8")
    if path.endswith(".go"):
        return re.search(rf"^func {re.escape(name)}\(", text, re.MULTILINE) is not None
    return re.search(rf"^(?:async )?def {re.escape(name)}\(", text, re.MULTILINE) is not None


def test_gate11_runbook_has_every_section_in_order() -> None:
    text = runbook_text()
    headings = re.findall(r"^## (.+?)\s*$", text, flags=re.MULTILINE)
    positions = []
    for section in RUNBOOK_SECTIONS:
        assert section in headings, f"missing ## {section}"
        positions.append(headings.index(section))
    assert positions == sorted(positions)


def test_gate11_flags_table_lists_the_five_switches_with_their_defaults() -> None:
    rows = _table_rows(markdown_section(runbook_text(), "Flags"))
    documented = {}
    for row in rows:
        name = re.fullmatch(r"`([A-Z0-9_]+)`", row[0])
        if name:
            documented[name.group(1)] = row
    for name, (setting, default) in SWITCHES.items():
        assert name in documented, f"flags table misses {name}"
        assert documented[name][1] == f"`{default}`", (name, documented[name][1])
        if setting is not None:
            assert str(Settings.model_fields[setting].default).lower() == default
    assert "API_V2_ENABLED" in " ".join(documented["API_V2_TELEMETRY_ENABLED"][2:])


def test_gate11_release_gate_lists_exactly_the_gate_commands() -> None:
    section = markdown_section(runbook_text(), "Release gate")
    blocks = fenced_blocks(section, "bash")
    assert len(blocks) == 1, "the release gate has exactly one bash block"
    commands = [
        re.sub(r"\s+#.*$", "", line).strip()
        for line in blocks[0].splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    assert len(commands) == len(set(commands))
    assert set(commands) == RELEASE_GATE_COMMANDS


def test_gate11_release_gate_commands_are_the_ones_make_and_ci_run() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    for command in RELEASE_GATE_COMMANDS:
        if command.startswith("make "):
            target = command.removeprefix("make ")
            assert re.search(rf"^{re.escape(target)}:", makefile, re.MULTILINE), target
            assert f"run: {command}" in workflow, command
        for path in re.findall(r"(?:tests|scripts)/\S+", command):
            assert (ROOT / path).is_file(), path
    recipe = markdown_section(
        makefile.replace("verify-influx:", "## verify-influx"), "verify-influx"
    )
    # Go parity runs inside `make verify-influx`.
    assert "go test" in recipe and "TestInfluxIntegration" in recipe
    assert "pytest -q tests/influx" in recipe


def test_gate11_every_adr_gate_maps_to_a_proving_test_or_a_deferral() -> None:
    section = markdown_section(runbook_text(), "ADR gate map", level=3)
    rows = {row[0].replace("`", ""): " | ".join(row[1:]) for row in _table_rows(section)}

    for requirement in ADR_REQUIREMENTS:
        assert requirement in rows, f"ADR gate map misses: {requirement}"
        proof = rows[requirement]
        ids = TEST_ID.findall(proof)
        for path, name in ids:
            assert _test_exists(path, name), f"{requirement}: no test {path}::{name}"
        if requirement in DEFERRED_ONLY:
            assert not ids or all(_test_exists(p, n) for p, n in ids)
        else:
            assert ids, f"{requirement}: no proving test"
        for prefix in GATE_PROVEN.get(requirement, ()):
            assert any(path in GATE_FILES and name.startswith(prefix) for path, name in ids), (
                requirement,
                prefix,
            )
        if requirement in DEFERRED:
            marker = f"DEFERRED → {DEFERRED[requirement]}"
            assert marker in proof, (requirement, proof)
            reason = TEST_ID.sub("", proof).replace(marker, "")
            assert len(re.sub(r"[\W_]+", "", reason)) >= 20, f"{requirement}: no reason"


def test_gate11_d16_precondition_is_stated() -> None:
    section = markdown_section(runbook_text(), "D16 precondition")
    normalized = " ".join(section.split())

    assert "rehearsal" in normalized.lower()
    assert "P3" in normalized
    assert "ALERT_EVALUATOR_TELEMETRY_SOURCE=v2" in normalized
    assert "V1_TELEMETRY_READ_SOURCE=v2" in normalized
    assert "(pond, device, metric, _time, value)" in normalized
    assert "max(longest rule window, legacy read window)" in normalized


def test_gate11_rollout_sequence_is_ordered() -> None:
    section = markdown_section(runbook_text(), "Rollout sequence")
    items = [
        " ".join(item.split()) for item in re.split(r"^\d+\. ", section, flags=re.MULTILINE)[1:]
    ]
    steps = (
        ("TELEMETRY_V2_WRITE_ENABLED=true",),
        ("keyed divergence",),
        ("TELEMETRY_LEGACY_DUAL_WRITE_ENABLED=true", "P3", "redrive"),
        ("API_V2_TELEMETRY_ENABLED=true",),
        ("ALERT_EVALUATOR_TELEMETRY_SOURCE=v2", "one environment"),
        ("V1_TELEMETRY_READ_SOURCE=v2", "retention"),
    )
    found = []
    for markers in steps:
        index = next(
            (i for i, item in enumerate(items) if all(m.lower() in item.lower() for m in markers)),
            None,
        )
        assert index is not None, f"no rollout step with {markers}"
        found.append(index)
    assert found == sorted(found) and len(set(found)) == len(found), found


def test_gate11_keyed_divergence_checks_are_keyed_flux_queries() -> None:
    section = " ".join(markdown_section(runbook_text(), "Keyed divergence checks").split())
    checks = divergence_checks()

    assert set(checks) == set(DIVERGENCE_CHECKS)
    for name, query in checks.items():
        for placeholder in DIVERGENCE_PLACEHOLDERS:
            assert placeholder in query, (name, placeholder)
        assert "water_quality" in query and "observation_numeric" in query, name
        assert "${" not in query, name
    assert "_time" in checks["points"] and "seq" in checks["points"]
    assert "counts alone" in section.lower()
    assert "1e-12" in section


def test_gate11_rollback_uses_flags_only_and_never_deletes_points() -> None:
    text = runbook_text()
    section = " ".join(markdown_section(text, "Rollback").split())

    assert re.search(r"never delete", section, re.IGNORECASE)
    assert re.search(r"restart|redeploy", section, re.IGNORECASE)
    for name, (_setting, default) in SWITCHES.items():
        assert f"{name}={default}" in section, name
    for block in re.findall(r"```.*?```", text, re.DOTALL):
        assert "influx delete" not in block and "/api/v2/delete" not in block


def test_gate11_stop_dual_write_and_retention_are_documented() -> None:
    text = runbook_text()
    stop = " ".join(markdown_section(text, "Stop-dual-write criteria").split())
    retention = " ".join(markdown_section(text, "Retention").split())

    assert "§10.6" in stop
    for criterion in (
        r"divergence",
        r"retention window",
        r"read window",
        r"V1_TELEMETRY_READ_SOURCE=v2",
    ):
        assert re.search(criterion, stop), criterion
    assert "D6" in retention
    assert "INFLUXDB_BUCKET_RAW" in retention
    assert "legacy read window" in retention
    assert "observation_numeric" in retention


def list_items(section: str) -> list[str]:
    """Top-level `- ` list items of a section (nested lines stay with their item), whitespace
    collapsed."""
    items = re.split(r"^- ", section, flags=re.MULTILINE)[1:]
    return [" ".join(item.split()) for item in items]


def assert_topic_in_one_item(section: str, topic: str, patterns: tuple[str, ...]) -> None:
    items = list_items(section)
    assert items, "the section must be a list"
    assert any(all(re.search(p, item) for p in patterns) for item in items), (topic, patterns)


@pytest.mark.parametrize(
    ("topic", "patterns"),
    [
        ("D15 out-of-range", (r"D15", r"out_of_range", r"quality-gated", r"alert")),
        ("D10 present-to-absent seq", (r"D10", r"present", r"absent", r"`?seq`?")),
        ("backdated Deployment end", (r"backdated", r"two v2 series")),
        ("seq winner order", (r"ingested_at",)),
        ("domain devices and ponds", (r"deleted|no longer listed", r"list_devices|domain")),
        ("summary relaxation", (r"1e-12", r"completed windows")),
        ("Go reader escaping", (r"#71", r"escap")),
        ("CI image pin", (r"sha256:d92a10e9", r"2\.7\.11", r"influxdb:2-alpine")),
        (
            "writer error classes",
            (
                r"misconfigur",
                r"status 0",
                r"429",
                r"5xx",
                r"422",
                r"4xx",
                r"over-limit",
                r"unclassified",
                r"TelemetryWriteError",
                r"never acknowledge",
            ),
        ),
    ],
)
def test_gate11_known_divergences_and_limitations(topic: str, patterns: tuple[str, ...]) -> None:
    section = markdown_section(runbook_text(), "Known divergences and limitations")
    assert_topic_in_one_item(section, topic, patterns)


@pytest.mark.parametrize(
    ("topic", "patterns"),
    [
        (
            "D13 phase split",
            (
                r"D13",
                r"#15\b",
                r"quarantine",
                r"connector[- ]health",
                r"last-seen",
                r"reconciliation",
                r"sequence gap",
                r"redrive",
                r"§23\.2",
            ),
        ),
        ("D17 AlertRuleV2", (r"D17", r"AlertRuleV2", r"#77\b")),
        ("P6 calibration states", (r"P6", r"calibration")),
        ("presence-aware value", (r"#6\b", r"Evaluation\.Value")),
        ("health views", (r"#17\b", r"health")),
    ],
)
def test_gate11_out_of_scope_names_every_deferred_item(
    topic: str, patterns: tuple[str, ...]
) -> None:
    section = markdown_section(runbook_text(), "Out of scope")
    assert_topic_in_one_item(section, topic, patterns)


def test_gate11_lossless_flux_literals_backlog_is_recorded() -> None:
    """#84 may sit in either "Known divergences and limitations" or "Out of scope"."""
    text = runbook_text()
    items = list_items(markdown_section(text, "Known divergences and limitations")) + list_items(
        markdown_section(text, "Out of scope")
    )
    assert any(re.search(r"#84\b", item) and re.search(r"Flux", item) for item in items)


# --- ADR phase split and README ----------------------------------------------------------------


# sha256 of the `## Implementation gate` text on main before P2-09: it must stay unchanged.
ADR_ORIGINAL_GATE_SHA256 = MappingProxyType(
    {
        ADR_006: "8a0096c1d9fbe4ac2752dbce277035ed7bcc64665bab6ba68d1c4c5a92cb9d8a",
        ADR_007: "1d95075ae0cacc9febf2cfe7bc957d7b620efb71afb090641080032fc1b3dbf6",
    }
)


@pytest.mark.parametrize(
    ("adr", "references"),
    [
        (
            ADR_006,
            ("P2", "P3", "D16", "#15", "#17", "quarantine", r"connector[- ]health", "event time"),
        ),
        (ADR_007, ("P2", "P3", "D16", "#15", "redrive", r"dual[- ]write", "cardinality")),
    ],
    ids=["ADR-006", "ADR-007"],
)
def test_adr_implementation_gate_ends_with_the_p2_phase_split(
    adr: Path, references: tuple[str, ...]
) -> None:
    text = adr.read_text(encoding="utf-8")
    headings = re.findall(r"^## (.+?)\s*$", text, flags=re.MULTILINE)
    assert headings == [
        "Context",
        "Decision",
        "Consequences",
        "V4 traceability",
        "Implementation gate",
        "Non-goals",
    ]
    assert text.count("**Phase split (P2).**") == 1
    section = markdown_section(text, "Implementation gate")
    paragraphs = [p.strip() for p in section.strip().split("\n\n") if p.strip()]
    assert len(paragraphs) >= 2, "the original gate paragraph must stay"
    # D19 (P3-11): the P2 paragraph may be followed by at most the P3 paragraph.
    (p2_index,) = [i for i, p in enumerate(paragraphs) if p.startswith("**Phase split (P2).**")]
    following = paragraphs[p2_index + 1 :]
    assert len(following) <= 1, "only the P3 paragraph may follow the P2 paragraph"
    assert all(p.startswith("**Phase split (P3).**") for p in following)
    original = "\n\n".join(paragraphs[:p2_index])
    assert hashlib.sha256(original.encode("utf-8")).hexdigest() == ADR_ORIGINAL_GATE_SHA256[adr]
    phase_split = " ".join(paragraphs[p2_index].split())
    for reference in references:
        assert re.search(reference, phase_split), reference
    # ADRs live in docs/adr/, so the runbook link is relative to it.
    assert "](../p2-telemetry-rollout-runbook.md)" in phase_split


def test_readme_links_the_rollout_runbook() -> None:
    assert f"]({RUNBOOK_LINK})" in README.read_text(encoding="utf-8")
