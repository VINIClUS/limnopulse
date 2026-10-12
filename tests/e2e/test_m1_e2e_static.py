"""Service-free checks for the M1 e2e harness (ops/e2e).

These guard the device-facing contract: the Wokwi sketch, its serial fixture
and the replay tool must emit exactly the legacy reading keys on the topic
Telegraf consumes, and the Node-RED verdict flow must stay on core nodes.
"""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import subprocess
import sys
import tomllib
import uuid
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "ops/e2e"
SKETCH = E2E / "wokwi/sketch/limnopulse_probe/limnopulse_probe.ino"
FIXTURE = E2E / "wokwi/fixtures/serial.log"
FLOWS = E2E / "node-red/flows.json"
FUNCTIONS = E2E / "node-red/functions"
EXAMPLE = ROOT / "examples/telemetry/reading.local.json"
TELEGRAF = ROOT / "infra/telegraf/telegraf.conf"
REGISTRY = ROOT / "infra/telegraf/device_registry.star"

CORE_NODE_TYPES = {
    "tab",
    "mqtt-broker",
    "mqtt in",
    "mqtt out",
    "http request",
    "function",
    "change",
    "switch",
}
DOMAIN_TAGS = {"tenant" + "_id", "pond" + "_id"}
VERDICT_TOPIC = "limnopulse/e2e/verdicts"


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _example_keys() -> set[str]:
    return set(json.loads(EXAMPLE.read_text(encoding="utf-8")))


def _telegraf_readings_topic() -> str:
    match = re.search(r'topics = \["(limnopulse/v1/devices/\+/readings)"\]', TELEGRAF.read_text())
    assert match is not None
    return match.group(1)


def _sketch_define(name: str) -> str:
    match = re.search(rf'#define {name} "([^"]*)"', SKETCH.read_text(encoding="utf-8"))
    assert match is not None, name
    return match.group(1)


def _fixture_payloads() -> list[dict]:
    payloads = []
    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        if line.startswith("READING "):
            payloads.append(json.loads(line[len("READING ") :]))
    return payloads


def test_sketch_payload_keys_match_the_legacy_example_exactly() -> None:
    sketch = SKETCH.read_text(encoding="utf-8")
    keys = re.findall(r'doc\["([a-z_]+)"\]\s*=', sketch)

    assert len(keys) == len(set(keys))
    assert set(keys) == _example_keys()
    assert keys == list(json.loads(EXAMPLE.read_text(encoding="utf-8")))
    for tag in DOMAIN_TAGS:
        assert tag not in sketch


def test_sketch_topic_matches_telegraf_subscription_and_registry() -> None:
    prefix = _sketch_define("LP_TOPIC_PREFIX")
    suffix = _sketch_define("LP_TOPIC_SUFFIX")
    device_id = _sketch_define("LP_DEVICE_ID")

    assert prefix + "+" + suffix == _telegraf_readings_topic()
    assert "/" not in device_id and "+" not in device_id and "#" not in device_id
    assert f'"{device_id}":' in REGISTRY.read_text(encoding="utf-8")


def test_sketch_prints_capture_lines_and_uses_expected_stack() -> None:
    sketch = SKETCH.read_text(encoding="utf-8")

    assert 'Serial.print("READING ");' in sketch
    assert "#include <PubSubClient.h>" in sketch
    assert "#include <ArduinoJson.h>" in sketch
    assert '"Wokwi-GUEST"' in sketch


def test_fixture_serial_log_lines_parse_with_exact_key_set() -> None:
    payloads = _fixture_payloads()

    assert len(payloads) >= 3
    for payload in payloads:
        assert set(payload) == _example_keys()
        assert payload["ts"].endswith("Z")
    assert [p["seq"] for p in payloads] == sorted(p["seq"] for p in payloads)


def test_replay_tool_agrees_with_fixture_and_rejects_domain_tags() -> None:
    replay = _load("m1_e2e_replay", E2E / "wokwi/replay.py")

    assert set(replay.EXPECTED_KEYS) == _example_keys()
    assert replay.TOPIC_TEMPLATE.format(device_id="+") == _telegraf_readings_topic()
    assert replay.extract_readings(FIXTURE.read_text(encoding="utf-8")) == _fixture_payloads()

    tagged = dict(_fixture_payloads()[0], **{tag: "x" for tag in DOMAIN_TAGS})
    with pytest.raises(replay.ReplayError):
        replay.extract_readings("READING " + json.dumps(tagged))


def test_replay_reanchor_keeps_spacing() -> None:
    replay = _load("m1_e2e_replay", E2E / "wokwi/replay.py")
    now = replay.parse_ts("2030-01-01T00:00:00Z")

    shifted = replay.reanchor(_fixture_payloads(), now)

    assert [p["ts"] for p in shifted] == [
        "2030-01-01T00:00:00Z",
        "2030-01-01T00:00:05Z",
        "2030-01-01T00:00:10Z",
    ]
    assert [p["seq"] for p in shifted] == [p["seq"] for p in _fixture_payloads()]


def test_flows_json_uses_only_core_nodes_and_subscribes_to_readings() -> None:
    flows = json.loads(FLOWS.read_text(encoding="utf-8"))
    types = {node["type"] for node in flows}
    by_type: dict[str, list[dict]] = {}
    for node in flows:
        by_type.setdefault(node["type"], []).append(node)

    assert types <= CORE_NODE_TYPES
    assert [n["topic"] for n in by_type["mqtt in"]] == [_telegraf_readings_topic()]
    assert all(n["libs"] == [] for n in by_type["function"])
    broker = by_type["mqtt-broker"][0]
    assert (broker["broker"], broker["port"]) == ("${MQTT_HOST}", "${MQTT_PORT}")
    topics = [
        rule["to"] for node in by_type["change"] for rule in node["rules"] if rule["p"] == "topic"
    ]
    assert topics == [VERDICT_TOPIC]
    code = "\n".join(n["func"] for n in by_type["function"])
    assert 'env.get("LIMNOPULSE_API_URL")' in code
    assert 'env.get("LIMNOPULSE_TENANT")' in code
    assert '"v2-unavailable"' in code

    ids = {node["id"] for node in flows}
    for node in flows:
        for output in node.get("wires", []):
            assert set(output) <= ids


def test_flows_json_is_generated_from_function_sources() -> None:
    builder = _load("m1_e2e_build_flows", E2E / "node-red/build_flows.py")

    assert FLOWS.read_text(encoding="utf-8") == builder.render()


def test_wokwi_project_points_at_compiled_sketch() -> None:
    config = tomllib.loads((E2E / "wokwi/wokwi.toml").read_text(encoding="utf-8"))["wokwi"]
    diagram = json.loads((E2E / "wokwi/diagram.json").read_text(encoding="utf-8"))
    scenario = yaml.safe_load((E2E / "wokwi/scenario.yaml").read_text(encoding="utf-8"))

    assert config["firmware"].startswith("build/limnopulse_probe.ino.")
    assert config["elf"] == "build/limnopulse_probe.ino.elf"
    part_types = [part["type"] for part in diagram["parts"]]
    assert part_types[0].startswith("board-esp32-devkit")
    assert "wokwi-ds18b20" in part_types
    assert part_types.count("wokwi-potentiometer") >= 4
    waits = [step["wait-serial"] for step in scenario["steps"] if "wait-serial" in step]
    assert "READING {" in waits


def test_e2e_artifacts_avoid_disallowed_storage_terms() -> None:
    forbidden = re.compile(
        "|".join(["post" + "gres", "sql" + "alchemy", "fire" + "store", "fire" + "base"]),
        re.IGNORECASE,
    )
    text_suffixes = {".py", ".js", ".json", ".yaml", ".yml", ".toml", ".sh", ".ino", ".h", ".log"}
    paths = [
        p
        for p in sorted(E2E.rglob("*"))
        if p.is_file() and p.suffix in text_suffixes and not {"build", "__pycache__"} & set(p.parts)
    ]
    paths += sorted((ROOT / "tests/e2e").glob("*.py"))
    paths.append(ROOT / ".github/workflows/e2e-m1.yml")

    offenders = [
        str(p.relative_to(ROOT)) for p in paths if forbidden.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []


# --- Node-RED function bodies executed under node (skipped without node) ---

NODE_HARNESS = r"""
const fs = require("fs");
const [file, input] = [process.argv[1], JSON.parse(process.argv[2])];
const body = fs.readFileSync(file, "utf8");
const env = { get: (k) => (input.env || {})[k] };
const fn = new Function("msg", "env", "node", body);
const out = fn(input.msg, env, {});
process.stdout.write(JSON.stringify(Array.isArray(out) ? out : [out]));
"""


def _run_function(name: str, msg: dict, env: dict | None = None) -> list:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [
            node,
            "-e",
            NODE_HARNESS,
            str(FUNCTIONS / name),
            json.dumps({"msg": msg, "env": env or {}}),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return json.loads(result.stdout)


def _ctx(**overrides: object) -> dict:
    ctx = {
        "base": "http://api.test",
        "tenant": "tnt_local_001",
        "device_id": "local-device-001",
        "seq": 7,
        "ts": "2026-10-12T12:00:00Z",
        "v1_pond_id": "pond_local_001",
        "component_id": None,
        "resolved_asset_id": None,
        "expected_asset_id": None,
    }
    ctx.update(overrides)
    return ctx


def test_node_function_ids_match_python_uuid5() -> None:
    tenant, pond, device = "tnt_local_001", "pond_local_001", "local-device-001"
    out = _run_function(
        "after_v1_device.js",
        {"e2e": _ctx(v1_pond_id=None), "statusCode": 200, "payload": {"pond_id": pond}},
    )

    ctx = out[0]["e2e"]
    assert out[1] is None
    assert (
        ctx["expected_asset_id"]
        == "ast_" + uuid.uuid5(uuid.NAMESPACE_URL, f"limnopulse/v4/legacy-pond/{tenant}/{pond}").hex
    )
    assert (
        ctx["component_id"]
        == "cmp_"
        + uuid.uuid5(
            uuid.NAMESPACE_URL, f"limnopulse/v4/legacy-device-component/{tenant}/{device}"
        ).hex
    )
    assert out[0]["url"].endswith(f"/v2/tenants/{tenant}/devices/{device}/components")


def test_node_function_reports_v2_unavailable_for_unmounted_routes() -> None:
    out = _run_function(
        "after_v2_components.js",
        {"e2e": _ctx(component_id="cmp_x"), "statusCode": 404, "payload": {"detail": "Not Found"}},
    )

    assert out[0] is None
    assert out[1]["payload"]["ok"] is None
    assert out[1]["payload"]["reason"] == "v2-unavailable"


@pytest.mark.parametrize(
    ("ts", "expected"),
    [
        ("2026-10-01T00:00:00Z", "ast_old"),
        ("2026-10-12T11:59:59Z", "ast_old"),
        ("2026-10-12T12:00:00Z", "ast_new"),
        ("2030-01-01T00:00:00Z", "ast_new"),
        ("2026-09-30T23:59:59Z", None),
    ],
)
def test_node_function_resolves_half_open_deployment_intervals(
    ts: str, expected: str | None
) -> None:
    deployments = [
        {
            "deployment_id": "dep_1",
            "component_id": "cmp_x",
            "site_id": "sit_1",
            "asset_id": "ast_old",
            "started_at": "2026-10-01T00:00:00Z",
            "ended_at": "2026-10-12T12:00:00Z",
            "status": "ended",
        },
        {
            "deployment_id": "dep_2",
            "component_id": "cmp_x",
            "site_id": "sit_1",
            "asset_id": "ast_new",
            "started_at": "2026-10-12T12:00:00Z",
            "ended_at": None,
            "status": "active",
        },
    ]
    out = _run_function(
        "resolve_deployment.js",
        {
            "e2e": _ctx(component_id="cmp_x", ts=ts),
            "statusCode": 200,
            "payload": {"items": deployments},
        },
    )

    if expected is None:
        assert out[0] is None
        assert out[1]["payload"]["reason"] == "no-deployment-for-ts"
        assert out[1]["payload"]["ok"] is False
    else:
        assert out[1] is None
        assert out[0]["e2e"]["resolved_asset_id"] == expected
        assert out[0]["url"].endswith(f"/assets/{expected}")


# --- native v2 devices and non-JSON bodies ---

NATIVE_DEVICE = "dev_" + "0123456789abcdef" * 2


def _deployment(component_id: str, asset_id: str, start: str, end: str | None = None) -> dict:
    return {
        "deployment_id": "dep_" + asset_id[-4:] + component_id[-4:],
        "component_id": component_id,
        "site_id": "site_1",
        "asset_id": asset_id,
        "started_at": start,
        "ended_at": end,
        "status": "active" if end is None else "ended",
    }


def test_flow_http_nodes_return_text_and_resolver_loops_for_native_devices() -> None:
    flows = json.loads(FLOWS.read_text(encoding="utf-8"))
    by_id = {node["id"]: node for node in flows}

    http_nodes = [node for node in flows if node["type"] == "http request"]
    assert http_nodes and all(node["ret"] == "txt" for node in http_nodes)
    resolve = by_id["m1e2e_resolve"]
    assert resolve["outputs"] == 3
    assert resolve["wires"][2] == ["m1e2e_http_deployments"]


def test_node_function_v1_404_for_canonical_device_falls_back_to_v2() -> None:
    out = _run_function(
        "after_v1_device.js",
        {
            "e2e": _ctx(device_id=NATIVE_DEVICE, v1_pond_id=None),
            "statusCode": 404,
            "payload": '{"detail":"not found"}',
        },
    )

    assert out[1] is None
    ctx = out[0]["e2e"]
    assert ctx["native"] is True
    assert ctx["expected_asset_id"] is None and ctx["v1_pond_id"] is None
    assert out[0]["url"] == (
        f"http://api.test/v2/tenants/tnt_local_001/devices/{NATIVE_DEVICE}/components"
    )


@pytest.mark.parametrize(
    ("device_id", "status", "body", "reason"),
    [
        ("local-device-001", 404, '{"detail":"not found"}', "v1-device-not-found"),
        ("dev_short", 404, '{"detail":"not found"}', "v1-device-not-found"),
        (NATIVE_DEVICE.upper(), 404, '{"detail":"not found"}', "v1-device-not-found"),
        ("local-device-001", 500, "Internal Server Error", "v1-error-500"),
        ("local-device-001", 200, "<html>proxy</html>", "v1-error-200"),
        ("local-device-001", 200, "[]", "v1-error-200"),
        ("local-device-001", 200, '{"device_id":"local-device-001"}', "v1-device-without-pond"),
    ],
)
def test_node_function_v1_failures_and_non_json_bodies(
    device_id: str, status: int, body: str, reason: str
) -> None:
    out = _run_function(
        "after_v1_device.js",
        {"e2e": _ctx(device_id=device_id, v1_pond_id=None), "statusCode": status, "payload": body},
    )

    assert out[0] is None
    assert out[1]["payload"]["ok"] is False
    assert out[1]["payload"]["reason"] == reason


def test_node_function_v1_text_body_is_parsed() -> None:
    out = _run_function(
        "after_v1_device.js",
        {"e2e": _ctx(v1_pond_id=None), "statusCode": 200, "payload": '{"pond_id":"pond_x"}'},
    )

    assert out[1] is None
    assert out[0]["e2e"]["v1_pond_id"] == "pond_x"
    assert out[0]["e2e"]["native"] is False


def test_node_function_native_components_start_with_the_first_component() -> None:
    body = json.dumps({"items": [{"component_id": "cmp_a"}, {"component_id": "cmp_b"}]})
    out = _run_function(
        "after_v2_components.js",
        {"e2e": _ctx(device_id=NATIVE_DEVICE, native=True), "statusCode": 200, "payload": body},
    )

    assert out[1] is None
    ctx = out[0]["e2e"]
    assert (ctx["component_id"], ctx["pending_component_ids"]) == ("cmp_a", ["cmp_b"])
    assert out[0]["url"].endswith("/deployments?component_id=cmp_a")


@pytest.mark.parametrize(
    ("status", "body", "ok", "reason"),
    [
        (404, '{"detail":"not found"}', False, "v2-native-device-not-found"),
        (404, '{"detail":"Not Found"}', None, "v2-unavailable"),
        (200, '{"items":[]}', False, "v2-native-no-components"),
        (500, "Internal Server Error", False, "v2-components-error-500"),
    ],
)
def test_node_function_native_component_failures(
    status: int, body: str, ok: bool | None, reason: str
) -> None:
    out = _run_function(
        "after_v2_components.js",
        {"e2e": _ctx(device_id=NATIVE_DEVICE, native=True), "statusCode": status, "payload": body},
    )

    assert out[0] is None
    assert (out[1]["payload"]["ok"], out[1]["payload"]["reason"]) == (ok, reason)


def _native_resolve(component_id: str, pending: list[str], matches: list, deployments: list):
    ctx = _ctx(
        device_id=NATIVE_DEVICE,
        native=True,
        v1_pond_id=None,
        component_id=component_id,
        pending_component_ids=pending,
        native_matches=matches,
        ts="2026-10-12T12:00:00Z",
    )
    return _run_function(
        "resolve_deployment.js",
        {"e2e": ctx, "statusCode": 200, "payload": json.dumps({"items": deployments})},
    )


def test_node_function_native_resolver_walks_every_component() -> None:
    first = _native_resolve(
        "cmp_a", ["cmp_b"], [], [_deployment("cmp_a", "ast_1", "2026-10-12T11:00:00Z")]
    )

    assert first[0] is None and first[1] is None
    assert first[2]["e2e"]["component_id"] == "cmp_b"
    assert [m["asset_id"] for m in first[2]["e2e"]["native_matches"]] == ["ast_1"]
    assert first[2]["url"].endswith("/deployments?component_id=cmp_b")

    last = _native_resolve(
        "cmp_b",
        [],
        first[2]["e2e"]["native_matches"],
        [_deployment("cmp_b", "ast_1", "2026-10-12T10:00:00Z")],
    )
    assert last[1] is None and last[2] is None
    assert last[0]["e2e"]["resolved_asset_id"] == "ast_1"
    assert last[0]["url"].endswith("/assets/ast_1")


@pytest.mark.parametrize(
    ("matches", "deployments", "reason"),
    [
        (
            [],
            [_deployment("cmp_b", "ast_2", "2026-10-12T12:00:01Z")],
            "v2-native-no-deployment-for-ts",
        ),
        (
            [
                {
                    "component_id": "cmp_a",
                    "deployment_id": "dep_a",
                    "asset_id": "ast_1",
                    "ended": False,
                }
            ],
            [_deployment("cmp_b", "ast_2", "2026-10-12T12:00:00Z")],
            "v2-native-ambiguous-assets",
        ),
        (
            [],
            [
                _deployment("cmp_b", "ast_1", "2026-10-12T11:00:00Z"),
                _deployment("cmp_b", "ast_2", "2026-10-12T11:30:00Z"),
            ],
            "v2-native-overlapping-deployments",
        ),
    ],
)
def test_node_function_native_resolver_failures(
    matches: list, deployments: list, reason: str
) -> None:
    out = _native_resolve("cmp_b", [], matches, deployments)

    assert out[0] is None and out[2] is None
    assert (out[1]["payload"]["ok"], out[1]["payload"]["reason"]) == (False, reason)


@pytest.mark.parametrize(
    ("status", "body", "ok", "reason"),
    [
        (200, '{"asset_id":"ast_1","pond_profile":{"legacy_pond_id":null}}', True, "v2-native"),
        (200, '{"asset_id":"ast_2"}', False, "v2-native-asset-mismatch"),
        (404, '{"detail":"not found"}', False, "v2-native-asset-lookup-error-404"),
        (500, "Internal Server Error", False, "v2-native-asset-lookup-error-500"),
    ],
)
def test_node_function_native_final_verdict(status: int, body: str, ok: bool, reason: str) -> None:
    ctx = _ctx(device_id=NATIVE_DEVICE, native=True, v1_pond_id=None, resolved_asset_id="ast_1")
    out = _run_function("final_verdict.js", {"e2e": ctx, "statusCode": status, "payload": body})

    verdict = out[0]["payload"]
    assert (verdict["ok"], verdict["reason"]) == (ok, reason)
    assert verdict["expected_asset_id"] is None and verdict["v1_pond_id"] is None


def test_node_function_legacy_final_verdict_parses_text_body() -> None:
    tenant, pond = "tnt_local_001", "pond_local_001"
    asset = (
        "ast_" + uuid.uuid5(uuid.NAMESPACE_URL, f"limnopulse/v4/legacy-pond/{tenant}/{pond}").hex
    )
    ctx = _ctx(resolved_asset_id=asset, expected_asset_id=asset)
    body = json.dumps({"asset_id": asset, "pond_profile": {"legacy_pond_id": pond}})
    out = _run_function("final_verdict.js", {"e2e": ctx, "statusCode": 200, "payload": body})

    assert (out[0]["payload"]["ok"], out[0]["payload"]["reason"]) == (True, "match")


# --- scenarios and summary helpers ---


def test_scenario_ids_match_the_flow_and_ts_rounding() -> None:
    scenarios = _load("m1_e2e_scenarios", E2E / "scenarios.py")
    tenant, pond, device = "tnt_local_001", "pond_local_001", "local-device-001"

    assert scenarios.legacy_asset_id(tenant, pond) == (
        "ast_" + uuid.uuid5(uuid.NAMESPACE_URL, f"limnopulse/v4/legacy-pond/{tenant}/{pond}").hex
    )
    flow_ctx = _run_function(
        "after_v1_device.js",
        {"e2e": _ctx(v1_pond_id=None), "statusCode": 200, "payload": {"pond_id": pond}},
    )[0]["e2e"]
    assert scenarios.legacy_component_id(tenant, device) == flow_ctx["component_id"]
    assert (
        scenarios.format_ts(
            scenarios.ceil_second(scenarios.parse_ts("2026-10-12T12:00:05.000001Z"))
        )
        == "2026-10-12T12:00:06Z"
    )
    assert (
        scenarios.format_ts(scenarios.ceil_second(scenarios.parse_ts("2026-10-12T12:00:05Z")))
        == "2026-10-12T12:00:05Z"
    )


def test_scenario_and_harness_avoid_moving_the_registry_device() -> None:
    scenarios_text = (E2E / "scenarios.py").read_text(encoding="utf-8")
    runner = (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")

    assert "local-device-001" in scenarios_text  # documented, never PATCHed
    assert "device_registry.star" in scenarios_text
    assert 'E2E_SCENARIOS="${E2E_SCENARIOS-relocation v2-native}"' in runner
    assert '"$E2E_DIR/scenarios.py"' in runner


def test_summarize_filters_several_devices_and_counts_reasons(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    summarize = _load("m1_e2e_summarize", E2E / "summarize_verdicts.py")
    keys = dict.fromkeys(summarize.VERDICT_KEYS)
    raw = tmp_path / "verdicts.raw"
    rows = [
        {**keys, "device_id": "a", "seq": 1, "ok": True, "reason": "match"},
        {**keys, "device_id": "b", "seq": 1, "ok": True, "reason": "v2-native"},
        {**keys, "device_id": "c", "seq": 1, "ok": False, "reason": "v1-device-not-found"},
    ]
    raw.write_text("".join(json.dumps({"topic": "t", "payload": json.dumps(r)}) for r in rows))

    code = summarize.main(
        ["--raw", str(raw), "--device-id", "a", "--device-id", "b", "--expect", "2"]
    )

    out = capsys.readouterr().out
    assert code == 0
    assert "REASONS true:match=1 true:v2-native=1" in out
    assert "verdicts=2 expected=2 ok_true=2 ok_false=0 ok_null=0 failures=0" in out


def test_ci_stack_runs_the_real_migration_and_enables_both_m1_flags() -> None:
    script = (E2E / "ci_stack.sh").read_text(encoding="utf-8")
    workflow = yaml.safe_load((ROOT / ".github/workflows/e2e-m1.yml").read_text(encoding="utf-8"))

    assert 'MIGRATION_MODULE="scripts.migrations.m1_backfill_default_site_assets"' in script
    assert "for mode in dry-run apply apply-again; do" in script
    assert "API_V2_ENABLED=true V1_POND_DEVICE_PROJECTION_ENABLED=true" in script
    # Strict by default: ok=null only with the dispatch input or the repository variable.
    assert workflow["env"]["ALLOW_V2_UNAVAILABLE"] == (
        "${{ (inputs.allow_v2_unavailable || vars.M1_E2E_ALLOW_V2_UNAVAILABLE == '1') "
        "&& '1' || '0' }}"
    )
    triggers = workflow[True]  # PyYAML reads the bare `on:` key as True
    assert set(triggers) == {"workflow_dispatch", "schedule"}
    # The token reaches only the step that runs wokwi-cli; the job is gated on
    # a boolean computed by a separate job.
    gate = workflow["jobs"]["wokwi-gate"]
    assert gate["steps"][0]["env"] == {"HAS_TOKEN": "${{ secrets.WOKWI_CLI_TOKEN != '' }}"}
    wokwi = workflow["jobs"]["wokwi"]
    assert wokwi["needs"] == "wokwi-gate"
    assert wokwi["if"] == "needs.wokwi-gate.outputs.has_token == 'true'"
    assert "WOKWI_CLI_TOKEN" not in wokwi.get("env", {})
    with_token = [step for step in wokwi["steps"] if "WOKWI_CLI_TOKEN" in step.get("env", {})]
    assert [step["name"] for step in with_token] == ["Simulate and capture serial output"]
    assert "wokwi-cli ops/e2e/wokwi" in with_token[0]["run"]
    raw = (ROOT / ".github/workflows/e2e-m1.yml").read_text(encoding="utf-8")
    assert raw.count("secrets.WOKWI_CLI_TOKEN") == 2


def test_ci_stack_down_is_guarded_and_stops_the_real_server() -> None:
    script = (E2E / "ci_stack.sh").read_text(encoding="utf-8")

    assert '"${1:-}" != "--force" && "${CI:-}" != "true" && "${GITHUB_ACTIONS:-}" != "true"' in (
        script
    )
    assert 'down) down "${2:-}" ;;' in script
    assert 'nohup "$uvicorn_bin" limnopulse_api.main:app' in script
    assert 'nohup "$UV" run' not in script
    assert (
        'pgrep -f "uvicorn limnopulse_api.main:app --host 127.0.0.1 --port ${API_PORT}"' in script
    )


def test_ci_stack_down_refuses_outside_ci(tmp_path: Path) -> None:
    env = {"PATH": "/usr/bin:/bin", "E2E_CI_STATE_DIR": str(tmp_path)}
    result = subprocess.run(
        ["bash", str(E2E / "ci_stack.sh"), "down"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )

    assert result.returncode == 2
    assert "refusing to run 'down' outside CI" in result.stderr


@pytest.mark.parametrize(
    ("script", "status", "body", "ok", "reason"),
    [
        ("after_v2_components.js", 404, {"detail": "Not Found"}, None, "v2-unavailable"),
        ("after_v2_components.js", 501, "Not Implemented", None, "v2-unavailable"),
        (
            "after_v2_components.js",
            503,
            '{"detail":"service unavailable"}',
            False,
            "v2-components-error-503",
        ),
        ("after_v2_components.js", 503, "Service Unavailable", False, "v2-components-error-503"),
        (
            "after_v2_components.js",
            404,
            '{"detail":"v2 disabled"}',
            False,
            "v2-components-error-404",
        ),
        (
            "after_v2_components.js",
            404,
            '{"detail":"Not Found","x":1}',
            False,
            "v2-components-error-404",
        ),
        ("after_v2_components.js", 404, "Not Found", False, "v2-components-error-404"),
        ("resolve_deployment.js", 404, '{"detail":"Not Found"}', None, "v2-unavailable"),
        ("resolve_deployment.js", 501, "", None, "v2-unavailable"),
        (
            "resolve_deployment.js",
            503,
            '{"detail":"service unavailable"}',
            False,
            "v2-deployments-error-503",
        ),
        ("resolve_deployment.js", 404, '{"detail":"not found"}', False, "v2-deployments-error-404"),
    ],
)
def test_node_function_v2_unavailable_is_only_an_unmounted_route(
    script: str, status: int, body: object, ok: bool | None, reason: str
) -> None:
    out = _run_function(
        script,
        {"e2e": _ctx(component_id="cmp_x"), "statusCode": status, "payload": body},
    )

    assert out[0] is None
    assert (out[1]["payload"]["ok"], out[1]["payload"]["reason"]) == (ok, reason)


@pytest.mark.parametrize(
    ("ts", "expected"),
    [
        # Transition at .000400: inside the first millisecond, which Date.parse
        # would truncate to .000 and so wrongly attribute to the new interval.
        ("2026-10-12T12:00:05Z", "ast_old"),
        ("2026-10-12T12:00:05.000399Z", "ast_old"),
        ("2026-10-12T12:00:05.0004Z", "ast_new"),
        ("2026-10-12T12:00:05.000400+00:00", "ast_new"),
        ("2026-10-12T12:00:05.001Z", "ast_new"),
    ],
)
def test_node_function_compares_deployment_bounds_at_microsecond_precision(
    ts: str, expected: str
) -> None:
    boundary = "2026-10-12T12:00:05.000400Z"
    deployments = [
        _deployment("cmp_x", "ast_old", "2026-10-12T11:00:00.123456Z", boundary),
        _deployment("cmp_x", "ast_new", boundary),
    ]
    out = _run_function(
        "resolve_deployment.js",
        {
            "e2e": _ctx(component_id="cmp_x", ts=ts),
            "statusCode": 200,
            "payload": json.dumps({"items": deployments}),
        },
    )

    assert out[1] is None
    assert out[0]["e2e"]["resolved_asset_id"] == expected


def test_summarize_counts_failed_scenarios_as_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    summarize = _load("m1_e2e_summarize", E2E / "summarize_verdicts.py")
    keys = dict.fromkeys(summarize.VERDICT_KEYS)
    raw = tmp_path / "verdicts.raw"
    raw.write_text(json.dumps({**keys, "device_id": "a", "seq": 1, "ok": True, "reason": "match"}))

    code = summarize.main(["--raw", str(raw), "--expect", "1", "--scenarios-failed", "1"])

    out = capsys.readouterr().out
    assert code == 1
    assert "failures=1 allow_v2_unavailable=0 scenarios_failed=1" in out


def test_failed_scenarios_still_report_their_devices(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    scenarios = _load("m1_e2e_scenarios", E2E / "scenarios.py")

    def failing_relocation(self: object) -> None:
        self.published["dev_" + "a" * 32] = 1  # type: ignore[attr-defined]
        raise scenarios.ScenarioError("seq=1: resolved mismatch")

    monkeypatch.setattr(scenarios.Harness, "relocation", failing_relocation)
    code = scenarios.main(["relocation", "--raw", str(tmp_path / "raw")])

    assert code == 1
    assert capsys.readouterr().out.splitlines() == ["SCENARIO_DEVICE dev_" + "a" * 32 + " 1"]


def test_run_script_fails_fast_without_replayed_line_and_reports_scenarios_failed() -> None:
    runner = (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")

    assert 'expected="${expected:-0}"' in runner
    assert "replay.py reported no REPLAYED line" in runner
    assert '--scenarios-failed "$scenarios_failed"' in runner
