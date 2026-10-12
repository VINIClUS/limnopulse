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
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
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
    assert "verdicts=2 duplicates=0 expected=2 missing=0 ok_true=2 ok_false=0 ok_null=0" in out


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
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
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
    monkeypatch.chdir(tmp_path)
    code = scenarios.main(["relocation", "--raw", "raw"])

    assert code == 1
    assert capsys.readouterr().out.splitlines() == ["SCENARIO_DEVICE dev_" + "a" * 32 + " 1"]


def test_run_script_fails_fast_without_replayed_line_and_reports_scenarios_failed() -> None:
    runner = (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")

    assert 'expected="${expected:-0}"' in runner
    assert "replay.py reported no REPLAYED line" in runner
    assert '--scenarios-failed "$scenarios_failed"' in runner


# --- Sonar: path containment, validated mqttx argv, JS fixes ---


def _verdict_rows(summarize: ModuleType, *rows: dict) -> str:
    keys = dict.fromkeys(summarize.VERDICT_KEYS)
    return "".join(json.dumps({"payload": json.dumps({**keys, **row})}) for row in rows)


def test_summarize_rejects_paths_outside_the_working_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    summarize = _load("m1_e2e_summarize", E2E / "summarize_verdicts.py")
    inside = tmp_path / "repo"
    inside.mkdir()
    outside = tmp_path / "outside.raw"
    outside.write_text("{}")
    monkeypatch.chdir(inside)

    assert summarize.main(["--raw", str(outside)]) == 2
    assert summarize.main(["--raw", "../outside.raw"]) == 2
    assert "inside the working directory" in capsys.readouterr().err
    (inside / "dir.raw").mkdir()
    assert summarize.main(["--raw", "dir.raw"]) == 2


def test_replay_rejects_serial_logs_outside_the_working_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    replay = _load("m1_e2e_replay", E2E / "wokwi/replay.py")
    inside = tmp_path / "repo"
    inside.mkdir()
    (tmp_path / "outside.log").write_text(FIXTURE.read_text(encoding="utf-8"))
    (inside / "big.log").write_bytes(b"x" * (replay.MAX_SERIAL_LOG_BYTES + 1))
    (inside / "ok.log").write_text(FIXTURE.read_text(encoding="utf-8"))
    monkeypatch.chdir(inside)
    monkeypatch.delenv("WOKWI_SERIAL_LOG", raising=False)

    for bad in (str(tmp_path / "outside.log"), "../outside.log", "missing.log", "big.log", "."):
        with pytest.raises(replay.ReplayError):
            replay.resolve_source(bad)
    assert replay.resolve_source("ok.log") == (inside / "ok.log").resolve()
    monkeypatch.setenv("WOKWI_SERIAL_LOG", "../outside.log")
    with pytest.raises(replay.ReplayError):
        replay.resolve_source(None)


def _fake_mqttx(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    tool = tmp_path / "bin" / "mqttx"
    tool.parent.mkdir()
    tool.write_text("#!/bin/sh\nexit 0\n")
    tool.chmod(0o755)
    monkeypatch.setenv("PATH", str(tool.parent))
    return tool


def test_replay_publish_argv_uses_only_validated_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    replay = _load("m1_e2e_replay", E2E / "wokwi/replay.py")
    tool = _fake_mqttx(monkeypatch, tmp_path)
    reading = _fixture_payloads()[0]

    argv = replay.publish_argv("127.0.0.1", "1883", "local-device-001", 1, reading)

    assert argv[0] == str(tool.resolve())
    assert argv[1:-1] == [
        "pub",
        "--hostname",
        "127.0.0.1",
        "--port",
        "1883",
        "--topic",
        "limnopulse/v1/devices/local-device-001/readings",
        "--qos",
        "1",
        "--message",
    ]
    assert json.loads(argv[-1]) == reading
    assert replay.publish_argv("mqtt-broker.local", 1883, "dev_" + "a" * 32, 0, reading)[3] == (
        "mqtt-broker.local"
    )

    bad_calls = [
        ("127.0.0.1;rm -rf /", 1883, "local-device-001", 1, reading),
        ("-h", 1883, "local-device-001", 1, reading),
        ("a..b", 1883, "local-device-001", 1, reading),
        ("127.0.0.1", 0, "local-device-001", 1, reading),
        ("127.0.0.1", 65536, "local-device-001", 1, reading),
        ("127.0.0.1", "18x", "local-device-001", 1, reading),
        ("127.0.0.1", 1883, "a/b", 1, reading),
        ("127.0.0.1", 1883, "-t", 1, reading),
        ("127.0.0.1", 1883, "+", 1, reading),
        ("127.0.0.1", 1883, "local-device-001", 3, reading),
        ("127.0.0.1", 1883, "local-device-001", 1, {**reading, "tenant" + "_id": "x"}),
        ("127.0.0.1", 1883, "local-device-001", 1, {**reading, "seq": {"$ne": 1}}),
    ]
    for args in bad_calls:
        with pytest.raises(replay.ReplayError):
            replay.publish_argv(*args)

    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    with pytest.raises(replay.ReplayError):
        replay.publish_argv("127.0.0.1", 1883, "local-device-001", 1, reading)


def test_scenarios_publish_through_the_validated_replay_path() -> None:
    text = (E2E / "scenarios.py").read_text(encoding="utf-8")

    assert "subprocess" not in text
    assert "self.replay.publish(" in text
    assert "--mqttx" not in text
    assert "--mqttx" not in (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")


# --- Codex review: identity-based verdict counting ---


@pytest.mark.parametrize(
    ("rows", "expect", "code", "fragment"),
    [
        # Two verdicts for seq 1 must not stand in for the missing seq 3.
        (
            [
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
                {"device_id": "a", "seq": 2, "ts": "t2", "ok": True, "reason": "match"},
            ],
            ["a,1,t1", "a,2,t2", "a,3,t3"],
            3,
            "verdicts=2 duplicates=1 expected=3 missing=1",
        ),
        # Agreeing duplicates (QoS-1 redelivery) are tolerated.
        (
            [
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
            ],
            ["a,1,t1"],
            0,
            "verdicts=1 duplicates=1 expected=1 missing=0 ok_true=1 ok_false=0 ok_null=0 failures=0",
        ),
        # Duplicates that disagree on the outcome or the deployment fail.
        (
            [
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": False, "reason": "asset-mismatch"},
            ],
            ["a,1,t1"],
            1,
            "FAIL: conflicting duplicate verdicts for ('a', '1', 't1')",
        ),
        (
            [
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "resolved_asset_id": "x"},
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "resolved_asset_id": "y"},
            ],
            ["a,1,t1"],
            1,
            "conflicting duplicate verdicts",
        ),
        # A verdict for a reading that was never published fails.
        (
            [
                {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
                {"device_id": "a", "seq": 9, "ts": "t9", "ok": True, "reason": "match"},
            ],
            ["a,1,t1"],
            1,
            "FAIL: unexpected verdict for ('a', '9', 't9')",
        ),
    ],
)
def test_summarize_counts_distinct_published_readings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    rows: list[dict],
    expect: list[str],
    code: int,
    fragment: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    summarize = _load("m1_e2e_summarize", E2E / "summarize_verdicts.py")
    (tmp_path / "v.raw").write_text(_verdict_rows(summarize, *rows))
    args = ["--raw", "v.raw"]
    for item in expect:
        args += ["--expect-reading", item]

    assert summarize.main(args) == code
    assert fragment in capsys.readouterr().out
    assert summarize.main([*args, "--count-only"]) == 0
    assert capsys.readouterr().out.strip() == str(
        len({tuple(e.split(",")) for e in expect} & {("a", str(r["seq"]), r["ts"]) for r in rows})
    )


def test_run_script_expects_every_published_reading() -> None:
    runner = (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")

    assert "grep '^PUBLISHED '" in runner
    assert '"${replay_expect[@]}" --count-only' in runner
    assert '"${all_expect[@]}" --scenarios-failed' in runner
    assert '--jsonl >"$WORK/verdicts.jsonl"' in runner
    assert "--jsonl-out" not in runner
    assert '--expect "$' not in runner
    assert 'WORK="$(realpath -m -- "${E2E_WORK_DIR:-$ROOT/.e2e-out/m1-e2e}")"' in runner
    assert '"$ROOT"/.e2e-out/?*) ;;' in runner
    assert 'cd "$ROOT"' in runner


# --- Codex review: relocation must keep the West history ---

WEST = {
    "deployment_id": "dep_" + "1" * 32,
    "component_id": "cmp_" + "c" * 32,
    "site_id": "site_" + "s" * 32,
    "asset_id": "ast_west",
    "started_at": "2026-10-12T12:00:00Z",
    "ended_at": None,
    "status": "active",
}
BOUNDARY = "2026-10-12T12:00:05.000400Z"


def _history(**overrides: dict) -> list[dict]:
    ended = {**WEST, "ended_at": BOUNDARY, "status": "ended", **overrides.get("ended", {})}
    east = {
        **WEST,
        "deployment_id": "dep_" + "2" * 32,
        "asset_id": "ast_east",
        "started_at": BOUNDARY,
        **overrides.get("east", {}),
    }
    return [ended, east]


def test_relocation_history_accepts_an_ended_west_and_active_east_interval() -> None:
    scenarios = _load("m1_e2e_scenarios", E2E / "scenarios.py")

    boundary = scenarios.check_relocation_history(_history(), WEST, "ast_east")

    assert boundary == scenarios.parse_ts(BOUNDARY)
    assert scenarios.ceil_second(boundary) == scenarios.parse_ts("2026-10-12T12:00:06Z")


@pytest.mark.parametrize(
    "after",
    [
        # CURRENT mutated West->East in place, history gone.
        [{**WEST, "asset_id": "ast_east"}],
        # West interval deleted.
        _history()[1:],
        # West not ended.
        _history(ended={"status": "active"}),
        # West reassigned to another asset/site.
        _history(ended={"asset_id": "ast_other"}),
        _history(ended={"site_id": "site_" + "x" * 32}),
        # West start rewritten.
        _history(ended={"started_at": "2026-10-12T12:00:01Z"}),
        # Gap or overlap at the boundary (microsecond precision).
        _history(ended={"ended_at": "2026-10-12T12:00:05.000399Z"}),
        _history(east={"started_at": "2026-10-12T12:00:05.000401Z"}),
        # East interval not active or not East.
        _history(east={"status": "ended", "ended_at": None}),
        _history(east={"asset_id": "ast_west"}),
        # Transition not after the West start.
        _history(
            ended={"ended_at": "2026-10-12T12:00:00Z"}, east={"started_at": "2026-10-12T12:00:00Z"}
        ),
    ],
)
def test_relocation_history_rejects_lost_or_rewritten_history(after: list[dict]) -> None:
    scenarios = _load("m1_e2e_scenarios", E2E / "scenarios.py")

    with pytest.raises(scenarios.ScenarioError):
        scenarios.check_relocation_history(after, WEST, "ast_east")


# --- Codex review: /v2 unavailable means the same as in the flow ---


@pytest.mark.parametrize(
    ("status", "payload", "unavailable"),
    [
        (501, None, True),
        (501, "Not Implemented", True),
        (404, {"detail": "Not Found"}, True),
        (404, {"detail": "not found"}, False),
        (404, {"detail": "Not Found", "x": 1}, False),
        (503, {"detail": "service unavailable"}, False),
    ],
)
def test_scenario_api_maps_v2_unavailable_like_the_flow(
    monkeypatch: pytest.MonkeyPatch, status: int, payload: object, unavailable: bool
) -> None:
    scenarios = _load("m1_e2e_scenarios", E2E / "scenarios.py")
    api = scenarios.Api("http://api.test", "tnt_local_001", "sub", "e@x.test")
    monkeypatch.setattr(api, "call", lambda *args, **kwargs: (status, payload))

    expected = scenarios.V2Unavailable if unavailable else scenarios.SetupError
    with pytest.raises(expected):
        api.expect(200, "GET", "/v2/tenants/tnt_local_001/sites")
    with pytest.raises(scenarios.SetupError):
        api.expect(200, "GET", "/v1/tenants/tnt_local_001/ponds")


# --- Sonar: prepare_v1_request.js ---


def test_node_function_prepare_strips_slashes_sorts_keys_and_reads_seq() -> None:
    reading = dict(_fixture_payloads()[0])
    reversed_reading = dict(reversed(list(reading.items())))
    msg = {"topic": "limnopulse/v1/devices/local-device-001/readings", "payload": reversed_reading}

    out = _run_function("prepare_v1_request.js", msg, {"LIMNOPULSE_API_URL": "http://api.test///"})

    assert out[1] is None
    assert out[0]["url"] == "http://api.test/v1/tenants/tnt_local_001/devices/local-device-001"
    assert out[0]["e2e"]["seq"] == reading["seq"]

    without_seq = {k: v for k, v in reading.items() if k != "seq"}
    out = _run_function("prepare_v1_request.js", {**msg, "payload": without_seq})
    assert out[1]["payload"]["seq"] is None
    assert out[1]["payload"]["reason"] == "payload-keys-mismatch"

    out = _run_function("prepare_v1_request.js", {**msg, "payload": ["not", "an", "object"]})
    assert out[1]["payload"]["reason"] == "payload-keys-mismatch"


def test_node_functions_avoid_flagged_constructs() -> None:
    prepare = (FUNCTIONS / "prepare_v1_request.js").read_text(encoding="utf-8")
    sha = (FUNCTIONS / "after_v1_device.js").read_text(encoding="utf-8")
    resolve = (FUNCTIONS / "resolve_deployment.js").read_text(encoding="utf-8")

    assert ".sort()" not in prepare and "/+$/" not in prepare
    assert re.search(r"\) \| 0;", sha) is None
    assert re.search(r"(?<![.\w])(NaN|parseInt)\b", resolve) is None


def test_summarize_jsonl_prints_selected_verdicts_without_writing_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    summarize = _load("m1_e2e_summarize_jsonl", E2E / "summarize_verdicts.py")
    monkeypatch.chdir(tmp_path)
    rows = [
        {"device_id": "a", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
        {"device_id": "b", "seq": 1, "ts": "t1", "ok": True, "reason": "match"},
    ]
    (tmp_path / "v.raw").write_text("".join(json.dumps(r) + "\n" for r in rows))

    assert summarize.main(["--raw", "v.raw", "--device-id", "a", "--jsonl"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert [json.loads(line)["device_id"] for line in lines] == ["a"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["v.raw"]


@pytest.mark.parametrize(
    ("body", "ok", "reason"),
    [
        (
            '{"asset_id":"ast_other","pond_profile":{"legacy_pond_id":"pond_a"}}',
            False,
            "asset-response-mismatch",
        ),
        ('{"pond_profile":{"legacy_pond_id":"pond_a"}}', False, "asset-response-mismatch"),
        ("not json", False, "asset-lookup-error-200"),
        ('{"asset_id":"ast_1","pond_profile":{"legacy_pond_id":"pond_a"}}', True, "match"),
    ],
)
def test_node_function_legacy_final_verdict_checks_the_returned_asset(
    body: str, ok: bool, reason: str
) -> None:
    ctx = _ctx(v1_pond_id="pond_a", resolved_asset_id="ast_1", expected_asset_id="ast_1")
    out = _run_function("final_verdict.js", {"e2e": ctx, "statusCode": 200, "payload": body})

    assert (out[0]["payload"]["ok"], out[0]["payload"]["reason"]) == (ok, reason)


@pytest.mark.parametrize(("reason", "code"), [("v2-unavailable", 0), ("something-else", 1)])
def test_summarize_allow_mode_only_excuses_v2_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reason: str, code: int
) -> None:
    monkeypatch.chdir(tmp_path)
    summarize = _load("m1_e2e_summarize_allow", E2E / "summarize_verdicts.py")
    keys = dict.fromkeys(summarize.VERDICT_KEYS)
    (tmp_path / "v.raw").write_text(
        json.dumps({**keys, "device_id": "a", "seq": 1, "ts": "t", "ok": None, "reason": reason})
    )

    assert summarize.main(["--raw", "v.raw", "--allow-v2-unavailable"]) == code


@pytest.mark.parametrize(("reason", "accepted"), [("v2-unavailable", True), ("other", False)])
def test_scenario_check_allow_mode_only_excuses_v2_unavailable(reason: str, accepted: bool) -> None:
    from types import SimpleNamespace

    scenarios = _load("m1_e2e_scenarios_allow", E2E / "scenarios.py")
    harness = SimpleNamespace(
        args=SimpleNamespace(allow_v2_unavailable=True), log=lambda message: None
    )
    verdict = {"seq": 1, "ok": None, "reason": reason}

    def run() -> None:
        scenarios.Harness.check(harness, verdict, reason="match", resolved="ast_1", v1_pond_id="p")

    if accepted:
        run()
    else:
        with pytest.raises(scenarios.ScenarioError):
            run()


@pytest.mark.parametrize("work", ["ops/e2e", "src", ".git", ".e2e-out", "../elsewhere"])
def test_run_script_refuses_work_dirs_outside_e2e_out(work: str) -> None:
    root = E2E.parents[1]
    result = subprocess.run(
        ["bash", str(E2E / "run_m1_e2e.sh")],
        cwd=root,
        env={"PATH": "/usr/bin:/bin", "E2E_WORK_DIR": str(root / work)},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 2
    assert "must be a subdirectory of" in result.stderr
    assert (root / "ops" / "e2e").is_dir() and (root / "src").is_dir()


_NOOP_SUMMARY = {
    "apply": True,
    "tenants_requested": 1,
    "tenants_completed": 1,
    "default_sites_created": 0,
    "assets_created": 0,
    "devices_projected": 0,
    "components_created": 0,
    "deployments_created": 0,
    "legacy_links_updated": 0,
    "already_migrated": 2,
    "schema_conflicts": 0,
    "conflicts": [],
    "scope_completed": True,
    "result": "success",
}


@pytest.mark.parametrize(
    ("override", "ok"),
    [
        ({}, True),
        ({"deployments_created": 1}, False),
        ({"legacy_links_updated": 2}, False),
        ({"devices_projected": 1}, False),
        ({"apply": False}, False),
        ({"result": "failed"}, False),
        ({"schema_conflicts": 1, "conflicts": [{"kind": "asset"}]}, False),
    ],
)
def test_assert_migration_noop(override: dict, ok: bool) -> None:
    result = subprocess.run(
        [sys.executable, str(E2E / "assert_migration_noop.py")],
        input=json.dumps({**_NOOP_SUMMARY, **override}),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert (result.returncode == 0) is ok, result.stderr


def test_ci_stack_checks_the_second_apply_is_a_noop() -> None:
    stack = (E2E / "ci_stack.sh").read_text(encoding="utf-8")

    assert 'assert_migration_noop.py" \\\n    <"$STATE/migration-apply-again.json"' in stack


@pytest.mark.parametrize(
    "bad",
    ["READING null", "READING []", 'READING {"ts":', "boot ok READING 42"],
)
def test_replay_rejects_every_malformed_reading_record(bad: str) -> None:
    replay = _load("m1_e2e_replay_malformed", E2E / "wokwi" / "replay.py")
    good = (E2E / "wokwi" / "fixtures" / "serial.log").read_text(encoding="utf-8")

    with pytest.raises(replay.ReplayError):
        replay.extract_readings(good + "\n" + bad + "\n")


def test_replay_ignores_words_that_only_contain_reading() -> None:
    replay = _load("m1_e2e_replay_words", E2E / "wokwi" / "replay.py")
    good = (E2E / "wokwi" / "fixtures" / "serial.log").read_text(encoding="utf-8")

    assert len(replay.extract_readings("PREREADING x\n" + good)) == 3


def test_replay_rejects_duplicate_readings() -> None:
    replay = _load("m1_e2e_replay_dupes", E2E / "wokwi" / "replay.py")
    good = (E2E / "wokwi" / "fixtures" / "serial.log").read_text(encoding="utf-8")
    first = next(line for line in good.splitlines() if line.startswith("READING "))

    with pytest.raises(replay.ReplayError, match="duplicate reading"):
        replay.extract_readings(good + "\n" + first + "\n")


def test_run_script_requires_distinct_published_readings_and_checks_pid_identity() -> None:
    runner = (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")

    assert "sort -u | wc -l" in runner
    assert 'record_pid "$node_red_pid"' in runner
    assert "record_pid $!" in runner
    assert '>>"$PIDFILE"' in runner.split("record_pid() {", 1)[1].split("}", 1)[0]
    assert 'echo $! >>"$PIDFILE"' not in runner


def test_run_script_stop_pids_skips_reused_pids(tmp_path: Path) -> None:
    runner = (E2E / "run_m1_e2e.sh").read_text(encoding="utf-8")
    functions = runner.split("pid_start() {", 1)[1].split("cleanup() {", 1)[0]
    sleeper = subprocess.Popen(["sleep", "30"])
    try:
        pidfile = tmp_path / "pids"
        # A stale entry whose recorded start time no longer matches the PID.
        pidfile.write_text(f"{sleeper.pid} 1\n")
        script = f'PIDFILE="{pidfile}"\npid_start() {{{functions}\nstop_pids\n'
        subprocess.run(["bash", "-c", script], check=True, timeout=30)
        assert sleeper.poll() is None
        # A matching entry is stopped.
        script = (
            f'PIDFILE="{pidfile}"\npid_start() {{{functions}\nrecord_pid {sleeper.pid}\nstop_pids\n'
        )
        subprocess.run(["bash", "-c", script], check=True, timeout=30)
        assert sleeper.wait(timeout=10) is not None
    finally:
        if sleeper.poll() is None:
            sleeper.kill()
