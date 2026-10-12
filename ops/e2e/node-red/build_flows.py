#!/usr/bin/env python3
"""Assemble ops/e2e/node-red/flows.json from the function sources in functions/.

The function bodies live as plain .js files so they can be reviewed and
unit-tested; flows.json is the generated artifact Node-RED loads. Run with
``--check`` to fail when flows.json is stale.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
FUNCTIONS = HERE / "functions"
FLOWS = HERE / "flows.json"

READINGS_TOPIC = "limnopulse/v1/devices/+/readings"
VERDICT_TOPIC = "limnopulse/e2e/verdicts"
TAB = "m1e2e_tab"
BROKER = "m1e2e_broker"
VERDICT = "m1e2e_verdict_topic"


def _function(
    node_id: str, name: str, source: str, outputs: int, x: int, y: int, wires: list
) -> dict:
    return {
        "id": node_id,
        "type": "function",
        "z": TAB,
        "name": name,
        "func": (FUNCTIONS / source).read_text(encoding="utf-8"),
        "outputs": outputs,
        "timeout": 0,
        "noerr": 0,
        "initialize": "",
        "finalize": "",
        "libs": [],
        "x": x,
        "y": y,
        "wires": wires,
    }


def _http(node_id: str, name: str, x: int, y: int, target: str) -> dict:
    return {
        "id": node_id,
        "type": "http request",
        "z": TAB,
        "name": name,
        "method": "use",
        # Text, not "obj": a non-JSON body (e.g. a text/plain 500) must not
        # trigger Node-RED's JSON parse warning; the functions parse it.
        "ret": "txt",
        "paytoqs": "ignore",
        "url": "",
        "tls": "",
        "persist": False,
        "proxy": "",
        "insecureHTTPParser": False,
        "authType": "",
        "senderr": False,
        "headers": [],
        "x": x,
        "y": y,
        "wires": [[target]],
    }


def build() -> list[dict]:
    return [
        {
            "id": TAB,
            "type": "tab",
            "label": "LimnoPulse M1 e2e verdicts",
            "disabled": False,
            "info": (
                "Subscribes to legacy readings, asks the API which deployment "
                "owned the device at the reading ts and publishes a verdict to "
                f"{VERDICT_TOPIC}. Canonical dev_ ids unknown to /v1 resolve "
                "through /v2 only (reason v2-native). Env: LIMNOPULSE_API_URL, LIMNOPULSE_TENANT, "
                "MQTT_HOST, MQTT_PORT."
            ),
            "env": [],
        },
        {
            "id": BROKER,
            "type": "mqtt-broker",
            "name": "limnopulse local broker",
            "broker": "${MQTT_HOST}",
            "port": "${MQTT_PORT}",
            "clientid": "",
            "autoConnect": True,
            "usetls": False,
            "protocolVersion": "4",
            "keepalive": "60",
            "cleansession": True,
            "autoUnsubscribe": True,
            "birthTopic": "",
            "birthQos": "0",
            "birthPayload": "",
            "birthMsg": {},
            "closeTopic": "",
            "closeQos": "0",
            "closePayload": "",
            "closeMsg": {},
            "willTopic": "",
            "willQos": "0",
            "willPayload": "",
            "willMsg": {},
            "userProps": "",
            "sessionExpiry": "",
        },
        {
            "id": "m1e2e_in_readings",
            "type": "mqtt in",
            "z": TAB,
            "name": "legacy readings",
            "topic": READINGS_TOPIC,
            "qos": "1",
            "datatype": "auto-detect",
            "broker": BROKER,
            "nl": False,
            "rap": True,
            "rh": 0,
            "inputs": 0,
            "x": 130,
            "y": 100,
            "wires": [["m1e2e_is_object"]],
        },
        {
            "id": "m1e2e_is_object",
            "type": "switch",
            "z": TAB,
            "name": "payload is object?",
            "property": "payload",
            "propertyType": "msg",
            "rules": [{"t": "istype", "v": "object", "vt": "object"}, {"t": "else"}],
            "checkall": "false",
            "repair": False,
            "outputs": 2,
            "x": 330,
            "y": 100,
            "wires": [["m1e2e_prepare"], ["m1e2e_reject"]],
        },
        _function(
            "m1e2e_prepare",
            "prepare v1 device GET",
            "prepare_v1_request.js",
            2,
            550,
            100,
            [["m1e2e_http_v1"], [VERDICT]],
        ),
        _function(
            "m1e2e_reject",
            "reject non-JSON payload",
            "reject_non_json.js",
            1,
            550,
            160,
            [[VERDICT]],
        ),
        _http("m1e2e_http_v1", "GET /v1 device", 780, 100, "m1e2e_after_v1"),
        _function(
            "m1e2e_after_v1",
            "default component + expected asset",
            "after_v1_device.js",
            2,
            1010,
            100,
            [["m1e2e_http_components"], [VERDICT]],
        ),
        _http(
            "m1e2e_http_components", "GET /v2 device components", 200, 260, "m1e2e_after_components"
        ),
        _function(
            "m1e2e_after_components",
            "check default component",
            "after_v2_components.js",
            2,
            450,
            260,
            [["m1e2e_http_deployments"], [VERDICT]],
        ),
        _http("m1e2e_http_deployments", "GET /v2 deployments", 690, 260, "m1e2e_resolve"),
        _function(
            "m1e2e_resolve",
            "resolve [started_at, ended_at)",
            "resolve_deployment.js",
            3,
            920,
            260,
            # Output 3 loops back for the next component of a native v2 device.
            [["m1e2e_http_asset"], [VERDICT], ["m1e2e_http_deployments"]],
        ),
        _http("m1e2e_http_asset", "GET /v2 asset", 200, 380, "m1e2e_final"),
        _function(
            "m1e2e_final",
            "final verdict",
            "final_verdict.js",
            1,
            400,
            380,
            [[VERDICT]],
        ),
        {
            "id": VERDICT,
            "type": "change",
            "z": TAB,
            "name": "verdict topic",
            "rules": [
                {"t": "set", "p": "topic", "pt": "msg", "to": VERDICT_TOPIC, "tot": "str"},
                {"t": "delete", "p": "e2e", "pt": "msg"},
                {"t": "delete", "p": "headers", "pt": "msg"},
                {"t": "delete", "p": "url", "pt": "msg"},
                {"t": "delete", "p": "method", "pt": "msg"},
                {"t": "delete", "p": "statusCode", "pt": "msg"},
                {"t": "delete", "p": "responseUrl", "pt": "msg"},
                {"t": "delete", "p": "redirectList", "pt": "msg"},
                {"t": "delete", "p": "retain", "pt": "msg"},
            ],
            "action": "",
            "property": "",
            "from": "",
            "to": "",
            "reg": False,
            "x": 640,
            "y": 380,
            "wires": [["m1e2e_out_verdicts"]],
        },
        {
            "id": "m1e2e_out_verdicts",
            "type": "mqtt out",
            "z": TAB,
            "name": "verdicts",
            "topic": "",
            "qos": "1",
            "retain": "false",
            "respTopic": "",
            "contentType": "",
            "userProps": "",
            "correl": "",
            "expiry": "",
            "broker": BROKER,
            "x": 840,
            "y": 380,
            "wires": [],
        },
    ]


def render() -> str:
    return json.dumps(build(), indent=4) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if flows.json is stale")
    args = parser.parse_args()
    rendered = render()
    if args.check:
        current = FLOWS.read_text(encoding="utf-8") if FLOWS.exists() else ""
        if current != rendered:
            print(
                "flows.json is stale; run: python ops/e2e/node-red/build_flows.py", file=sys.stderr
            )
            return 1
        return 0
    FLOWS.write_text(rendered, encoding="utf-8")
    print(f"wrote {FLOWS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
