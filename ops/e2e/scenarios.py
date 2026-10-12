#!/usr/bin/env python3
"""API-driven M1 e2e scenarios, run by ``run_m1_e2e.sh`` while the verdict flow is up.

``relocation`` (needs V1_POND_DEVICE_PROJECTION_ENABLED=true):
  creates a pond "East" and a NEW device in the West pond (``E2E_WEST_POND_ID``,
  default ``pond_local_001``) through ``/v1``, publishes a reading at t0, moves
  the device to East with ``PATCH /v1/.../devices/{id}`` and publishes at t1.
  The verdicts must resolve the West asset for t0 and the East asset for t1.
  ``local-device-001`` is never moved: Telegraf's static registry
  (infra/telegraf/device_registry.star) only enriches known devices, so the new
  device's readings reach the flow but are not ingested into Influx.

``v2-native`` (needs API_V2_ENABLED=true):
  creates a Site, a pond Asset, a canonical ``dev_`` Device, a probe Component
  and a Deployment through ``/v2`` and publishes one reading. ``/v1`` has no
  row for the device, so the flow must resolve it through ``/v2`` only
  (verdict reason ``v2-native``).

Each scenario waits for its own verdicts on the capture written by
``mqttx sub --output-mode clean`` (``--raw``) and checks the resolved assets.
Prints ``SCENARIO_DEVICE <device_id> <verdict count>`` for the final summary.
Exit codes: 0 pass (or skipped with --allow-v2-unavailable while /v2 is
not mounted), 1 a check failed, 2 setup failed. Standard library only.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
EXAMPLE = ROOT / "examples/telemetry/reading.local.json"
TOPIC_TEMPLATE = "limnopulse/v1/devices/{device_id}/readings"


class ScenarioError(Exception):
    pass


class SetupError(Exception):
    pass


class V2Unavailable(Exception):
    pass


def _load_summarizer() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "m1_e2e_summarize", HERE / "summarize_verdicts.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def legacy_asset_id(tenant: str, pond_id: str) -> str:
    return (
        "ast_" + uuid.uuid5(uuid.NAMESPACE_URL, f"limnopulse/v4/legacy-pond/{tenant}/{pond_id}").hex
    )


def legacy_component_id(tenant: str, device_id: str) -> str:
    name = f"limnopulse/v4/legacy-device-component/{tenant}/{device_id}"
    return "cmp_" + uuid.uuid5(uuid.NAMESPACE_URL, name).hex


def format_ts(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def ceil_second(value: datetime) -> datetime:
    whole = value.replace(microsecond=0)
    return whole if whole == value else whole + timedelta(seconds=1)


class Api:
    def __init__(self, base: str, tenant: str, sub: str, email: str) -> None:
        self.base = base.rstrip("/")
        self.tenant = tenant
        self.headers = {
            "Accept": "application/json",
            "X-Dev-User-Sub": sub,
            "X-Dev-User-Email": email,
        }
        # Loopback calls must never go through an outbound proxy.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def call(
        self, method: str, path: str, body: dict | None = None, *, idempotent: bool = False
    ) -> tuple[int, object]:
        headers = dict(self.headers)
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if idempotent:
            headers["Idempotency-Key"] = f"m1-e2e-{uuid.uuid4().hex}"
        request = urllib.request.Request(
            f"{self.base}/{path.lstrip('/')}", data=data, method=method, headers=headers
        )
        try:
            with self.opener.open(request, timeout=15) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as exc:
            status, raw = exc.code, exc.read()
        try:
            payload: object = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = raw.decode("utf-8", errors="replace")
        return status, payload

    def expect(
        self, status: int, method: str, path: str, body: dict | None = None, **kwargs: bool
    ) -> dict:
        got, payload = self.call(method, path, body, **kwargs)
        if got != status or not isinstance(payload, dict):
            if got == 404 and payload == {"detail": "Not Found"} and path.startswith("/v2/"):
                raise V2Unavailable(f"{method} {path}: route not mounted")
            raise SetupError(f"{method} {path} -> {got} {payload!r} (expected {status})")
        return payload

    def tenant_path(self, version: str, suffix: str) -> str:
        return f"/{version}/tenants/{self.tenant}/{suffix.lstrip('/')}"


class Harness:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.api = Api(args.api_url, args.tenant, args.dev_user_sub, args.dev_user_email)
        self.summarizer = _load_summarizer()
        self.template = json.loads(EXAMPLE.read_text(encoding="utf-8"))
        self.run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")

    def log(self, message: str) -> None:
        print(f"[m1-e2e:{self.args.scenario}] {message}", file=sys.stderr)

    def publish(self, device_id: str, seq: int, ts: datetime) -> dict:
        reading = {**self.template, "seq": seq, "ts": format_ts(ts)}
        topic = TOPIC_TEMPLATE.format(device_id=device_id)
        body = json.dumps(reading, separators=(",", ":"))
        command = [self.args.mqttx, "pub", "-h", self.args.host, "-p", str(self.args.port)]
        command += ["-t", topic, "-q", "1", "-m", body]
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise SetupError(f"mqttx pub failed ({result.returncode}): {result.stderr.strip()}")
        self.log(f"published {device_id} seq={seq} ts={reading['ts']}")
        return reading

    def wait_verdict(self, device_id: str, seq: int) -> dict:
        deadline = time.monotonic() + self.args.timeout
        while True:
            text = (
                self.args.raw.read_text(encoding="utf-8", errors="replace")
                if (self.args.raw.exists())
                else ""
            )
            for verdict in self.summarizer.parse_stream(text):
                if verdict.get("device_id") == device_id and verdict.get("seq") == seq:
                    print(json.dumps(verdict, sort_keys=True))
                    return verdict
            if time.monotonic() >= deadline:
                raise ScenarioError(f"no verdict for {device_id} seq={seq}")
            time.sleep(0.25)

    def check(self, verdict: dict, *, reason: str, resolved: str, v1_pond_id: str | None) -> None:
        if verdict.get("ok") is None and self.args.allow_v2_unavailable:
            self.log(f"seq={verdict.get('seq')} ok=null accepted (--allow-v2-unavailable)")
            return
        problems = []
        if verdict.get("ok") is not True or verdict.get("reason") != reason:
            problems.append(f"ok={verdict.get('ok')} reason={verdict.get('reason')}")
        if verdict.get("resolved_asset_id") != resolved:
            problems.append(f"resolved {verdict.get('resolved_asset_id')} != {resolved}")
        if verdict.get("v1_pond_id") != v1_pond_id:
            problems.append(f"v1_pond_id {verdict.get('v1_pond_id')} != {v1_pond_id}")
        if problems:
            raise ScenarioError(f"seq={verdict.get('seq')}: " + "; ".join(problems))
        self.log(f"seq={verdict.get('seq')} ok=true reason={reason} resolved={resolved}")

    # -- scenarios -------------------------------------------------------------------------

    def relocation(self) -> tuple[str, int]:
        api, tenant, west = self.api, self.args.tenant, self.args.west_pond_id
        east = api.expect(
            201,
            "POST",
            api.tenant_path("v1", "ponds"),
            {"name": f"E2E East {self.run_id}", "description": "m1 e2e relocation target"},
        )["pond_id"]
        device = api.expect(
            201,
            "POST",
            api.tenant_path("v1", "devices"),
            {"pond_id": west, "name": f"e2e-relocation-{self.run_id}"},
        )
        device_id = device["device_id"]
        west_asset, east_asset = legacy_asset_id(tenant, west), legacy_asset_id(tenant, east)
        self.log(f"device {device_id} created in {west} ({west_asset}); East is {east}")

        # t0: after the device's created_at (= start of its first deployment, second
        # precision) and before the move. The verdict is awaited before the PATCH because
        # the flow's expected asset comes from the device's current /v1 pond.
        t0 = max(datetime.now(UTC), parse_ts(device["created_at"]))
        self.check(
            self._publish_and_wait(device_id, 1, t0),
            reason="match",
            resolved=west_asset,
            v1_pond_id=west,
        )

        moved = api.expect(
            200,
            "PATCH",
            api.tenant_path("v1", f"devices/{device_id}"),
            {"expected_version": device["version"], "pond_id": east},
        )
        if moved.get("pond_id") != east:
            raise ScenarioError(f"PATCH did not move the device: {moved!r}")
        # t1: at or after the transition instant (microsecond precision), rounded up to
        # the second the reading carries.
        transition = datetime.now(UTC)
        try:
            listing = api.expect(
                200,
                "GET",
                api.tenant_path(
                    "v2", f"deployments?component_id={legacy_component_id(tenant, device_id)}"
                ),
            )
            active = [d for d in listing.get("items", []) if d.get("ended_at") is None]
            if len(active) == 1:
                transition = parse_ts(active[0]["started_at"])
        except V2Unavailable:
            if not self.args.allow_v2_unavailable:
                raise
        t1 = max(ceil_second(transition), ceil_second(datetime.now(UTC)))
        wait = (t1 - datetime.now(UTC)).total_seconds()
        if wait > 0:
            time.sleep(math.ceil(wait * 10) / 10)
        self.log(f"moved {device_id} to {east} at {transition.isoformat()}")
        self.check(
            self._publish_and_wait(device_id, 2, t1),
            reason="match",
            resolved=east_asset,
            v1_pond_id=east,
        )
        return device_id, 2

    def v2_native(self) -> tuple[str, int]:
        api = self.api
        site_id = api.expect(
            201,
            "POST",
            api.tenant_path("v2", "sites"),
            {"name": f"E2E site {self.run_id}"},
            idempotent=True,
        )["site_id"]
        asset_id = api.expect(
            201,
            "POST",
            api.tenant_path("v2", "assets"),
            {"site_id": site_id, "type": "pond", "name": f"E2E native pond {self.run_id}"},
            idempotent=True,
        )["asset_id"]
        device_id = api.expect(
            201,
            "POST",
            api.tenant_path("v2", "devices"),
            {"name": f"e2e-native-{self.run_id}"},
            idempotent=True,
        )["device_id"]
        component_id = api.expect(
            201,
            "POST",
            api.tenant_path("v2", f"devices/{device_id}/components"),
            {
                "kind": "probe",
                "name": "e2e probe",
                "probe_profile": {"measurement_metric_keys": ["temp_c", "ph", "do_mg_l"]},
            },
            idempotent=True,
        )["component_id"]
        started_at = datetime.now(UTC) - timedelta(seconds=5)
        deployment = api.expect(
            201,
            "POST",
            api.tenant_path("v2", "deployments"),
            {
                "component_id": component_id,
                "asset_id": asset_id,
                "started_at": started_at.isoformat().replace("+00:00", "Z"),
            },
            idempotent=True,
        )
        self.log(
            f"native {device_id} / {component_id} deployed to {asset_id} "
            f"({deployment.get('deployment_id')}) from {deployment.get('started_at')}"
        )
        status, _ = api.call("GET", api.tenant_path("v1", f"devices/{device_id}"))
        if status != 404:
            raise ScenarioError(f"/v1 knows native device {device_id} (status {status})")
        self.check(
            self._publish_and_wait(device_id, 1, datetime.now(UTC)),
            reason="v2-native",
            resolved=asset_id,
            v1_pond_id=None,
        )
        return device_id, 1

    def _publish_and_wait(self, device_id: str, seq: int, ts: datetime) -> dict:
        self.publish(device_id, seq, ts)
        return self.wait_verdict(device_id, seq)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("scenario", choices=("relocation", "v2-native"))
    parser.add_argument("--raw", required=True, type=Path, help="mqttx sub clean-mode capture")
    parser.add_argument(
        "--api-url", default=os.environ.get("LIMNOPULSE_API_URL", "http://127.0.0.1:8000")
    )
    parser.add_argument("--tenant", default=os.environ.get("LIMNOPULSE_TENANT", "tnt_local_001"))
    parser.add_argument(
        "--west-pond-id", default=os.environ.get("E2E_WEST_POND_ID", "pond_local_001")
    )
    parser.add_argument(
        "--dev-user-sub", default=os.environ.get("LIMNOPULSE_DEV_USER_SUB", "local-user-001")
    )
    parser.add_argument(
        "--dev-user-email",
        default=os.environ.get("LIMNOPULSE_DEV_USER_EMAIL", "local@example.test"),
    )
    parser.add_argument("--host", default=os.environ.get("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    parser.add_argument("--mqttx", default=os.environ.get("MQTTX_BIN", "mqttx"))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("E2E_TIMEOUT", "45")))
    parser.add_argument("--allow-v2-unavailable", action="store_true")
    args = parser.parse_args(argv)

    harness = Harness(args)
    run = harness.relocation if args.scenario == "relocation" else harness.v2_native
    try:
        device_id, count = run()
    except V2Unavailable as exc:
        if args.allow_v2_unavailable:
            harness.log(f"skipped: /v2 unavailable ({exc})")
            return 0
        harness.log(f"FAIL: /v2 unavailable ({exc})")
        return 1
    except SetupError as exc:
        harness.log(f"setup failed: {exc}")
        return 2
    except ScenarioError as exc:
        harness.log(f"FAIL: {exc}")
        return 1
    print(f"SCENARIO_DEVICE {device_id} {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
